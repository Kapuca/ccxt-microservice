import asyncio
import json
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import sys
from typing import List, Union, Optional, Awaitable
import yaml

from tornado.options import define, options
from ccxt_microservice.connector import Connector

define("port", default=5000, help="run on the given port", type=int)
define("apikeys", default="", help="apiKeys and secrets.", type=str)
define("nonce_msec", default=False, help="use milliseconds as nonce.", type=bool)


class Application(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/", HomeHandler),
            (r"/([^/]+)/([^/]+)", ExchangeAPIHandler),
            (r"/parallel/([^/]+)/([^/]+)", ParallelExchangeAPIHandler),
            (r"/parallel/method/fetch_order_books/([^/]+)", ParallelFetchOrderBooks),
            (r"/async/fetch/", AsyncFetchAPIHandler),
        ]
        settings = dict(
            debug=True,
        )
        super(Application, self).__init__(handlers, **settings)

        self.exchanges = {}
        self.async_exchanges = {}

        apikeys = {}
        if options.apikeys != "":
            with open(options.apikeys, "r") as f:
                apikeys = yaml.load(f)
        self.apikeys = apikeys


def get_module(module: str):
    return sys.modules[module].__dict__


def get_async_ccxt_class(exchange: str):
    module = get_module("ccxt.async_support")
    if exchange not in module:
        raise ValueError("Invalid exchange %s" % exchange)
    return module[exchange]


class HomeHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("hello world!")


class BaseHandler(tornado.web.RequestHandler):

    @property
    def exchanges(self):
        return self.application.async_exchanges

    def get_apikey(self, exchange):
        """Get apiKey and secret for an exchange. Returns empty dict if not available.
        """
        return self.application.apikeys.get(exchange, {})

    def get_ccxt_class(self, exchange: str):
        return get_async_ccxt_class(exchange)

    def get_exchange(self, exchange: str):
        if exchange not in self.exchanges:
            cls = self.get_ccxt_class(exchange)
            if options.nonce_msec:
                cls.nonce = lambda self: cls.milliseconds()
            apikey = self.get_apikey(exchange)
            self.exchanges[exchange] = cls(apikey)
        return self.exchanges[exchange]


class ExchangeAPIHandler(BaseHandler):

    async def post(self, exchange, method):
        ex = Connector.get_instance(exchange)

        if len(self.request.body) > 0:
            request = json.loads(self.request.body)
        else:
            request = {}
        result = await ex.request(method, **request)

        self.write(json.dumps(result))


async def parallel_run(async_callable, params: List[dict]):
    return asyncio.gather(async_callable(**param) for param in params)


class ParallelExchangeAPIHandler(BaseHandler):

    async def post(self, name: str, method: str):
        print(self.request.body)
        if self.request.body:
            params = json.loads(self.request.body)
        else:
            params = {}
        exchange = Connector.get_instance(name)

        cors = [exchange.request(method, **param) for param in params]
        results = await asyncio.gather(*cors)
        self.write(json.dumps(results))


class ParallelFetchOrderBooks(BaseHandler):

    async def post(self, name: str):
        exchange = self.get_exchange(name)

        request = json.loads(self.request.body)

        # TODO validation
        symbols = request["symbols"]
        params = [
            {"symbol": symbol, "params": request.get("params", {})} for symbol in symbols
        ]
        order_books = await parallel_run(exchange.fetch_order_book, params)
        response = {
            symbol: order_book for symbol, order_book in zip(symbols, order_books)
        }
        return self.write(response)


class AsyncFetchAPIHandler(tornado.websocket.WebSocketHandler):

    def open(self, *args: str, **kwargs: str) -> Optional[Awaitable[None]]:
        pass

    def on_message(self, message: Union[str, bytes]) -> Optional[Awaitable[None]]:

        message = json.load(message)
        responses = []
        for request in message:

            connectors = []
            for exchange in request['exchanges']:
                connectors.append(Connector.get_instance(exchange))

            methods = []
            for method_shema in request['methods']:
                methods.extend(self._generateMethods(method_shema))

            for conn in connectors:
                for method in methods:
                    responses.append(self._forwardResponse(conn, method))

        return asyncio.ensure_future(asyncio.gather(*responses))

    def _generateMethods(self, merhod_shema):
        return []

    async def _forwardResponse(self, conn, method):
        await self.write_message({
            'exchange': conn.name,
            'method': method.signature,
            'result': await conn.request(**method)
        })

    def close(self, code: int = None, reason: str = None) -> None:
        pass


def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
