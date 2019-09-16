import asyncio

import ccxt.async_support as ccxt

from ccxt_microservice.bottleneck import Bottleneck
from ccxt_microservice.requestObject import RequestObject


class Connector:
    __connectorsDict = {}

    @classmethod
    def get_instance(cls, exchange, *args, **kwargs):
        if exchange in cls.__connectorsDict.keys():
            return cls.__connectorsDict[exchange]
        if exchange in ccxt.exchanges:
            instance = Connector(exchange, *args, **kwargs)
            cls.__connectorsDict[exchange] = instance
            return instance
        else:
            raise NotImplementedError(f'{exchange} connector not yet implemented')

    def __init__(self, exchange, *args, **kwargs):
        self.name = exchange
        self.api: ccxt.Exchange = getattr(ccxt, exchange)({'verbose': True})

        self.bottlenecks = [Bottleneck(exchange, kwargs.get('apiKey', None))]
        self.waitingList = []

    async def request(self, method, *args, **kwargs):
        newReq = RequestObject(method, args, kwargs)
        appended = False
        for req in self.waitingList:
            if req == newReq:
                req.append(newReq)
                appended = True
                break
        if not appended:
            bn = self._get_bottleneck(newReq.kwargs.get('apiKey', None))
            self.waitingList.append(newReq)
            await bn.put(newReq)
            asyncio.get_event_loop().create_task(self._process(bn))
        return await newReq.get()

    async def _process(self, source):
        # self.log.debug('Waiting task...')
        task = await source.get()
        try:
            self.api.open()
            response = await getattr(self.api, task.method)(*task.args, **task.kwargs)
            task.put(response)
            self.waitingList.remove(task)

        except ccxt.errors.BaseError as e:
            self._handleErrors(e)

        if not self.waitingList:
            await self.api.close()
        # self.log.debug(f'Distributed {task.signature}')

    def _handleErrors(self, err):
        # todo handle ccxt errors
        # task.priority = 0
        # await self.taskQ.put(task)
        # await self._process()
        pass

    def _get_bottleneck(self, key):
        bn = None
        for bottleneck in self.bottlenecks:
            if bottleneck.key == key:
                bn = bottleneck
        if not bn:
            bn = Bottleneck(self.name, key)
            self.bottlenecks.append(bn)
        return bn
