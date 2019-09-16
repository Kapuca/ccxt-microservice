import asyncio

import ccxt.async_support as ccxt

from ccxt_microservice.requestObject import RequestObject


class Connector:
    __connectorsDict = {}

    @classmethod
    def get_instance(cls, exchange, *args, **kwargs):
        if exchange in cls.__connectorsDict.keys():
            for conn in cls.__connectorsDict[exchange]:
                if conn.key == kwargs.get('key', None):
                    return conn
            instance = Connector(exchange, *args, **kwargs)
            cls.__connectorsDict[exchange].append(instance)
            return instance

        if exchange in ccxt.exchanges:
            instance = Connector(exchange, *args, **kwargs)
            cls.__connectorsDict[exchange] = [instance]
            return instance
        else:
            raise NotImplementedError(f'{exchange} connector not yet implemented')

    def __init__(self, exchange, *args, **kwargs):
        self.name = exchange
        self.key = kwargs.get('key', None)
        self.api = getattr(ccxt, exchange)({'apiKey': self.key, 'verbose': True})

        limit = BucketLimits.get(exchange, BucketLimits['default'])
        self.bucket = Bucket(**limit, startFill=limit['size'] / 2)
        self.waitingList = []
        self.tqLock = asyncio.Lock()
        self.taskQ = asyncio.PriorityQueue()

    async def request(self, method, *args, **kwargs):
        newReq = RequestObject(method, args, kwargs)
        appended = False
        for req in self.waitingList:
            if req == newReq:
                req.append(newReq)
                appended = True
                break
        if not appended:
            await self.taskQ.put(newReq)
            asyncio.get_event_loop().create_task(self._processTasks())
            self.waitingList.append(newReq)
        return await newReq.get()
        # await self.bucket.add(1)
        # return await getattr(self.api, method)(*args, **kwargs)

    async def _processTasks(self):
        # self.log.debug('Waiting task...')
        async with self.tqLock:
            task = await self.taskQ.get()
            await self.bucket.add(task.cost)

        try:
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
        # await self._processTasks()
        pass
