import asyncio

import ccxt.async_support as ccxt

from .bucket import Bucket
from .settings import BucketLimits


class ReqObject:
    def __init__(self, method, args, kwargs, cost=1, priority=10):
        self.method = method
        self.args = args
        self.kwargs = kwargs

        self.cost = cost
        self.priority = priority

        self.response = asyncio.Queue(maxsize=1)
        self.responseList = [self.response]

    def __eq__(self, other):
        # todo determine when two are equivalent
        return self.method == other.method

    async def get(self):
        await self.response.get()

    def put(self, result):
        for r in self.responseList:
            r.put_nowait(result)

    def append(self, req_obj):
        self.responseList.append(req_obj.response)


class Connector:
    __connectorsDict = {}

    def __new__(cls, exchange, *args, **kwargs):
        if exchange in cls.__connectorsDict.keys():
            for conn in cls.__connectorsDict[exchange].values():
                if conn.key == kwargs.get('key', None):
                    return conn
            instance = cls.__init__(exchange, *args, **kwargs)
            cls.__connectorsDict[exchange].append(instance)
            return instance

        if exchange in ccxt.exchanges:
            instance = cls.__init__(exchange, *args, **kwargs)
            cls.__connectorsDict[exchange] = [instance]
            return instance
        else:
            raise NotImplementedError(f'{exchange} connector not yet implemented')

    def __init__(self, exchange, *args, **kwargs):
        self.name = exchange
        self.api = getattr(ccxt, exchange)
        self.key = kwargs.get('key', None)

        limit = BucketLimits.get(exchange, BucketLimits['default'])
        self.bucket = Bucket(**limit, startFill=limit['size'] / 2)
        self.waitingList = []
        self.tqLock = asyncio.Lock()
        self.taskQ = asyncio.PriorityQueue()

    async def request(self, method, *args, **kwargs):
        newReq = ReqObject(method, args, kwargs)
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
        except ccxt.errors.BaseError as e:
            self._handleErrors(e)
            # task.priority = 0
            # await self.taskQ.put(task)
            # await self._processTasks()
            return

        task.put(response)
        self.waitingList.remove(task)
        # self.log.debug(f'Distributed {task.signature}')

    def _handleErrors(self, err):
        # todo handle ccxt errors
        pass
