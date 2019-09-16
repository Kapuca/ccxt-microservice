import asyncio

from ccxt_microservice.bucket import Bucket
from ccxt_microservice.settings import BucketLimits


class Bottleneck(object):

    def __init__(self, exchange, key):
        self.key = key
        limit = BucketLimits.get(exchange, BucketLimits['default'])
        self.bucket = Bucket(**limit, startFill=limit['size'] / 2)
        self.tqLock = asyncio.Lock()
        self.taskQ = asyncio.PriorityQueue()

    async def put(self, reqObj):
        await self.taskQ.put(reqObj)

    async def get(self):
        async with self.tqLock:
            task = await self.taskQ.get()
            await self.bucket.add(task.cost)
        return task
