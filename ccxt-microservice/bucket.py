import asyncio
from datetime import datetime


class Bucket:

    def __init__(self, size, refRate, startFill=0):
        self.maxSize = size
        self.refRate = refRate
        self.__startSize = startFill
        self.__startTs = datetime.now().timestamp()
        self.lock = asyncio.Lock()

    def state(self):
        return max(self.__startSize - self._deltaFill(), 0)

    def push(self, fill):
        if fill > self.maxSize:
            raise Exception('Fill bigger than max bucket size')
        state = self.state()
        if fill + state > self.maxSize:
            raise Exception('Bucket too full as of now.')
        elif state == 0:
            self.__startSize = fill
            self.__startTs = datetime.now().timestamp()
        else:
            self.__startSize += fill
        return state + fill

    def _deltaFill(self):
        return self.refRate * self._deltaTime()

    def _deltaTime(self):
        return datetime.now().timestamp() - self.__startTs

    def timeToWait(self, fill):
        return max(fill + self.state() - self.maxSize, 0)/self.refRate

    async def add(self, fill):
        async with self.lock:
            delay = self.timeToWait(fill)
            while delay != 0:
                await asyncio.sleep(delay)
                delay = self.timeToWait(fill)
            return self.push(fill)
