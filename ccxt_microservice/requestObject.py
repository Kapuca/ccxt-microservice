import asyncio


class RequestObject:
    def __init__(self, method, args, kwargs, cost=1, priority=10):
        self.method = method
        self.args = args
        self.kwargs = kwargs

        self.cost = cost
        self.priority = priority

        self.response = asyncio.Queue(maxsize=1)
        self.responseList = [self.response]

    def __eq__(self, other):
        if self.method == other.method:

            if len(self.args) == len(other.args):
                for arg1, arg2 in zip(self.args, other.args):
                    if not (arg1 == arg2):
                        return False
            else:
                return False

            if len(self.kwargs.keys()) == len(other.kwargs.keys()):
                for key in self.kwargs.keys():
                    if not (self.kwargs[key] == other.kwargs[key]):
                        return False
            else:
                return False

            return True
        return False

    def __gt__(self, other):
        return self.priority > other.priority

    def __lt__(self, other):
        return self.priority < other.priority

    async def get(self):
        return await self.response.get()

    def put(self, result):
        for r in self.responseList:
            r.put_nowait(result)

    def append(self, req_obj):
        self.responseList.append(req_obj.response)
