import ccxt


class Connector:
    __connectorsDict = {}
    __connectorTemplates = {}

    def __new__(cls, exchange, *args, **kwargs):
        if exchange in cls.__connectorsDict.keys():
            if cls.__connectorTemplates[exchange].restriction == 'KEY':
                for conn in cls.__connectorsDict[exchange].values():
                    if conn.key == kwargs['key']:
                        return conn
                instance = cls.__init__(exchange, *args, **kwargs)
                cls.__connectorsDict[exchange].append(instance)
                return instance
            return cls.__connectorsDict[exchange][0]

        if exchange in cls.__connectorTemplates.keys() or exchange in ccxt.exchanges:
            instance = cls.__init__(exchange, *args, **kwargs)
            cls.__connectorsDict[exchange] = [instance]
            return instance
        else:
            raise NotImplementedError(f'{exchange} connector not yet implemented')

    def __init__(self, exchange, *args, **kwargs):
        pass

    async def request(self, *args, **kwargs):
        pass