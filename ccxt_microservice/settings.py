BucketLimits = {}


def addBucketLimit(exchange, size, refRate):
    BucketLimits[exchange] = {'size': size, 'refRate': refRate}


addBucketLimit('default', 20, 2)
addBucketLimit('kraken', 18, 2)
addBucketLimit('hitbtc2', 12, 3)
