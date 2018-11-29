
class ConsumerWorkerLookupError(Exception):
    pass


class InvalidConsumerType(Exception):
    pass

async def default_worker(*args, **kwargs):
    print(args, kwargs)
    return
