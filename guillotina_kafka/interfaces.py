from zope.interface import Interface


class IKafak(Interface):
    async def init():
        pass

    def is_ready():
        pass

    async def stop():
        pass


class IProducer(IKafak):
    pass


class IProducerUtility(Interface):
    async def send():
        pass


class IConsumer(IKafak):
    pass


class IConsumerUtility(Interface):
    async def consume():
        pass
