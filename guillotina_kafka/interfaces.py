from zope.interface import Interface


class IKafka(Interface):
    async def init():
        pass

    def is_ready():
        pass

    async def stop():
        pass


class IProducer(IKafka):
    pass

class IKafkaProducerUtility(IKafka):
    pass


class IProducerUtility(Interface):
    async def send():
        pass


class IConsumer(IKafka):
    pass


class IConsumerUtility(Interface):
    async def consume():
        pass
