from zope.interface import Interface


class IKafkaUtility(Interface):
    async def get(self):
        pass

