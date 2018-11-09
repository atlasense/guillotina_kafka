from zope.interface import Interface


class IConsumer(Interface):
    def consumer(self):
        pass
