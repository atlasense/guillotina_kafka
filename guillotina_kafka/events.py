from zope.interface import implementer

from .interfaces import IKafkaMessageConsumedEvent


@implementer(IKafkaMessageConsumedEvent)
class KafkaMessageConsumedEvent:
    def __init__(self, msg):
        self.msg = msg
