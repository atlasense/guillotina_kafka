from .interfaces import IKafkaMessageConsumedEvent
from zope.interface import implementer


@implementer(IKafkaMessageConsumedEvent)
class KafkaMessageConsumedEvent:

    def __init__(self, msg, utility):
        self.msg = msg
        self.utility = utility
