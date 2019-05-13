from .interfaces import IKafkaMessageConsumedEvent
from .interfaces import IKafkaMessageProducedEvent
from zope.interface import implementer


@implementer(IKafkaMessageConsumedEvent)
class KafkaMessageConsumedEvent:

    def __init__(self, msg, utility):
        self.msg = msg
        self.utility = utility


@implementer(IKafkaMessageProducedEvent)
class KafkaMessageProducedEvent:

    def __init__(self, future, utility):
        self.future = future
        self.utility = utility
