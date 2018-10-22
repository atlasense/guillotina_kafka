from guillotina4_kafka.consumers.template import ITemplateConsumer

CONSUMER_INTERFACE_REGISTRY = {
    'template': ITemplateConsumer
}


class ConsumerLookupError(Exception):
    pass
