from collections import namedtuple
from guillotina import app_settings
from guillotina.utils.modules import resolve_dotted_name

import inspect

Message = namedtuple('Message', 'value partition offset topic')


class KafkaProducerUtility:
    is_ready = True

    tested_consumers = (
        'es_security_updates',
    )

    def __init__(self, settings, loop=None):
        # Get kafka connection details from app settings
        self.loop = loop
        self.settings = settings or {}
        self.sent = {}
        self.results = {}
        self.topic_handlers = {}

    async def initialize(self):
        for worker in app_settings['kafka']['consumer']['workers']:
            handler = resolve_dotted_name(worker['path'])
            if inspect.isclass(handler):
                handler = handler()
            if hasattr(handler, 'initialize'):
                await handler.initialize()
            self.topic_handlers[worker['path']] = handler

    async def finalize(self):
        for handler in self.topic_handlers.values():
            if hasattr(handler, 'finalize'):
                await handler.finalize()

    async def send(self, topic, value):
        if topic not in self.sent:
            self.sent[topic] = []
        self.sent[topic].append(value)
        # check if we should process this one...
        for worker in app_settings['kafka']['consumer']['workers']:
            if 'topics' in worker and topic in worker['topics']:
                result = await self.topic_handlers[worker['path']](
                    Message(value, '1', len(self.sent[topic]), topic))
                if topic not in self.results:
                    self.results[topic] = []
                self.results[topic].append(result)

    async def stop(self):
        pass
