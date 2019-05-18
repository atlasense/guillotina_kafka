import os
import asyncio
import logging
import aiotask_context
from guillotina import app_settings
from aiokafka import AIOKafkaConsumer
from guillotina.tests.utils import login
from aiokafka.errors import IllegalStateError
from guillotina.utils import resolve_dotted_name
from guillotina.commands.server import ServerCommand
from guillotina.tests.utils import get_mocked_request
from guillotina_kafka.consumer import ConsumerWorkerLookupError
from guillotina_kafka.commands.kafka_consumer import StartConsumerCommand

logger = logging.getLogger(__name__)


class StopConsumerException(Exception):
    pass


class StartConsumersCommand(StartConsumerCommand):

    def get_parser(self):
        parser = super(StartConsumersCommand, self).get_parser()
        parser.add_argument(
            '--consumer-worker', type=str, default='default', nargs='+',
            help='Application consumer that will consume messages from topics.'
        )
        parser.add_argument(
            '--api-version', type=str,
            default='auto', help='Kafka server api version.'
        )
        return parser

    async def run_consumer(self, worker, topic, worker_conf):
        '''
        Run the consumer in a way that makes sure we exit
        if the consumer throws an error
        '''
        request = get_mocked_request()
        login(request)
        aiotask_context.set('request', request)

        try:
            await worker(topic, request, worker_conf, app_settings)
        except Exception:
            logger.error('Error running consumer', exc_info=True)
            await topic.stop()
            os._exit(1)

    def init_worker(self, worker_name, arguments):
        worker = self.get_worker(worker_name)
        if not worker:
            raise ConsumerWorkerLookupError(
                f'{worker_name}: Worker has not been registered.')
        try:
            worker['handler'] = resolve_dotted_name(
                worker['path']
            )
        except KeyError:
            raise ConsumerWorkerLookupError(
                f'{worker_name}: Worker has not been registered.')
        return worker

    def run(self, arguments, settings, app):
        self.tasks = []
        self.init_producers()
        for worker_name in arguments.consumer_worker:
            worker = self.init_worker(worker_name, arguments)
            for topic in worker['topics']:
                topic_prefix = app_settings["kafka"].get("topic_prefix", "")
                consumer = AIOKafkaConsumer(
                    f'{topic_prefix}{topic}', **{
                        "api_version": arguments.api_version,
                        "group_id": worker.get("group", "default"),
                        "bootstrap_servers": app_settings['kafka']['brokers'],
                        'loop': self.get_loop(),
                        'metadata_max_age_ms': 5000,
                    })
                self.tasks.append(
                    self.run_consumer(worker['handler'], consumer, worker))
        asyncio.gather(*self.tasks, loop=self.get_loop())
        return super().run(arguments, settings, app)


class BaseConsumerWorker:

    def __init__(self, msg_deserializer=lambda data: data.decode('utf-8')):
        self.msg_deserializer = msg_deserializer
        self.ready = False

    async def seek(self, topic, step):
        for tp in topic.assignment():
            try:
                position = await topic.position(tp)
            except IllegalStateError:
                position = 0

            if position > 0:
                topic.seek(tp, position + step)
        return topic

    async def reposition_offset(self, topic, position):

        try:
            position = int(position)
        except ValueError:
            pass
        else:
            return await self.seek(topic, position)

        try:
            await {
                'beginning': topic.seek_to_beginning,
                'end': topic.seek_to_end,
            }[position]()
        except KeyError:
            raise Exception('Invalid offset position')

        return topic

    async def setup(self):
        raise NotImplementedError()

    async def __call__(self, topic, request, worker_conf, settings):
        raise NotImplementedError()
