import asyncio
import logging
import aiotask_context
from guillotina import app_settings
from aiokafka import AIOKafkaConsumer
from guillotina.tests.utils import login
from guillotina.utils import resolve_dotted_name
from guillotina.commands.server import ServerCommand
from guillotina.tests.utils import get_mocked_request
from guillotina_kafka.consumer import ConsumerWorkerLookupError

logger = logging.getLogger(__name__)


class StopConsumerException(Exception):
    pass


class StartConsumersCommand(ServerCommand):

    description = 'Start Kafka consumer'

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

    def get_worker(self, name):
        for worker in app_settings['kafka']['consumer']['workers']:
            if name == worker['name']:
                worker = {
                    **worker, "topics": list({
                        *worker.get('topics', []),
                        *app_settings['kafka']['consumer'].get('topics', [])
                    })
                }
                return worker
        return {}

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
            exit(1)

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
        self.evry = arguments.check_interval
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
