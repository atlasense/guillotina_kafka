import asyncio
import logging
import aiotask_context
from guillotina import app_settings
from aiokafka import AIOKafkaConsumer
from asyncio import InvalidStateError
from guillotina.tests.utils import login
from aiokafka.errors import IllegalStateError
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
            '--check-interval', type=int, default=60,
            help='The time interval between consumers state check.'
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

    async def run_consumer(self, worker, topic, arguments):
        '''
        Run the consumer in a way that makes sure we exit
        if the consumer throws an error
        '''
        request = get_mocked_request()
        login(request)
        aiotask_context.set('request', request)

        try:
            await worker(topic, request, arguments, app_settings)
        except Exception:
            logger.error('Error running consumer', exc_info=True)
            await topic.stop()
            raise

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

    async def monitor(self, interval=60):
        while True:
            logger.warnings('Checking consumer workers')
            for task in self.tasks:
                exception = None
                try:
                    exception = task.exception()
                    if exception is not None:
                        for task in filter(
                                lambda _task: _task != task, self.tasks):
                            task.cancel()
                        exit(1)
                except InvalidStateError:
                    continue
            await asyncio.sleep(interval)

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

    async def run(self, arguments, settings, app):
        self.tasks = []
        self.evry = arguments.check_interval
        for worker_name in arguments.consumer_worker:
            worker = self.init_worker(worker_name, arguments)

            for topic in worker['topics']:
                topic_prefix = app_settings["kafka"].get("topic_prefix", "")
                topic = AIOKafkaConsumer(
                    f'{topic_prefix}{topic}', **{
                        "api_version": arguments.api_version,
                        "group_id": worker.get("group", "default"),
                        "bootstrap_servers": app_settings['kafka']['brokers'],
                        'loop': self.get_loop(),
                        'metadata_max_age_ms': 5000,
                    })

                if worker.get('start_from'):
                    topic = await self.reposition_offset(
                        topic, worker.get('start_from'))

                task = asyncio.ensure_future(
                    self.run_consumer(worker['handler'], topic, arguments),
                    loop=self.get_loop()
                )
                self.tasks.append(task)

        asyncio.ensure_future(
            self.monitor(arguments.check_interval), loop=self.get_loop())
        return super().run(arguments, settings, app)
