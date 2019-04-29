import asyncio
import logging
import aiotask_context
from aiokafka import AIOKafkaConsumer
from asyncio import InvalidStateError
from guillotina.tests.utils import login
from guillotina.utils import resolve_dotted_name
from guillotina.commands.server import ServerCommand
from guillotina.tests.utils import get_mocked_request
from guillotina_kafka.consumer import ConsumerWorkerLookupError


logger = logging.getLogger(__name__)


class StopConsumerException(Exception):
    pass


class TaskWrapper:

    next_id = 0

    def __init__(self, loop, routine, *args, **kwargs):
        self.id = TaskWrapper.next_id
        TaskWrapper.next_id += 1
        self.routine = routine
        self.kwargs = kwargs
        self.future = None
        self.loop = loop
        self.args = args

    def run(self):
        self.future = asyncio.ensure_future(
            self.routine(*self.args, **self.kwargs),
            loop=self.loop
        )
        return self.future

    @property
    def running(self):
        result = True
        exception = None
        if self.future is not None:
            if self.future.cancelled():
                result = False
            try:
                exception = self.future.exception()
                if exception:
                    result = False
            except (InvalidStateError):
                pass
        return result

    @property
    def was_stoped(self):
        result = False
        if self.future is not None:
            try:
                exception = self.future.exception()
                if isinstance(exception, StopConsumerException):
                    result = True
            except InvalidStateError:
                pass
        return result

    def restart(self):
        if self.running:
            self.future.cancel()
        self.run()


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

    def get_worker(self, name, settings):
        for worker in settings['kafka']['consumer']['workers']:
            if name == worker['name']:
                worker = {
                    **worker, "topics": list({
                        *worker.get('topics', []),
                        *settings['kafka']['consumer'].get('topics', [])
                    })
                }
                return worker
        return {}

    async def run_consumer(self, worker, topic, arguments, settings):
        '''
        Run the consumer in a way that makes sure we exit
        if the consumer throws an error
        '''
        request = get_mocked_request()
        login(request)
        aiotask_context.set('request', request)

        try:
            await worker(topic, request, arguments, settings)
        except Exception:
            logger.error('Error running consumer', exc_info=True)
        finally:
            await topic.stop()

    def init_worker(self, worker_name, arguments, settings):
        worker = self.get_worker(worker_name, settings)
        if not worker:
            raise ConsumerWorkerLookupError(
                f'{worker_name}: Worker has not been registered.')
        try:
            worker['handler'] = resolve_dotted_name(
                worker['path']
            )
            worker['topics'] = [
                AIOKafkaConsumer(topic, **{
                    "api_version": arguments.api_version,
                    "group_id": worker.get('group', 'default'),
                    "bootstrap_servers": settings['kafka']['brokers'],
                    'loop': self.get_loop(),
                    'metadata_max_age_ms': 5000,
                })
                for topic in worker['topics']
            ]
        except KeyError:
            raise ConsumerWorkerLookupError(
                f'{worker_name}: Worker has not been registered.')
        return worker

    async def check(self):
        while True:
            for _id, task in enumerate(self.tasks):
                if task.running or task.was_stoped:
                    continue
                task.restart()
                self.tasks[_id] = task
            await asyncio.sleep(self.evry)

    def run(self, arguments, settings, app):
        self.tasks = []
        self.evry = arguments.check_interval
        self.manager_loop = self.get_loop()
        for worker_name in arguments.consumer_worker:
            worker = self.init_worker(worker_name, arguments, settings)
            for topic in worker['topics']:
                for _ in range(worker.get('replicas', 1)):
                    task = TaskWrapper(
                        self.manager_loop, self.run_consumer,
                        worker['handler'], topic, arguments, settings
                    )
                    task.run()
                    self.tasks.append(task)

        manager_task = TaskWrapper(self.manager_loop, self.check)
        manager_task.run()
        return super().run(arguments, settings, app)
