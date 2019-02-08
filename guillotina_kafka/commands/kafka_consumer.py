from guillotina.commands import Command
from guillotina.component import get_adapter
from guillotina.utils import resolve_dotted_name
from guillotina_kafka.interfaces import IConsumerUtility
from guillotina_kafka.consumer.batch import BatchConsumer
from guillotina_kafka.consumer.stream import StreamConsumer
from guillotina_kafka.consumer import (
    ConsumerWorkerLookupError, InvalidConsumerType)



class StartConsumerCommand(Command):

    description = 'Start Kafka consumer'

    def get_parser(self):
        parser = super(StartConsumerCommand, self).get_parser()

        parser.add_argument(
            '--consumer-type', type=str, default='stream',
        )
        parser.add_argument(
            '--topics', nargs='*',
            help='Kafka topics to consume from', type=str
        )
        parser.add_argument(
            '--consumer-worker', type=str, default='default',
            help='Application consumer that will consume messages from topics.'
        )
        parser.add_argument(
            '--consumer-group', type=str, help='Application consumer group.'
        )
        parser.add_argument(
            '--api-version', type=str,
            default='auto', help='Kafka server api version.'
        )
        parser.add_argument(
            '--take', type=int
        )
        parser.add_argument(
            '--within', type=int
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

    def get_consumer(self, arguments, settings):

        worker = self.get_worker(arguments.consumer_worker, settings)
        if not worker:
            raise ConsumerWorkerLookupError(
                'Worker has not been registered.'
            )

        try:
            consumer_worker = resolve_dotted_name(
                worker['path']
            )
        except:
            raise ConsumerWorkerLookupError(
                'Worker has not been registered.'
            )

        topic_prefix = settings['kafka'].get('topic_prefix')
        if settings['kafka'].get('topic_prefix'):
            worker['topics'] = [
                f'{topic_prefix}{topic}'
                for topic in worker['topics']
            ]

        try:
            consumer = {
                'batch': BatchConsumer,
                'stream': StreamConsumer,
            }[arguments.consumer_type](
                arguments.topics or worker['topics'],
                worker=consumer_worker,
                group_id=arguments.consumer_group or worker['group'],
                api_version=arguments.api_version,
                bootstrap_servers=settings['kafka']['brokers']
            )
        except:
            raise InvalidConsumerType(f'{arguments.consumer_type} is not valid.')

        return get_adapter(consumer, IConsumerUtility, name=arguments.consumer_type)

    async def run(self, arguments, settings, app):
        consumer = self.get_consumer(arguments, settings)
        await consumer.consume(arguments, settings)
