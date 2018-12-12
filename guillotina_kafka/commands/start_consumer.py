from guillotina import app_settings
from guillotina.commands import Command
from guillotina.component import get_adapter
from guillotina.utils import resolve_dotted_name
from guillotina_kafka.consumers import Consumer
from guillotina_kafka.consumers import ConsumerLookupError


class StartConsumerCommand(Command):

    description = 'Start Kafka consumer'

    def get_parser(self):
        parser = super(StartConsumerCommand, self).get_parser()
        parser.add_argument(
            '--topics', nargs='*',
            help='Kafka topics to consume from', type=str
        )
        parser.add_argument(
            '--name', type=str,
            help='Application consumer that will consume messages from topics.'
        )
        parser.add_argument(
            '--consumer-group', type=str,
            help='Application consumer group.'
        )
        return parser

    def get_consumer(self, arguments, settings):
        consumer = Consumer(
            arguments.name,
            settings['kafka'].get('host', '127.0.0.1'),
            settings['kafka'].get('port', 9092),
            arguments.consumer_group,
            arguments.topics
        )

        try:
            consumer_interface = app_settings['kafka']['consumers'][arguments.name]  # noqa
            consumer_interface = resolve_dotted_name(consumer_interface)
        except KeyError:
            raise ConsumerLookupError(
                f'Consumer {arguments.name} has not been define.')
        except Exception:
            raise ConsumerLookupError(
                'Could not resolve Interface path for this consumer '
                'please check CONSUMER_INTERFACE_REGISTRY in the settings.'
            )

        return get_adapter(consumer, consumer_interface)

    async def run(self, arguments, settings, app):
        consumer = self.get_consumer(arguments, settings)
        await consumer.consume(arguments=arguments, settings=settings, app=app)
