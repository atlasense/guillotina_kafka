from guillotina.commands import Command
from guillotina_kafka.consumers.elasticsearch import es_consumer
from guillotina_kafka.consumers.generic import generic_consumer
from guillotina.component import get_utilities_for, get_utility
from guillotina_kafka.interfaces import IConsumer


consumer_app_map = {
    'es_consumer': es_consumer
}


class ConsumerCommand(Command):

    description = 'Kafka consumer'

    def get_parser(self):
        parser = super(ConsumerCommand, self).get_parser()
        parser.add_argument(
            '--topics', nargs='*',
            help='Kafka topic to consume from', type=str)
        parser.add_argument(
            '--consumer-app', type=str,
            help='Application consumer that do work with the data consumed.')
        parser.add_argument(
            '--consumer-group', type=str,
            help='Application consumer group.')
        return parser

    async def run(self, arguments, settings, app):
        import pdb; pdb.set_trace()

        consumer_name = arguments.consumer_app
        consumers = [name for (name, _) in get_utilities_for(IConsumer)]
        if consumer_name not in consumers:
            raise Exception(f'Selected consumer "{consumer_name}" is not registered.')

        consumer = get_utility(IConsumer, name=consumer_name)
        await consumer.consumer()(
            '{host}:{port}'.format(**settings['kafka']),
            arguments.topics,
            group_id=arguments.consumer_group)
