from guillotina.commands import Command
from guillotina_kafka.consumers.elasticsearch import es_consumer


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
        consumer = consumer_app_map[arguments.consumer_app]
        await consumer(
            '{host}:{port}'.format(**settings['kafka']),
            arguments.topics,
            group_id=arguments.consumer_group)
