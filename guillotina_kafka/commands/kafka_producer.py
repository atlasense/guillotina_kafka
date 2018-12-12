from guillotina.commands import Command
from guillotina_kafka.producers import CliSendMessage
from guillotina_kafka.utilities import get_kafka_producer


class SendMessageCommand(Command):

    description = 'Start Kafka producer'

    def get_parser(self):
        parser = super(SendMessageCommand, self).get_parser()
        parser.add_argument(
            '--topic', type=str, help='Kafka topic to produce to.'
        )
        parser.add_argument(
            '--data', type=str, help='Data to send to the topic.'
        )
        parser.add_argument(
            '--max-size', type=int, default=104857600,
            help='The maximum size of a request.'
        )
        parser.add_argument(
            '-i', '--interactive', action='store_true', default=False)
        return parser

    async def send(self, arguments):
        utility = get_kafka_producer()
        utility.max_request_size = arguments.max_size
        producer = CliSendMessage(utility)
        if arguments.interactive:
            return (await producer.send(arguments.topic))
        else:
            return (await producer.send_one(arguments.topic, arguments.data))

    async def run(self, arguments, settings, app):
        result = await self.send(arguments)
        if result is not None:
            print(result)
