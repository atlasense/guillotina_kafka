from guillotina.commands import Command
from guillotina.component import get_adapter
from guillotina_kafka.producer import Producer
from guillotina_kafka.producer import ICliSendMessage


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

    async def send(self, arguments, settings):
        producer = Producer(
            settings['kafka'].get('host', '127.0.0.1'),
            settings['kafka'].get('port', 9092),
            arguments.topic,
            max_request_size=arguments.max_size
        )
        producer = get_adapter(producer, ICliSendMessage)
        if arguments.interactive:
            return (await producer.send())
        else:
            return (await producer.send_one(arguments.data))

    async def run(self, arguments, settings, app):
        result = await self.send(arguments, settings)
        if result is not None:
            print(result)
