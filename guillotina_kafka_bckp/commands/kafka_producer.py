from guillotina.commands import Command
from guillotina.component import get_adapter
from guillotina_kafka.utilities.producers import Producer
from guillotina_kafka.utilities.producers.default import DefaultProducerUtility


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

    # async def send(self, arguments, settings):
    #     producer = Producer(
    #         'cil-producer', 
    #         settings['kafka'].get('host', '127.0.0.1'),
    #         settings['kafka'].get('port', 9092),
    #         arguments.topic, max_request_size=arguments.max_size
    #     )
    #     producer = get_adapter(producer, DefaultProducerUtility)
    #     if arguments.interactive:
    #         return (await producer.send())
    #     else:
    #         return (await producer.send_one(arguments.data))

    async def send(self, arguments, settings, app):
        host = settings['kafka']['host']
        port = settings['kafka']['port']
        producer = Producer(
            'cli-producer',
            bootstrap_servers=[f"{host}:{port}"]
        )
        print(producer)
        producer = get_adapter(producer, DefaultProducerUtility)

    async def run(self, arguments, settings, app):
        result = await self.send(arguments, settings, app)
        # result = None
        # try:
        #     result = await self.send(arguments, settings, app)
        # except Exception:
        #     pass
        # if result is not None:
        #     print(result)
