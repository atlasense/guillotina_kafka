import logging

import aiokafka
from guillotina import app_settings
from guillotina.commands.server import ServerCommand

logger = logging.getLogger(__name__)


class ConsumerStatCommand(ServerCommand):

    description = "Start Kafka consumer"

    def get_parser(self):
        parser = super(ConsumerStatCommand, self).get_parser()
        parser.add_argument(
            "--consumer-worker",
            type=str,
            default="default",
            nargs="+",
            help="Application consumer that will consume messages from topics.",
        )
        return parser

    def get_worker(self, name):
        for worker in app_settings["kafka"]["consumer"]["workers"]:
            if name == worker["name"]:
                worker = {
                    **worker,
                    "topics": list(
                        {
                            *worker.get("topics", []),
                            *app_settings["kafka"]["consumer"].get("topics", []),
                        }
                    ),
                }
                return worker
        return {}

    async def run_stat(self, worker):
        topic_prefix = app_settings["kafka"].get("topic_prefix", "")
        consumer = aiokafka.AIOKafkaConsumer(
            *[f"{topic_prefix}{topic}" for topic in worker["topics"]],
            loop=self.get_loop(),
            bootstrap_servers=app_settings["kafka"]["brokers"],
        )
        await consumer.start()
        position_assignment = list(consumer.assignment())
        beginning_offsets = await consumer.beginning_offsets(position_assignment)
        end_offsets = await consumer.end_offsets(position_assignment)

        for tp in position_assignment:
            position = await consumer.position(tp)
            beginning = beginning_offsets[tp]
            end = end_offsets[tp]
            print(
                f'{tp}:consumer_group{worker.get("group", "default")}: '
                f"beginning={beginning}, end={end}, position={position}, "
                f"to_process={end - position}"
            )

        await consumer.stop()

    async def run(self, arguments, settings, app):
        for worker_name in arguments.consumer_worker:
            worker = self.get_worker(worker_name)
            if worker:
                await self.run_stat(worker)
