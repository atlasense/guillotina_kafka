from guillotina import configure


app_settings = {
    "commands": {
        "kafka-producer": "guillotina4_kafka.commands.kafka_producer.SendMessageCommand",
        "start-consumer": "guillotina4_kafka.commands.start_consumer.StartConsumerCommand"
    },
    "kafka": {
        "host": "localhost",
        "port": 9092
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina4_kafka.api')
    configure.scan('guillotina4_kafka.install')
