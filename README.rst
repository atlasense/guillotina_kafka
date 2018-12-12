guillotina_kafka Docs
=====================

This package provides a guillotina addon to make your guillotina app
support kafka. Essentially, it registers two utilities (singletons),
one for producing to Kafka and another one to consume from Kafka.


Producers
---------

You can get the producer utility as follows::

  from guillotina_kafka.utilities import get_kafka_producer

  producer = get_kafka_producer()

As it is a singleton, this makes sure there is only one open
connection to the Kafka cluster.

You may then use it across your application to send messages to
Kafka. Notice how you can specify the topic where you want to send the
data to, and the serialization to be used::

  await producer.send('my-topic',
                      data={'foo': 'bar'},
                      serializer=lambda x: json.dump(x).encode())

The consumer, then, has to be aware of the deserialization needed to
retrieve the data.

Consumers
---------

The `KafkaConsumer` base implementation is provided with this package,
which keeps a connection open to the Kafka cluster. However, you may
write custom adapters for it to extend the consumer behavior, such as
it's done in the `ITemplateConsumer`.

The adapter implementation could set group, topics and deserializer
for the messages that is expected to read in each case. For instance::

  def IMyOwnConsumer(Interface):
      async def consume(self, **kwargs):
          pass

  @configure.adapter(for_=IKafkaConsumer, provides=IMyOwnConsumer)
  class MyOwnConsumer:
    def __init__(self, consumer: KafkaConsumer):
        self.consumer = consumer

    async def do_my_onw_stuff(self, message):
        pass

    async def consume(self, **kwargs):
        print('Started My Own Consumer.')
        try:
            async for message in self.consumer:
                await self.do_my_own_stuff(message)
        finally:
            await self.consumer.stop()
            print('Stoped My Own Consumer')

Then you can instantiate your own consumer by getting the
corresponding adapter to the `KafkaConsumer` object. For instance::

  from guillotina.component import get_adapter
  from my_package.consumer import IMyOwnConsumer
  from guillotina_kafka.consumers import KafkaConsumer
  import json

  consumer = KafkaConsumer(
      'my-app',
      topics=['mytopic'],
      group='consumergroup',
      deserializer=lamda x: json.load(x),
  )

  own_consumer = get_adapter(consumer, IMyOwnConsumer)
  await own_consumer.consume()


Commands
--------

This package provides your guillotina application with the
`kafka-consumer` command, which will look into the app configuration
and load the corresponding consumer interface.

For instance, if we had registered our own consumer in the app settings config.json::

  app_settings = {
      "kafka": {
          "host": "localhost",
          "port": 9092,
          "consumers": {
              "my_own": "my_package.consumer.IMyOwnConsumer"
          }
      }
  }

Then you would be able to start a consumer with the following command::

  guillotina -c config.json kafka-consumer --name my_own --consumer-group test-group --topics topic1 topic2

This makes it easy to write custom consumers and launch them with the
generic command. The only requirement for this to work is to have the
`consume` method implemented in your consumer class, and have the
interface registered with a specified name (`my_own`) in the app's
config settings.


Installation and Configuration
------------------------------

You can install this addon by add adding `guillotina_kafka` into your
`applications`::

  {
     "applications": ["guillotina_kafka"],
     "kafka": {
         "host": "localhost",
         "port": "9092"
     }
  }
