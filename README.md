guillotina_kafka Docs
=====================

This package provides a guillotina addon to make your guillotina app support kafka.  
Essentially, it provides two APIs, one for producing to Kafka and another one to consume from Kafka.


Producers
---------

You can get the producer utility as follows:
```python
from guillotina_kafka.utilities import get_kafka_producer
producer = get_kafka_producer()
```
Then set up your producer to suit your requirements.  
You can find the exhaustive list of the configuration options [here](https://aiokafka.readthedocs.io/en/stable/api.html)

```python
await producer.setup(
    request_timeout_ms=5000,
    value_serializer=lambda data: json.dumps(data).encode('utf-8'),
    bootstrap_servers=['kafka-brokers0', ...]
)
```
As it is a singleton, this makes sure there is only one open connection to the Kafka cluster.  
You may then use it across your application to send messages to Kafka.  
Notice how you can specify the topic where you want to send the data to.
```python
await producer.send('my-topic', value={'foo': 'bar'})
```
*The consumer, then, has to be aware of the deserialization needed to retrieve the data.


Consumers
---------

Consumer workers are `python coroutines` that define the logic  
you want to apply on the messages consumed from kafka.

Example 1: Single running consumer per pod

```python
async def my_consumer(msg, arguments=None, settings=None):
    print(f"{msg.topic}:{msg.partition}:{msg.offset}: key={msg.key} value={msg.value}")
    deserialized_msg = json.loads(msg.value.decode('utf-8'))
    await do_something(deserialized_msg)
```

```bash
guillotina -c config.json start-consumer --consumer-type=stream --consumer-worker=es_consumer
```

Example 2: Multiple running consumers per pod

``` python
async def default_worker(
        topic, request, arguments, settings, *args, **kwargs):
    await topic.start()
    async for msg in topic:
        print('default_worker', msg)

async def es_worker(
        topic, request, arguments, settings, *args, **kwargs):
    await topic.start()
    async for msg in topic:
        print('es_worker', msg)
```

```bash
guillotina -c config.json start-consumers --consumer-worker default es
```

Installation and Configuration
------------------------------
```json
{
    "applications": ["guillotina_kafka"],
    "kafka": {
        "topic_prefix": "prefix-",
        "brokers": [
            "brokers.host0:9092",
            ...
        ],
        "consumer": {
            "workers": [
                {
                    "name": "my_consumer",
                    "group": "my_group",
                    "path": "absolut.path.to.my_consumer",
                    "topics": ["my-topic"]
                }
            ],
            "topics": ["default-topic"]
        }
    }
}
```
With this config, `my_consumer` will be consuming data from `my-topic` and `default-topic`.  
It will also collaborate with any consumer using `default_group` as a consumer group.  
You can find more info about `kafka` consumer [here](https://kafka.apache.org/documentation/#theconsumer)

*You can also define a regular expression as a topic. 
```json
{
    "name": "my_consumer",
    "group": "my_group",
    "path": "absolut.path.to.my_consumer",
    "regex_topic": "regex-topic-*"
}
```
With this config `my_consumer` will be consuming data from any topic matching this `regex-topic-*` regular expression.   
```
Example:  
    regex-topic-1  
    regex-topic-2  
    ...
```
The caveat in the case is that `my_consumer` will not consume from `default-topic`

Commands
--------

This package provides your guillotina application with the `start-consumer` command,  
which will look into the app configuration and load the corresponding consumer interface.

For instance, with the consumer config above, you would be able to start a consumer with the following command:
```bash
guillotina -c config.json start-consumer --consumer-type=stream --consumer-worker=es_consumer
```

Run `guillotina -c config.json start-consumer --help` to have the list of available options.  
Important note, options passed through the command line will override the corresponding configurations in config.json
