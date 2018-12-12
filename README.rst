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


Consumers
---------



Commands
--------


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
