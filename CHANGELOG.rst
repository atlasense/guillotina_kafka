Changelog
=========

2.2.0
-----

- Add `guillotina_kafka.interfaces.IKafkaMessageConsumedEvent` event
  [vangheem]


2.1.8
-----

- Make consumer worker config available to the worker
- Properly exit on consumer crash

2.1.7
-----

- Multiple consumers in a single process fix
- Add consumer-stat command to have a global stat on a topic 

2.1.6
-----

- Use configured app settings

2.1.5
-----
- Now we can run multiple consumers in a single process

2.1.4
------
- Remove tasks cancelation on consumer exception.

2.1.3
------
- Really exit the consumer on exception.

2.1.2
------
- Added stat endpoint to consumer.

2.1.1
------
- Code improvement to support consumer subscription to regex topic.