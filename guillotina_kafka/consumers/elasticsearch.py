from guillotina_kafka.consumers import make_consumer


async def update_elasticsearch(conn, index_name, action, doc):
    print(conn, index_name, action, doc)


async def es_consumer(kafka_hosts, topics, group_id='es_consumer'):
    print(f'Starting es_consumer:{group_id} <= {topics!r}')
    consumer = await make_consumer(kafka_hosts, topics, group_id)
    await consumer.start()
    try:
        async for msg in consumer:
            """
            msg = {
                "action": "index",  # ["index", "delete"]
                "index": "index_name",
                "data": {}
            }
            """
            print(msg)
            if msg.value:
                index = msg.value.get('index')
                action = msg.value.get('action')
                data = msg.value.get('data')
                result = await update_elasticsearch(
                    None, index, action, data)
    finally:
        print('Stoping consumer ...')
        consumer.stop()
