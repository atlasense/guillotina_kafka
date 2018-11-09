import json
import backoff
import logging
from guillotina import configure
from guillotina_kafka.consumers import make_consumer
from guillotina.interfaces import ICatalogUtility
from guillotina.component import get_utility
from guillotina_kafka.util import get_kafka_producer
from guillotina_kafka.interfaces import IConsumer

logger = logging.getLogger('guillotina_kafka')


def log_result(result, label):
    if 'errors' in result and result['errors']:
        try:
            if result['error']['caused_by']['type'] in (
                    'index_not_found_exception', 'cluster_block_exception'):
                return  # ignore these...
        except KeyError:
            return
        logger.error(label + ': ' + json.dumps(result))
    else:
        logger.debug(label + ': ' + json.dumps(result))


async def backoff_hdlr(details):
    kafka_producer = get_kafka_producer()
    topic = 'T-elasticsearch-dead-letter'
    await kafka_producer.send(topic, details)


@backoff.on_exception(
    backoff.constant, Exception,
    interval=1, max_tries=5, on_giveup=[backoff_hdlr])
async def es_upsert(conn, index_name, data):
    return await conn.bulk(
            index=index_name, doc_type=None, body=data)


@backoff.on_exception(
    backoff.constant, Exception,
    interval=1, max_tries=5, on_giveup=[backoff_hdlr])
async def es_delete(conn, index_name, data):
    return await conn.bulk(index=index_name, body=data)


@backoff.on_exception(
    backoff.constant, Exception,
    interval=1, max_tries=5, on_giveup=[backoff_hdlr])
async def es_delete_children(conn, index_name, data):
    conn_es = await conn.transport.get_connection()
    async with conn_es._session.post(
            conn_es._base_url.human_repr() + index_name + '/_delete_by_query',
            data=json.dumps(data)) as resp:
        result = await resp.json()
        if 'deleted' in result:
            logger.debug(f'Deleted {result["deleted"]} children')
            logger.debug(f'Deleted {json.dumps(data)}')
        else:
            log_result(result, 'Deletion of children')


async def update_elasticsearch(index_name, action, data):
    util = get_utility(ICatalogUtility)
    return await {
        'index': es_upsert,
        'delete': es_delete,
        'delete_children': es_delete_children,
    }[action](util.conn, index_name, data)


def parser(payload):
    if sorted(payload.keys()) == sorted(['index', 'action', 'data']):
        return payload['action'] in ['index', 'delete', 'delete_children']
    return False


@configure.utility(provides=IConsumer, name='elasticsearch')
class EsConsumer:
    def consumer(self):
        return es_consumer


async def es_consumer(kafka_hosts, topics, group_id='es_consumer'):
    print(f'Starting es_consumer:{group_id} <= {topics!r}')
    consumer = await make_consumer(kafka_hosts, topics, group_id)
    await consumer.start()
    try:
        async for msg in consumer:
            if msg.value and parser(msg.value):
                _ = await update_elasticsearch(
                    msg.value.get('index'),
                    msg.value.get('action'),
                    msg.value.get('data')
                )
    finally:
        print('Stoping consumer ...')
        consumer.stop()
