import aiotask_context
import json
import pytest
import asyncio

pytestmark = pytest.mark.asyncio

async def test_kafka_producer(event_loop, kafka_container):
    pytest.skip('WIP')
    pass
    # aiotask_context.set('request', dummy_request)

    # TEST_TOPIC = 'test-topic-api'

    # async with container_requester as requester:
    #     resp, status = await requester(
    #         'POST', f'/db/guillotina/@kafka-producer/{TEST_TOPIC}',
    #         data=json.dumps({'foo': 'bar'})
    #     )
    #     assert status == 200
    #     assert resp['topic'] == TEST_TOPIC
    #     assert resp['offset'] == 0

    # aiotask_context.set('request', None)