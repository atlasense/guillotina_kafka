import asyncio


async def test_install(guillotina4_kafka_requester):  # noqa
    async with guillotina4_kafka_requester as requester:
        response, _ = await requester('GET', '/db/guillotina/@addons')
        assert 'guillotina4_kafka' in response['installed']
