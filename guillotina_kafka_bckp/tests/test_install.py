import asyncio


async def test_install(guillotina_kafka_requester):  # noqa
    async with guillotina_kafka_requester as requester:
        response, _ = await requester('GET', '/db/guillotina/@addons')
        assert 'guillotina_kafka' in response['installed']
