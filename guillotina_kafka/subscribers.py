from guillotina.interfaces import IApplicationCleanupEvent
from guillotina.component import get_utility
from guillotina import configure
from guillotina_kafka import IKafkaProducerUtility

@configure.subscriber(for_=(IApplicationCleanupEvent))
async def app_cleanup(event):
    util = get_utility(IKafkaProducerUtility)
    try:
        await util.stop_active_consumers()
    except Exception:
        pass
