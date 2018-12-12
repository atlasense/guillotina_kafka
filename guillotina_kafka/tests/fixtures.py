from guillotina import app_settings
from guillotina import testing

import pytest


base_kafka_settings = {
    "host": "localhost",
    "port": 9092,
}


def base_settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].append('guillotina_kafka')
    else:
        settings['applications'] = ['guillotina_kafka']
    settings['kafka'] = base_kafka_settings


testing.configure_with(base_settings_configurator)


@pytest.fixture('function')
async def kafka_consumer(loop, kafka_container):
    # Create consumer and pass in pytest loop
    consumer = None
    yield consumer


@pytest.fixture('function')
def kafka_container(kafka):
    app_settings.setdefault('kafka', {})
    app_settings['kafka'].update({
        "host": kafka[0],
        "port": kafka[1],
    })
    yield kafka
