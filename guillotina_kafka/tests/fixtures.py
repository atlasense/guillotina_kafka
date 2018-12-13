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
def kafka_container(kafka):
    app_settings.setdefault('kafka', {})
    app_settings['kafka'].update({
        "host": kafka[0],
        "port": kafka[1],
    })
    yield kafka
    import subprocess
    from subprocess import check_output
    cmd_path = '/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh'
    image_id = check_output(["docker ps | grep spotify | awk '{print $1}'"], shell=True)  # noqa
    if image_id:
        image_id = image_id.decode('utf-8').splitlines()[-1]
        topics_list_cmd = f"docker exec -it {image_id} {cmd_path} --zookeeper 127.0.0.1:2181 --list"  # noqa
        topics_list = check_output([topics_list_cmd], shell=True)
        topics_list = topics_list.decode('utf-8')
        for topic in topics_list:
            topic = topic.decode('utf-8').splitlines()[-1]
            if topic != "__consumer_offsets":
                subprocess.Popen(
                    [f"docker exec -it {image_id} {cmd_path} --zookeeper 127.0.0.1:2181 --delete --topic {topic}"],  # noqa
                    shell=True)