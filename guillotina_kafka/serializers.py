import json


def json_serialize(data):
    return json.dumps(data).encode('utf-8')


def str_serializer(data):
    return data.encode('utf-8')
