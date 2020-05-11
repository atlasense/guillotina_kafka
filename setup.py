from setuptools import find_packages
from setuptools import setup


try:
    README = open('README.rst').read()
except IOError:
    README = None

try:
    VERSION = open('VERSION').read().strip('\n')
except IOError:
    VERSION = None

setup(
    name='guillotina_kafka',
    version=VERSION,
    description='Guillotina Kafka add-on',
    long_description=README,
    install_requires=[
        'guillotina==6.0.0b3',
        'aiokafka'
    ],
    author='Sekou Oumar',
    author_email='sekou@onna.com',
    url='',
    packages=find_packages(exclude=['demo']),
    include_package_data=True,
    tests_require=[
        'pytest',
    ],
    extras_require={
        'test': [
            'aiohttp',
            'pytest',
            'docker',
            'backoff',
            'psycopg2-binary',
            'pytest-asyncio>=0.8.0',
            'pytest-cov',
            'coverage>=4.4',
            'pytest-docker-fixtures>=1.2.7',
            'async_asgi_testclient',
        ]
    },
    classifiers=[],
    entry_points={
    }
)
