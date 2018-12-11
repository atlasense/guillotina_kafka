from setuptools import find_packages
from setuptools import setup


try:
    README = open('README.rst').read()
except IOError:
    README = None

setup(
    name='guillotina_kafka',
    version="1.0.0",
    description='Guillotina Kafka add-on',
    long_description=README,
    install_requires=[
        'guillotina==3.2.16',
        'kafka-python==1.4.3',
        'aiokafka==0.4.2',
        'backoff==1.6.0',
    ],
    author='Onna',
    author_email='sekou@onna.com',
    url='',
    packages=find_packages(exclude=['demo']),
    include_package_data=True,
    extras_require={
        'test': [
            'pytest',
            'docker',
            'backoff',
            'psycopg2',
            'pytest-asyncio>=0.8.0',
            'pytest-aiohttp',
            'pytest-cov',
            'coverage>=4.4',
            'pytest-docker-fixtures>=1.2.7',
            'cassettedeck==1.1.12',
        ]
    },
    classifiers=[],
    entry_points={
    }
)
