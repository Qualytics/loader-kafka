#!/usr/bin/env python
from setuptools import setup

setup(
    name="loader-kafka",
    version="0.1.0",
    description="Meltano loader for publishing data to a Kafka topic",
    author="Qualytics",
    url="https://github.com/Qualytics/loader-kafka",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["loader_kafka"],
    install_requires=['singer-python>=5.9.1',
                      'kafka-python>=1.4.7',
                      'confluent-kafka>=1.5.0',
                      'voluptuous>=0.10.5'
                      ],
    entry_points="""
    [console_scripts]
    loader-kafka=loader_kafka:main
    """,
    packages=["loader_kafka"],
    include_package_data=True,
)
