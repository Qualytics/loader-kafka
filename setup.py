#!/usr/bin/env python
from setuptools import setup

setup(
    name="loader-kafka",
    version="0.1.0",
    description="Meltano loader for extracting data from a Kafka",
    author="Qualytics",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=['jsonschema==2.6.0',
                      'singer-python==2.1.4', 'requests>=2.4.2', 'python-dotenv','kafka-python'],
    entry_points="""
    [console_scripts]
    loader-kafka=loader_kafka:main
    """
)
