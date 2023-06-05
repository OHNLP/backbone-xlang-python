import setuptools
from setuptools import setup

setup(
    name="ohnlp-backbone-xlang-python",
    version="1.0.2",
    description="Python support for OHNLP Toolkit Backbone Components",
    author="Andrew Wen",
    author_email="contact@ohnlp.org",
    packages=setuptools.find_packages(),
    install_requires=[
        'certifi==2023.5.7',
        'charset-normalizer==3.1.0',
        'cloudpickle==2.2.1',
        'crcmod==1.7',
        'dill==0.3.1.1',
        'dnspython==2.3.0',
        'docopt==0.6.2',
        'fastavro==1.7.4',
        'fasteners==0.18',
        'grpcio==1.54.0',
        'hdfs==2.7.0',
        'httplib2==0.21.0',
        'idna==3.4',
        'numpy==1.24.3',
        'objsize==0.6.1',
        'orjson==3.8.12',
        'proto-plus==1.22.2',
        'protobuf==4.22.5',
        'py4j==0.10.9.7',
        'pyarrow==11.0.0',
        'pydot==1.4.2',
        'pymongo==4.3.3',
        'pyparsing==3.0.9',
        'python-dateutil==2.8.2',
        'pytz==2023.3',
        'regex==2023.5.5',
        'requests==2.30.0',
        'six==1.16.0',
        'typing_extensions==4.5.0',
        'urllib3==2.0.2',
        'zstandard==0.21.0'
    ]
)
