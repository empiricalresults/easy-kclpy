"""
Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
"""

from setuptools import setup, find_packages

PACKAGE_NAME = 'easy-kclpy'

if __name__ in ('__main__', 'builtins'):
    __version__ = None
    exec(open("easy_kclpy/version.py").read())
    assert __version__

    setup(
        name=PACKAGE_NAME,
        version=__version__,
        description='A simpler class interface and launch utils for processing kinesis '
                    'streams with the Amazon Kinesis Client Library MultiLangDaemon',
        license='Amazon Software License',
        packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
        install_requires=[
            "amazon-kclpy==2.0.2"
        ],
        author='david-matheson',
        author_email='david@empiricalresults.com',
        url='https://github.com/empiricalresults/easy-kclpy',
        # download_url='https://github.com/empiricalresults/kclpy/archive/0.1.2.tar.gz',
        keywords=['amazon', 'kinesis', 'kinesis-client-library', 'client-library', 'library'],
        classifiers=[
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
        ]
    )
