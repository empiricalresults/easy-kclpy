# easy_kclpy
A simpler class interface and launch utils for processing kinesis streams with the Amazon Kinesis Client Library MultiLangDaemon.  It includes a launcher that is fully configured from environment variables.

## Installing the package
The package can be installed with pip like any other package.  Here is a summary for installing it with python3.

First create a virtual environment
```
pip3 install virtualenv
virtualenv -p python3 venv
. venv/bin/activate
```
Then install the package
```
pip3 install --upgrade pip
pip3 install --editable . --process-dependency-links
```

## Running the example

Here's an example of launching an example kinesis worker that dumps the stream to local files.

First configure the environment variables.  
```
export APPLICATION_NAME="LocalFileWriter"
export EXECUTABLE_NAME="python3 easy_kclpy/utils/kinesis_to_file_processor.py"
export STREAM_NAME=[your-kinesis-stream]
```

Then launch the daemon that will run a worker per shard.  
```
python -m easy_kclpy.utils.launch_multilangdaemon -j /usr/bin/java --generate-properties -p kinesis.properties
```
This requires you to have AWS credentials with permission to create a dynamo table and read from the kinesis stream.  The name of the dynamo table will be the same as the name of the app defined in the environment variables above (ie LocalFileWriter).

