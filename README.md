# easy_kclpy
A simpler class interface and launch utils for processing kinesis streams with the Amazon Kinesis Client Library MultiLangDaemon

## Running a processor
Here's an example of launching a kinesis worker from environment variables
python -m easy_kclpy.utils.launch_multilangdaemon -j /usr/bin/java --generate-properties -p kinesis.properties