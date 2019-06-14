import json
import logging
import os

from amazon_kclpy import kcl
from easy_kclpy.kinesis_processor import PerRecordProcessorBase

log = logging.getLogger(__name__)


class KinesisToFileProcessor(PerRecordProcessorBase):
    def __init__(self, filename_prefix, **kwargs):
        """
        Args:
            filename (str) Name of file to dump kinesis data to
        """
        super(KinesisToFileProcessor, self).__init__(**kwargs)
        self.filename_prefix = filename_prefix
        self.file = None

    def initialize(self, initialize_input):
        super(KinesisToFileProcessor, self).initialize(initialize_input)
        filename = '{}_{}.jsonl'.format(self.filename_prefix, initialize_input.shard_id)
        self.file = open(filename, 'w')

    def process_record(self, data, partition_key,
                       sequence_number, sub_sequence_number,
                       approximate_arrival_timestamp):
        output = json.dumps({'data': data.decode('utf-8'),
                             'partition_key': partition_key,
                             'sequence_number': sequence_number,
                             'sub_sequence_number': sub_sequence_number,
                             'approximate_arrival_timestamp': str(approximate_arrival_timestamp)})
        self.file.write('{}\n'.format(output))

    def should_checkpoint(self):
        return True

    def before_checkpoint(self, sequence_number, sub_sequence_number):
        self.file.flush()


def main():
    kinesis_output_filename_prefix = os.getenv("KINESIS_OUTPUT_FILENAME_PREFIX", 'kinesis_dump')
    log_per_record_processor = KinesisToFileProcessor(filename_prefix=kinesis_output_filename_prefix)
    kcl_process = kcl.KCLProcess(log_per_record_processor)
    kcl_process.run()


if __name__ == '__main__':
    main()
