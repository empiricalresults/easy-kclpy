import logging
from time import sleep

from amazon_kclpy import kcl
from amazon_kclpy.v3 import processor

log = logging.getLogger(__name__)


class PerRecordProcessorBase(processor.RecordProcessorBase):
    """
    A RecordProcessor processes data from a shard in a stream. Its methods will be called with this pattern:

    * initialize will be called once
    * process_records will be called zero or more times
    * shutdown will be called if this MultiLangDaemon instance loses the lease to this shard, or the shard ends due
        a scaling change.
    """
    def __init__(self, **kwargs):
        self.checkpoint_error_sleep_seconds = kwargs.get('checkpoint_error_sleep_seconds', 5)
        self.checkpoint_retries = kwargs.get('checkpoint_retries', 5)
        self.shard_id = None
        self._largest_seq = (None, None)
        self._largest_sub_seq = None
        self._last_checkpoint_time = None

    def initialize(self, initialize_input):
        """
        Called once by a KCLProcess before any calls to process_records

        :param amazon_kclpy.messages.InitializeInput initialize_input: Information about the lease that this record
            processor has been assigned.
        """
        shard_id = initialize_input.shard_id.replace('shardId-', '')
        log.info("Initialize {}".format(shard_id))
        self.shard_id = shard_id
        self._largest_seq = (None, None)

    def checkpoint(self, checkpointer, sequence_number=None, sub_sequence_number=None):
        """
        Checkpoints with retries on retryable exceptions.

        :param amazon_kclpy.kcl.Checkpointer checkpointer: the checkpointer provided to either process_records
            or shutdown
        :param str or None sequence_number: the sequence number to checkpoint at.
        :param int or None sub_sequence_number: the sub sequence number to checkpoint at.
        """
        log.debug('Checkpointing {}'.format(self.shard_id))

        self.before_checkpoint(sequence_number, sub_sequence_number)

        for n in range(0, self.checkpoint_retries):
            try:
                checkpointer.checkpoint(sequence_number, sub_sequence_number)
                break
            except kcl.CheckpointError as e:
                if 'ShutdownException' == e.value:
                    #
                    # A ShutdownException indicates that this record processor should be shutdown. This is due to
                    # some failover event, e.g. another MultiLangDaemon has taken the lease for this shard.
                    #
                    log.error('Encountered shutdown exception, skipping checkpoint')
                    return
                elif 'ThrottlingException' == e.value:
                    #
                    # A ThrottlingException indicates that one of our dependencies is is over burdened, e.g. too many
                    # dynamo writes. We will sleep temporarily to let it recover.
                    #
                    if self.checkpoint_retries - 1 == n:
                        log.error('Failed to checkpoint after {n} attempts, giving up.'.format(n=n))
                        return
                    else:
                        log.error('Was throttled while checkpointing, will attempt again in {s} seconds'
                                  .format(s=self.checkpoint_error_sleep_seconds))
                elif 'InvalidStateException' == e.value:
                    log.error('MultiLangDaemon reported an invalid state while checkpointing.')
                else:  # Some other error
                    log.error('Encountered an error while checkpointing: {e}.'.format(e=e))
            sleep(self.checkpoint_error_sleep_seconds)

        self.after_checkpoint(sequence_number, sub_sequence_number)

    def process_record(self, data, partition_key, sequence_number, sub_sequence_number, approximate_arrival_timestamp):
        """
        Called for each record that is passed to process_records.

        :param str data: The blob of data that was contained in the record.
        :param str partition_key: The key associated with this record.
        :param int sequence_number: The sequence number associated with this record.
        :param int sub_sequence_number: the sub sequence number associated with this record.
        :param approximate_arrival_timestamp: the time the record arrived
        """
        raise NotImplementedError

    def should_update_sequence(self, sequence_number, sub_sequence_number):
        """
        Determines whether a new larger sequence number is available

        :param int sequence_number: the sequence number from the current record
        :param int sub_sequence_number: the sub sequence number from the current record
        :return boolean: true if the largest sequence should be updated, false otherwise
        """
        return self._largest_seq == (None, None) or sequence_number > self._largest_seq[0] or \
            (sequence_number == self._largest_seq[0] and sub_sequence_number > self._largest_seq[1])

    def process_records(self, process_records_input):
        """
        Called by a KCLProcess with a list of records to be processed and a checkpointer which accepts sequence numbers
        from the records to indicate where in the stream to checkpoint.

        :param amazon_kclpy.messages.ProcessRecordsInput process_records_input: the records, and metadata about the
            records.
        """

        for record in process_records_input.records:
            data = record.binary_data
            seq = int(record.sequence_number)
            sub_seq = record.sub_sequence_number
            key = record.partition_key
            aat = record.approximate_arrival_timestamp

            try:

                self.process_record(data, key, seq, sub_seq, aat)
                if self.should_update_sequence(seq, sub_seq):
                    self._largest_seq = (seq, sub_seq)

            except Exception as e:
                log.exception("Encountered an exception while processing record - skipping the record: {e}".format(e=e))

        if self.should_checkpoint():
            self.checkpoint(process_records_input.checkpointer, str(self._largest_seq[0]), self._largest_seq[1])

    def lease_lost(self, lease_lost_input):
        log.warning("Lease has been lost.")

    def shard_ended(self, shard_ended_input):
        log.warning("Shard has ended. Checkpointing...")
        shard_ended_input.checkpointer.checkpoint()

    def shutdown_requested(self, shutdown_requested_input):
        log.warning("Shutdown has been requested. Checkpointing...")
        shutdown_requested_input.checkpointer.checkpoint()

    def should_checkpoint(self):
        raise NotImplementedError

    def before_checkpoint(self, sequence_number, sub_sequence_number):
        pass

    def after_checkpoint(self, sequence_number, sub_sequence_number):
        pass
