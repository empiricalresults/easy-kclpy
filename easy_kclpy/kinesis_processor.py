import logging
from time import sleep

from amazon_kclpy import kcl
from amazon_kclpy.v2 import processor

log = logging.getLogger(__name__)


class PerRecordProcessorBase(processor.RecordProcessorBase):
    def __init__(self, **kwargs):
        self.checkpoint_error_sleep_seconds = kwargs.get('checkpoint_error_sleep_seconds', 5)
        self.checkpoint_retries = kwargs.get('checkpoint_retries', 5)
        self.shard_id = None

    def initialize(self, initialize_input):
        """
        Called once by a KCLProcess before any calls to process_records

        :param amazon_kclpy.messages.InitializeInput initialize_input: Information about the lease that this record
            processor has been assigned.
        """
        shard_id = initialize_input.shard_id.replace('shardId-', '')
        log.info("Initialize {}".format(shard_id))
        self.shard_id = shard_id

    def process_record(self, data, partition_key,
                       sequence_number, sub_sequence_number,
                       approximate_arrival_timestamp):
        """
        Called for each record that is passed to process_records.

        :param str data: The blob of data that was contained in the record.
        :param str partition_key: The key associated with this recod.
        :param str sequence_number: The sequence number associated with this record.
        :param str sub_sequence_number: the sub sequence number associated with this record.
        :param approximate_arrival_timestamp: the time the record arrived
        """
        raise NotImplementedError

    def should_checkpoint(self):
        raise NotImplementedError

    def before_checkpoint(self, sequence_number, sub_sequence_number):
        pass

    def after_checkpoint(self, sequence_number, sub_sequence_number):
        pass

    def process_records(self, process_records_input):
        """
        Called by a KCLProcess with a list of records to be processed and a checkpointer which accepts sequence numbers
        from the records to indicate where in the stream to checkpoint.

        :param amazon_kclpy.messages.ProcessRecordsInput process_records_input: the records, and metadata about the
            records.
        """
        try:
            for record in process_records_input.records:
                data = record.binary_data
                key = record.partition_key
                seq = record.sequence_number
                sub_seq = record.sub_sequence_number
                aat = record.approximate_arrival_timestamp
                self.process_record(data, key, seq, sub_seq, aat)
                if self.should_checkpoint():
                    self.checkpoint(process_records_input.checkpointer, seq, sub_seq)
        except Exception as e:
            log.error("Encountered an exception while processing records. Exception was {e}".format(e=e))

    def checkpoint(self, checkpointer, sequence_number=None, sub_sequence_number=None):
        """
        Checkpoints with retries on retryable exceptions.

        :param amazon_kclpy.kcl.Checkpointer checkpointer: the checkpointer provided to either process_records
            or shutdown
        :param str or None sequence_number: the sequence number to checkpoint at.
        :param int or None sub_sequence_number: the sub sequence number to checkpoint at.
        """
        log.info('Checkpointing {}'.format(self.shard_id))
        self.before_checkpoint(sequence_number, sub_sequence_number)
        for n in range(0, self.checkpoint_retries):
            try:
                checkpointer.checkpoint(sequence_number, sub_sequence_number)
                self.after_checkpoint(sequence_number, sub_sequence_number)
                return
            except kcl.CheckpointError as e:
                if 'ShutdownException' == e.value:
                    #
                    # A ShutdownException indicates that this record processor should be shutdown. This is due to
                    # some failover event, e.g. another MultiLangDaemon has taken the lease for this shard.
                    #
                    print('Encountered shutdown exception, skipping checkpoint')
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
                    log.error('Encountered an error while checkpointing, error was {e}.'.format(e=e))
            sleep(self.checkpoint_error_sleep_seconds)

    def shutdown(self, shutdown_input):
        """
        Called by a KCLProcess instance to indicate that this record processor should shutdown. After this is called,
        there will be no more calls to any other methods of this record processor.

        As part of the shutdown process you must inspect :attr:`amazon_kclpy.messages.ShutdownInput.reason` to
        determine the steps to take.

            * Shutdown Reason ZOMBIE:
                **ATTEMPTING TO CHECKPOINT ONCE A LEASE IS LOST WILL FAIL**

                A record processor will be shutdown if it loses its lease.  In this case the KCL will terminate the
                record processor.  It is not possible to checkpoint once a record processor has lost its lease.
            * Shutdown Reason TERMINATE:
                **THE RECORD PROCESSOR MUST CHECKPOINT OR THE KCL WILL BE UNABLE TO PROGRESS**

                A record processor will be shutdown once it reaches the end of a shard.  A shard ending indicates that
                it has been either split into multiple shards or merged with another shard.  To begin processing the new
                shard(s) it's required that a final checkpoint occurs.


        :param amazon_kclpy.messages.ShutdownInput shutdown_input: Information related to the shutdown request
        """
        try:
            if shutdown_input.reason == 'TERMINATE':
                # Checkpointing with no parameter will checkpoint at the
                # largest sequence number reached by this processor on this
                # shard id
                log.info('Was told to terminate, will attempt to checkpoint.')
                self.checkpoint(shutdown_input.checkpointer, None)
            else:  # reason == 'ZOMBIE'
                log.info('Shutting down due to failover. Will not checkpoint.')
        except:
            pass
