import argparse
import os


def env_to_config_case(env_var):
    segs = [seg.capitalize() for seg in env_var.split('_')]
    segs[0] = segs[0].lower()
    return ''.join(segs)


def generate_kcl_properties_file_from_env(filename):
    # start with default vars
    vars = {
        'AWSCredentialsProvider': 'DefaultAWSCredentialsProviderChain',
        'processingLanguage': 'python/3',
        'validateSequenceNumberBeforeCheckpointing': 'true',
        'initialPositionInStream': 'TRIM_HORIZON'
    }

    # now override possible vars
    possible_vars = ('executableName', 'streamName', 'applicationName', 'AWSCredentialsProvider',
                     'processingLanguage', 'initialPositionInStream', 'regionName', 'failoverTimeMillis',
                     'workerId', 'shardSyncIntervalMillis', 'maxRecords', 'idleTimeBetweenReadsInMillis',
                     'callProcessRecordsEvenForEmptyRecordList', 'parentShardPollIntervalMillis',
                     'cleanupLeasesUponShardCompletion', 'taskBackoffTimeMillis', 'metricsBufferTimeMillis',
                     'metricsMaxQueueSize', 'validateSequenceNumberBeforeCheckpointing', 'maxActiveThreads',
                     'shardConsumerDispatchPollIntervalMillis',
                     )
    raw_env_vars = set()
    for k, v in os.environ.items():
        var = env_to_config_case(k)
        if var in possible_vars:
            vars[var] = v
            raw_env_vars.add(k)

    # check that all required vars have been provided
    required_env_vars = ('EXECUTABLE_NAME', 'STREAM_NAME', 'APPLICATION_NAME')
    for required_env_var in required_env_vars:
        if required_env_var not in raw_env_vars:
            raise Exception('Required environment variable {} is not set'.format(required_env_var))

    # generate the file
    with open(filename, 'w') as f:
        f.writelines(sorted(['{} = {}\n'.format(var, value) for var, value in vars.items()]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser("A script for generating a command to run an Amazon KCLpy app")

    parser.add_argument("-p", "--properties", "--props", "--prop", dest="properties",
                        help="The path to a properties file (relative to where you are running this script)",
                        metavar="PATH_TO_PROPERTIES")
    args = parser.parse_args()
    generate_kcl_properties_file_from_env(args.properties)
