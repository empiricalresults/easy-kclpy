#!/usr/bin/env python
# Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
"""
This script provides two utility functions:

    ``--print_classpath``
        which prints a java class path. It optionally takes --properties
        and any number of --path options. It will generate a java class path which will include
        the properties file and paths and the location of the KCL jars based on the location of
        the amazon_kclpy.kcl module.

    ``--print_command``
        which prints a command to run an Amazon KCLpy application. It requires a --java
        and --properties argument and optionally takes any number of --path arguments to prepend
        to the classpath that it generates for the command.
"""
import os
import argparse
import sys
import subprocess

from glob import glob
from amazon_kclpy import kcl
from easy_kclpy.utils.generate_kcl_properties_file_from_env import generate_kcl_properties_file_from_env


def get_dir_of_file(f):
    '''
    Returns the absolute path to the directory containing the specified file.

    :type f: str
    :param f: A path to a file, either absolute or relative

    :rtype:  str
    :return: The absolute path of the directory represented by the relative path provided.
    '''
    return os.path.dirname(os.path.abspath(f))

def get_kcl_dir():
    '''
    Returns the absolute path to the dir containing the amazon_kclpy.kcl module.

    :rtype: str
    :return: The absolute path of the KCL package.
    '''
    return get_dir_of_file(kcl.__file__)

def get_kcl_jar_path():
    '''
    Returns the absolute path to the KCL jars needed to run an Amazon KCLpy app.

    :rtype: str
    :return: The absolute path of the KCL jar files needed to run the MultiLangDaemon.
    '''
    return ':'.join(glob(os.path.join(get_kcl_dir(), 'jars', '*jar')))

def get_kcl_classpath(properties=None, paths=[]):
    '''
    Generates a classpath that includes the location of the kcl jars, the
    properties file and the optional paths.

    :type properties: str
    :param properties: Path to properties file.

    :type paths: list
    :param paths: List of strings. The paths that will be prepended to the classpath.

    :rtype: str
    :return: A java class path that will allow your properties to be found and the MultiLangDaemon and its deps and
        any custom paths you provided.
    '''
    # First make all the user provided paths absolute
    paths = [os.path.abspath(p) for p in paths]
    # We add our paths after the user provided paths because this permits users to
    # potentially inject stuff before our paths (otherwise our stuff would always
    # take precedence).
    paths.append(get_kcl_jar_path())
    if properties:
        # Add the dir that the props file is in
        dir_of_file = get_dir_of_file(properties)
        paths.append(dir_of_file)
    return ":".join([p for p in paths if p != ''])

def get_kcl_app_command(java, multi_lang_daemon_class, properties, java_loglevel_properties=None, paths=[]):
    '''
    Generates a command to run the MultiLangDaemon.

    :type java: str
    :param java: Path to java

    :type multi_lang_daemon_class: str
    :param multi_lang_daemon_class: Name of multi language daemon class e.g. com.amazonaws.services.kinesis.multilang.MultiLangDaemon

    :type properties: str
    :param properties: Optional properties file to be included in the classpath.

    :type paths: list
    :param paths: List of strings. Additional paths to prepend to the classpath.

    :rtype: str
    :return: A command that will run the MultiLangDaemon with your properties and custom paths and java.
    '''
    ll = '-Djava.util.logging.config.file={} '.format(java_loglevel_properties) if java_loglevel_properties else ''
    return "{java} -cp {cp} {ll}{daemon} {props}".format(java=java,
                                    cp = get_kcl_classpath(properties, paths),
                                    ll=ll,
                                    daemon = multi_lang_daemon_class,
                                    # Just need the basename becasue the path is added to the classpath
                                    props = os.path.basename(properties))



if __name__ == '__main__':
    parser = argparse.ArgumentParser("A script for generating a command to run an Amazon KCLpy app")

    parser.add_argument("-j", "--java", dest="java",
                        help="The path to the java executable e.g. <some root>/jdk/bin/java",
                        metavar="PATH_TO_JAVA")
    parser.add_argument("-p", "--properties", "--props", "--prop", dest="properties",
                        help="The path to a properties file (relative to where you are running this script)",
                        metavar="PATH_TO_PROPERTIES")
    parser.add_argument("-l", "--java-loglevel-properties", dest="java_loglevel_properties",
                        help="Log level properties file for java runner")
    parser.add_argument("--generate-properties", "--generate-properties", dest="generate_properties",
                        help="Generate the properties file from environment variables",
                        action="store_true", default=False)
    parser.add_argument("-c", "--classpath", "--path", dest="paths", action="append", default=[],
                        help="Additional path to add to java class path. May be specified any number of times",
                        metavar="PATH")
    parser.add_argument("--print-only", "--print-only", dest="print_only",
                        help="Only print the command to run",
                        action="store_true", default=False)
    args = parser.parse_args()


    if args.generate_properties:
        generate_kcl_properties_file_from_env(args.properties)

    if args.java and args.properties:
        multi_lang_daemon_class = 'com.amazonaws.services.kinesis.multilang.MultiLangDaemon'
        kcl_app_command = get_kcl_app_command(args.java, multi_lang_daemon_class, args.properties,
                                  java_loglevel_properties=args.java_loglevel_properties,
                                  paths=args.paths)
        print(kcl_app_command)
        if not args.print_only:
            subprocess.call(kcl_app_command.split(' '))

    else:
        sys.stderr.write("Must provide arguments: --java and --properties\n")
        parser.print_usage()

