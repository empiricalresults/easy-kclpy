#!/usr/bin/env python
# Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import print_function

import logging
import subprocess

from amazon_kclpy import kcl
from glob import glob
import os
import argparse
import sys

from easy_kclpy.utils.generate_kcl_properties_file_from_env import generate_kcl_properties_file_from_env

log = logging.getLogger(__name__)

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


def get_kcl_app_command(java, multi_lang_daemon_class, properties, log_configuration, paths=[]):
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
    return "{java} -cp {cp} {daemon} {props} {log_config}".format(
        java=java,
        cp=get_kcl_classpath(args.properties, paths),
        daemon=multi_lang_daemon_class,
        props=properties,
        log_config=log_configuration
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser("A script for generating a command to run an Amazon KCLpy app")

    parser.add_argument("-j", "--java", dest="java",
                        help="The path to the java executable e.g. <some root>/jdk/bin/java",
                        metavar="PATH_TO_JAVA")
    parser.add_argument("-p", "--properties", "--props", "--prop", dest="properties",
                        help="The path to a properties file (relative to where you are running this script)",
                        metavar="PATH_TO_PROPERTIES")
    parser.add_argument("-c", "--classpath", "--path", dest="paths", action="append", default=[],
                        help="Additional path to add to java class path. May be specified any number of times",
                        metavar="PATH")
    parser.add_argument("-l", "--log-configuration", dest="log_configuration",
                        help="This will use the logback.xml which will be used by the KCL to log.",
                        metavar="PATH_TO_LOG_CONFIGURATION")

    parser.add_argument("--generate-properties", "--generate-properties", dest="generate_properties",
                        help="Generate the properties file from environment variables",
                        action="store_true", default=False)

    parser.add_argument("--print-only", "--print-only", dest="print_only",
                        help="Only print the command to run",
                        action="store_true", default=False)

    args = parser.parse_args()

    if args.generate_properties:
        generate_kcl_properties_file_from_env(args.properties)

    if args.java and args.properties:
        multi_lang_daemon_class = 'software.amazon.kinesis.multilang.MultiLangDaemon'
        properties_argument = "--properties-file {props}".format(props=args.properties)
        log_argument = ''
        if args.log_configuration is not None:
            log_argument = "--log-configuration {log}".format(log=args.log_configuration)

        kcl_app_command = get_kcl_app_command(
            args.java, multi_lang_daemon_class, properties_argument, log_argument, paths=args.paths)
        print(kcl_app_command)
        if not args.print_only:
            subprocess.call(kcl_app_command.split(' '))
    else:
        log.error("Must provide arguments: --java and --properties")
        parser.print_usage()
