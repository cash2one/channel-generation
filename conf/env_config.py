"""
##
# @file env_config.py
# @Synopsis  config environment
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-06
"""
import sys
sys.path.append('..')
import os
import datetime as dt

class EnvConfig(object):
    """
    # @Synopsis  config environment
    """
    DEBUG = True
    SMS_RECEIVERS = ['18612861842']
    MAIL_RECEIVERS = ['guming02@baidu.com']
    # tv corresponds to tvplay in some cases, to tv itself in others...wtf, and
    # there is not such a word as 'tvplay', should be 'tvshow', I use 'tv' as the
    # only key correspond to the meaning of tvshow in this project
    WORKS_TYPE_ALIAS_DICT = dict({
        'movie': 'movie',
        'tv': 'tvplay',
        'comic': 'comic',
        'show': 'show',
        })

    #find project root path according to the path of this config file
    CONF_PATH = os.path.split(os.path.realpath(__file__))[0]
    # if the 'conf' module is provided to spark-submit script in a .zip file,
    # the real path of this file would be project_path/conf.zip/conf(refer to
    # the dal.spark_submit module), while the
    # real path of config file we wanna locate is project_path/conf, thus the
    # following transformation would be neccessary.
    if '.zip' in CONF_PATH:
        path_stack = CONF_PATH.split('/')
        CONF_PATH = '/'.join(path_stack[:-2]) + '/conf'
    PROJECT_PATH = os.path.join(CONF_PATH, '../')
    LOG_PATH = os.path.join(PROJECT_PATH, 'log')
    GENERAL_LOG_FILE = os.path.join(LOG_PATH, 'general.log')
    #script path

    #tool path
    if DEBUG:
        TOOL_PATH = '/home/video/guming02/tools/'
    else:
        TOOL_PATH = '/home/video/guming/tools'
    HADOOP_PATH = os.path.join(TOOL_PATH, 'hadoop-client')
    HADOOP_JAVA_HOME = os.path.join(HADOOP_PATH, 'java6')
    HADOOP_BIN = os.path.join(HADOOP_PATH, 'hadoop/bin/hadoop')
    SPARK_SUBMIT_BIN = os.path.join(TOOL_PATH, 'spark-client/bin/spark-submit')
    JAVA_HOME = os.path.join(TOOL_PATH, 'hadoop-client/java6')
    MYSQL_BIN = 'mysql'
    MOLA_PATH = os.path.join(TOOL_PATH, 'mola')
    SMS_BIN = 'gsmsend'

    #HDFS input and output path
    HDFS_ROOT_PATH = "/app/vs/ns-video/"
    #user behavior log path
    HDFS_PC_PATH = os.path.join(HDFS_ROOT_PATH,
            'video-pc-data/vd-pc/behavior2/')
    HDFS_MOBILE_PATH = os.path.join(HDFS_ROOT_PATH,
            'video-mobile-data/vd-mobile/android-behavior-rec/')
    HDFS_PC_PLAY_LOG_PATH_DICT = dict()
    for works_type in WORKS_TYPE_ALIAS_DICT:
        HDFS_PC_PLAY_LOG_PATH_DICT[works_type] = os.path.join(HDFS_PC_PATH,
                'play/{0}/'.format(WORKS_TYPE_ALIAS_DICT[works_type]))
    HDFS_PC_PLAY_LOG_PATH_DICT['short'] = os.path.join(HDFS_PC_PATH,
            'play/short/')
    HDFS_MOBILE_PLAY_LOG_PATH = os.path.join(HDFS_MOBILE_PATH, 'play/')
    #long video meta information path
    HDFS_LONG_VIDEO_INFO_PATH = os.path.join(HDFS_ROOT_PATH,
            'guming/long-video-info/')
    HDFS_LONG_VIDEO_INFO_PATH_DICT = dict()
    for works_type in WORKS_TYPE_ALIAS_DICT:
        HDFS_LONG_VIDEO_INFO_PATH_DICT[works_type] = os.path.join(
                HDFS_LONG_VIDEO_INFO_PATH, '{0}-info'.format(works_type))
    #derivant output path
    HDFS_DERIVANT_PATH = os.path.join(HDFS_ROOT_PATH,
            'guming/channel-generation/')
    HDFS_CHANNEL_PATH_DICT = dict()
    for works_type in WORKS_TYPE_ALIAS_DICT:
        HDFS_CHANNEL_PATH_DICT[works_type] = os.path.join(
                HDFS_DERIVANT_PATH, '{0}_channel'.format(works_type))
    #final output path
    HDFS_FINAL_PATH = os.path.join(HDFS_ROOT_PATH, \
            'video-pc-result/vr-pc-play-daily-trackpush-spark/%s/' % \
            dt.date.today().strftime('%Y%m%d'))
    HDFS_CHANNEL_CONTENT_PATH_DICT = dict()
    for works_type in WORKS_TYPE_ALIAS_DICT:
        HDFS_CHANNEL_CONTENT_PATH_DICT[works_type] = os.path.join(
                HDFS_FINAL_PATH, '{0}_channel_content'.format(works_type))

    #local path
    LOCAL_OUTPUT_PATH = os.path.join(PROJECT_PATH, 'output/')
    LOCAL_DATA_PATH = os.path.join(PROJECT_PATH, 'data/')
    LOCAL_DATA_PATH_DICT = dict()
    for works_type in WORKS_TYPE_ALIAS_DICT:
        LOCAL_DATA_PATH_DICT[works_type] = os.path.join(LOCAL_DATA_PATH,
                '{0}/'.format(works_type))
    LOCAL_DATA_PATH_DICT['short'] = os.path.join(LOCAL_DATA_PATH, 'short')
    LOCAL_GENOME_VIDEO_PATH = os.path.join(LOCAL_DATA_PATH, 'genome_video')
    LOCAL_GENOME_PATH = os.path.join(LOCAL_DATA_PATH, 'genome')
    LOCAL_PERSON_PATH = os.path.join(LOCAL_DATA_PATH, 'person')
    LOCAL_FINAL_TABLE_PATH_DICT = dict()
    LOCAL_VIDEO_PERSON_RELATION_PATH_DICT = dict()
    LOCAL_VIDEO_INFO_PATH_DICT = dict()
    LOCAL_CHANNEL_PATH_DICT = dict()
    LOCAL_PM_REVIEWED_CHANNEL_DICT = dict()
    LOCAL_CHANNEL_CONTENT_DICT = dict()
    LOCAL_CHANNEL_CONTENT_TITLE_DICT = dict()
    for works_type in WORKS_TYPE_ALIAS_DICT:
        LOCAL_FINAL_TABLE_PATH_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH_DICT[works_type],
                '{0}_final'.format(works_type))
        LOCAL_VIDEO_PERSON_RELATION_PATH_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH_DICT[works_type],
                '{0}_person_relation'.format(works_type))
        LOCAL_VIDEO_INFO_PATH_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH_DICT[works_type],
                '{0}_info'.format(works_type))
        LOCAL_CHANNEL_PATH_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH_DICT[works_type],
                '{0}_channel'.format(works_type))
        LOCAL_PM_REVIEWED_CHANNEL_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH, 'pm_reviewed_channels',
                '{0}_channel'.format(works_type))
        LOCAL_CHANNEL_CONTENT_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH_DICT[works_type],
                '{0}_channel_content'.format(works_type))
        LOCAL_CHANNEL_CONTENT_TITLE_DICT[works_type] = os.path.join(
                LOCAL_DATA_PATH_DICT[works_type],
                '{0}_channel_content_title'.format(works_type))

    LOG_NAME = 'channel-generation'


