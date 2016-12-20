# coding=utf-8
import sys
sys.path.append('../..')
import time
import datetime as dt
from conf.env_config import EnvConfig
from dao.hdfs import HDFS
import os
import logging

class Util:
    def __init__(self):
        pass


    @staticmethod
    def genLogList(day_cnt, days_before,
            works_type='movie', log_type='play', platform='PC'):

        """
        # @Synopsis  get hdfs log file list, to be the input to spark, already
        # filtered blank hdfs path which would cause error
        #
        # @Args day_cnt
        # @Args days_before how many days the time window is before today, e.g. if
        # you want to get the log up to yesterday, days_before is 1
        # @Args works_type
        # @Args platform
        #
        # @Returns  log path joined by ','
        # TODO: should add argument to determine which kind of log to get(play,
        # view, search, etc)
        """
        today = dt.date.today()
        time_window = [(today - dt.timedelta(i)).strftime('%Y%m%d') for
                i in range(days_before, days_before + day_cnt)]

        hdfs_path = ''
        if platform == 'PC':
            hdfs_path = EnvConfig.HDFS_PC_PLAY_LOG_PATH_DICT[works_type]
        elif platform == 'Mobile':
            hdfs_path = EnvConfig.HDFS_MOBILE_PATH + '{0}/'.format(log_type)
        file_list = [(hdfs_path + date) for date in time_window]

        return ','.join(filter(lambda x: HDFS.exists(x), file_list))

    @staticmethod
    def getOneHourSampleLog(works_type='movie', platform='PC'):
        """
        # @Synopsis get a hourly play log file hdfs path, for test only
        #
        # @Args works_type
        # @Args platform
        #
        # @Returns  hdfs path
        """
        sample_day = dt.date.today() - dt.timedelta(2)
        sample_day_str = sample_day.strftime('%Y%m%d')
        hdfs_path = ''
        if platform == 'PC':
            hdfs_path = EnvConfig.HDFS_PC_PLAY_LOG_PATH_DICT[works_type]
        elif platform == 'Mobile':
            hdfs_path = EnvConfig.HDFS_MOBILE_PLAY_LOG_PATH_DICT[works_type]

        log_file = '{0}{1}/{1}20'.format(hdfs_path, sample_day_str)
        return log_file


    @staticmethod
    def save_rdd(rdd, hdfs_path, local_path):
        """
        # @Synopsis  save a rdd both to hdfs and local file system
        #
        # @Args rdd
        # @Args hdfs_path
        # @Args local_path
        #
        # @Returns  succeeded or not
        """

        succeeded = True
        if HDFS.exists(hdfs_path):
            succeeded = HDFS.rmr(hdfs_path)
        if succeeded:
            try:
                rdd.saveAsTextFile(hdfs_path)
            except Exception as e:
                print 'failed to save {0} to {1}'.format(
                    rdd, hdfs_path)
                print e.message
                return False
            if os.path.exists(local_path):
                print 'local path {0} exists, deleting it'\
                        .format(local_path)
                try:
                    os.remove(local_path)
                except Exception as e:
                    print 'failed to save {0} to {1}'.format(
                        hdfs_path, local_path)
                    return False
            succeeded = HDFS.getmerge(hdfs_path, local_path)
            return succeeded
        else:
            return False

    @staticmethod
    def debugRdd(rdd, rdd_name, logger):
        """
        # @Synopsis  a debug function, send the infomation of a rdd to logger
        #
        # @Args rdd
        # @Args logger
        #
        # @Returns  nothing
        """
        logger.debug('{0}: {1}\t{2}'.format(rdd_name, rdd.count(),
            rdd.take(5)))


if __name__ == '__main__':
    print Util.genLogList(2, 3, works_type='tv')

