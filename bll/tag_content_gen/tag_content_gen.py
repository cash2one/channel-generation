"""
# @file tag_content_gen.py
# @Synopsis  generate long video channel content given channel_id
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-15
"""

import sys
import os

import datetime as dt
import pyspark as ps
import math
import time
import logging

sys.path.append('../..')
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from bll.service.rdd_funcs import RddFuncs
from bll.service.util import Util
from bll.long_video_info.long_video_info import LongVideoInfo
from dao.mola import Mola
from dao.hdfs import HDFS
from gen_tag_content_title import GenTagContentTitle


def main():
    """
    # @Synopsis  spark main program
    #
    # @Returns   succeeded or not(to be added)
    """
    works_type = 'movie'
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    HDFS.overwrite(EnvConfig.LOCAL_PM_REVIEWED_CHANNEL_DICT[works_type],
            EnvConfig.HDFS_CHANNEL_PATH_DICT[works_type])
    LongVideoInfo.genVideoInfo(works_type)
    sc = ps.SparkContext()

    play_log = Util.genLogList(3, 1, works_type=works_type)
    # play_log = Util.getOneHourSampleLog(works_type=works_type, platform='PC')
    logger.debug('play_log: {0}'.format(play_log))

    play_log_rdd = sc.textFile(play_log)\
            .map(RddFuncs.parsePlayLog)\
            .map(lambda x: (x[0], x[1]))
    # play_log_rdd = sc.textFile(play_log)\
    #         .map(RddFuncs.parsePlayLog)\
    #         .filter(lambda x: x[2] == 'search')\
    #         .map(lambda x: (x[0], x[1]))
    Util.debugRdd(play_log_rdd, 'play_log_rdd', logger)

    #(vid, uid)
    item_user_rdd = play_log_rdd \
            .map(lambda x: (x[1], x[0]))

    # (group_id, group_name)
    group_rdd = sc.textFile(EnvConfig.HDFS_CHANNEL_PATH_DICT[works_type])\
            .map(RddFuncs.parseGroupFile)
    Util.debugRdd(group_rdd, 'group_rdd', logger)

    # (group_id, feature), a group_id may contain more than one feature
    group_feature_rdd = group_rdd\
            .flatMap(lambda x: [(x[0], i) for i in x[0].split('$$')])
    Util.debugRdd(group_feature_rdd, 'group_feature_rdd', logger)

    # (feature, set(vid))
    feature_vids_rdd = \
            sc.textFile(EnvConfig.HDFS_LONG_VIDEO_INFO_PATH_DICT[works_type])\
            .flatMap(lambda x: RddFuncs.parseVideoFeature(x, works_type))\
            .distinct()\
            .filter(lambda x: x is not None)\
            .map(lambda x: (x[1], x[0]))\
            .groupByKey()\
            .map(lambda x: (x[0], set(x[1])))
    Util.debugRdd(feature_vids_rdd, 'feature_vids_rdd', logger)

    # (feature, (group_id, vids)) => (group_id, vids)
    group_vids_rdd = group_feature_rdd\
            .map(lambda x: (x[1], x[0]))\
            .join(feature_vids_rdd)\
            .map(lambda x: (x[1][0], x[1][1]))\
            .reduceByKey(lambda a, b: a & b)
    Util.debugRdd(group_vids_rdd, 'group_vids_rdd', logger)

    # (group_id, (vids, group_name)) => (group_name, vids) => (group_name, vid)
    group_vid_rdd = group_vids_rdd\
            .join(group_rdd)\
            .map(lambda x: (x[1][1], x[1][0]))\
            .flatMap(lambda x: [(x[0], i) for i in x[1]])
    Util.debugRdd(group_vid_rdd, 'group_vid_rdd', logger)

    #(uid, (group_name, cnt))
    user_played_movie_cnt_in_group_rdd = group_vid_rdd \
            .map(lambda x: (x[1], x[0])) \
            .join(item_user_rdd) \
            .map(lambda x:((x[1][0], x[1][1]), 1)) \
            .reduceByKey(lambda a, b: a + b)\
            .map(lambda x: (x[0][1], (x[0][0], x[1])))
    Util.debugRdd(user_played_movie_cnt_in_group_rdd,
            'user_played_movie_cnt_in_group_rdd', logger)

    #(uid, vid) => (uid, (vid, (group_name, cnt))) => ((group_name, vid), cnt)
    group_item_sim_rdd = play_log_rdd \
            .join(user_played_movie_cnt_in_group_rdd) \
            .map(lambda x: ((x[1][1][0], x[1][0]), x[1][1][1] - 1)) \
            .reduceByKey(lambda a, b: a + b)
    Util.debugRdd(group_item_sim_rdd, 'group_item_sim_rdd', logger)

    group_vid_key_rdd = group_vid_rdd \
            .map(lambda x: (x, 1))

    # we only need the similarity between a vid and the group it belongs,
    # ignore the similarity between vid and other groups, and vid in the group
    # but with no play history should still be in the group, with 0 similarity
    # ((group, vid), sim)
    group_item_sim_rdd = group_item_sim_rdd \
            .rightOuterJoin(group_vid_key_rdd) \
            .map(RddFuncs.fillZeroSim)
    Util.debugRdd(group_item_sim_rdd, 'group_item_sim_rdd', logger)

    # ((group, vid), sim) => (group, (vid, sim)) => (group, [(vid, sim)]) =>
    #(group_name\t vid:sim$$vid:sim...)
    group_content_rdd = group_item_sim_rdd\
            .map(lambda x: (x[0][0], (x[0][1], x[1])))\
            .groupByKey()\
            .map(lambda x: (x[0], sorted(x[1], key=lambda a: -a[1])))\
            .map(lambda x: RddFuncs.video_info_map_func(x, works_type))\
            .map(lambda x: '%s\t%s' % (x[0], x[1]))\
            .repartition(1)
    Util.debugRdd(group_content_rdd, 'group_content_rdd', logger)

    hdfs_path = EnvConfig.HDFS_CHANNEL_CONTENT_PATH_DICT[works_type]
    local_path = EnvConfig.LOCAL_CHANNEL_CONTENT_DICT[works_type]
    succeeded = Util.save_rdd(group_content_rdd, hdfs_path, local_path)
    if succeeded:
        if not EnvConfig.DEBUG:
            succeeded = Mola.updateDb('movie_channel_channel2id:', local_path)
    if not succeeded:
        logger.fatal('{0} channel_content_gen failed'.format(works_type))

    GenTagContentTitle.replaceIdWithTitle(works_type)

    return succeeded


if __name__ == '__main__':
    main()
