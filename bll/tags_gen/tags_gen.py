# -*- coding:utf-8 -*-
"""
# @file tags_gen.py
# @Synopsis  generate tags from the feature of long video: director, actor,
# year, genome, etc.
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-14
"""

import sys
import os
sys.path.append('../..')
import pyspark as ps
import logging
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from bll.service.rdd_funcs import RddFuncs
from bll.service.tag_name_gen import TagNameGen
from bll.service.util import Util
from bll.service.db_tables import DbTables
from bll.long_video_info.long_video_info import LongVideoInfo
from bll.tags_gen.group_filter import GroupFilter
from dao.hdfs import HDFS


global genome_dict
genome_dict = DbTables.getGenomeDict()

FATHER_BLACK_DICT = {
        '3': '情节',
        '5': '时代', #can be more precicely generated from release time
        '7': '警告',
        '10': '基于',
        '12': '节奏',
        '13': '受众',
        '14': '视觉',
        '16': '配乐',
        '4877': '地区',
        '4921': '受众',
        }

GENOME_BLACK_DICT = {
        '467': '剧情',
        '592': '亚洲',
        '58': '大气',
        '1339': '国产',
        '975': '女性',
        '593': '中国大陆',
        '44': '紧张',
        '50': '精彩',
        '830': '士兵',
        '567': '当代',
        '967': '全明星',
        '973': '出人意料',
        '45': '有启发',
        '912': '帅气',
        '470': '年代戏',
        '57': '风格化',
        '27': '风趣',
        '30': '勇敢',
        '38': '震撼',
        '872':'农民',
        '940':'兄弟姊妹',
        '42': '粗犷',
        '34': '机智',
        '642': '北京',
        '49': '调侃',
        '46': '怀旧',
        '871': '商人',
        '47': '离奇',
        '25': '感人',
        '1136': '厨师',
        '684': '小岛',
        }

WORKS_TYPE_RELATED_GENOME_BLACK_DICT = dict()
WORKS_TYPE_RELATED_GENOME_FATHER_BLACK_DICT = dict()
for works_type in EnvConfig.WORKS_TYPE_ALIAS_DICT:
    WORKS_TYPE_RELATED_GENOME_BLACK_DICT[works_type] = dict()
    WORKS_TYPE_RELATED_GENOME_FATHER_BLACK_DICT[works_type] = dict()
WORKS_TYPE_RELATED_GENOME_BLACK_DICT['comic'] = {
        '506': '动画',
        '511': '动漫',
        # '1338': '日本', #this blocked when we are dividing Japanese Comics
        # '594': '日本', # same reason as above
        '1325': '卡通',
        '944': '儿童',
        '29': '可爱',
        '1490': '特摄',
        '163': '大恶棍',
        '507': '家庭',
        '72': '童心',
        '648': '东京',
        '686': '森林',
        '41': '脱俗',
        '23': '愉快',
        '36': '轻松',
        '22': '动情',
        '1261': '动情',
        '1206': '单纯',
        '26': '振奋',
        '482': '短片',
        '468': '喜剧',
        '1207': '美好',
        '1098': '怪物',
        '8704': '梦想',
        '1259': '纯真',
        '964': '超现实',
        '483': '动作',
        '21': '幽默',
        '498': '冒险', #有'冒险者'基因，比此基因更精确
        '484': '惊悚',
        '508': '犯罪',
        '1202': '刺激',
        '24': '真情',
        '55': '黑色幽默',
        '828': '警察',
        '63': '感伤',
        '1472': '日韩', #the coverage of this gene is too low
        '8749': '少儿', # coverage too low
        '1017': '原创', # no sure meaning, original? which comic is not?
        '1014': '大片', # not suitable for comic
        '68': '凄凉',
        '37': '犀利',
        '207': '少年英雄',
        '952': '公主', # low coverage
        # '939': '武士', #与'日本武士'重复
        }
WORKS_TYPE_RELATED_GENOME_FATHER_BLACK_DICT['comic'] = {
        '6': '地点', # comic content is weakly related to area
        }
WORKS_TYPE_RELATED_GENOME_BLACK_DICT['show'] = {
        '4949': '生活', # low precision
        '4917': '企业家', # low quality
        '4948': '竞赛', # low quality
        '4887': '调侃',
        '4919': '名人',
        '8725': '解说',
        '5349': 'CCTV证券资讯频道秀',
        '4900': '互动',
        '4913': '影视明星',
        '5110': '李晨',
        '4891': '刺激',
        '4932': '歌舞',
        '4920': '歌手',
        '4890': '愉快',
        '4910': '娱乐',
        '5255': '晨阳', # low precision
        '6037': '浙江卫视经济生活频道',
        '5156': 'tvb', # low precision
        '5298': 'CCTV',
        '4892': '轻松',
        '4884': '有启发',
        '4889': '深刻',
        '8735': '短剧',
        '4885': '讽刺',
        '8730': '厨艺',
        '4888': '机智',
        '4908': '益智',
        '5712': '八大电视台',
        '6700': 'CCTV-少儿', # low precision
        '4881': '励志', # low precision
        '6504': '56网', # low quality
        '5878': '优酷网', # low quality
        '4965': '爱奇艺', # low quality
        '8721': '网络红人', # low quality
        '4901': '颁奖典礼', # low quality
        '5048': '广东卫视', # low quality
        '5377': '青海卫视', # low quality
        '4882': '感人', # low quality
        '5460': '河南卫视', # low quality
        '4992': '黑龙江卫视', # low quality
        '4904': '公开课', # low precision
        '5036': '宁夏卫视', # low quality
        '5004': '贵州卫视', # low quality
        '5076': '湖北卫视', # low quality
        '5060': '厦门卫视', # low quality
        '7187': '风行网', # low quality
        '4955': '河北卫视', # low quality
        '5248': '广西卫视', # low quality
        '5062': '三立都会台', # low quality
        '5397': '陕西卫视', # low quality
        '6678': '腾讯视频', # low quality
        '5860': '新疆卫视', # low quality
        '5055': '深圳卫视', # low quality
        '6438': '美国CBS电视台', # low quality
        '5045': '东南卫视', # low quality
        '5926': '江苏电视台综艺频道', # low quality
        '5805': '搜狐视频', # low quality
        '5954': 'BTV体育频道', # low quality
        '4957': '山东卫视', # low quality
        '5395': '甘肃卫视', # low quality
        '5469': '重庆卫视', # low quality
        '5894': '韩国SBS电视台', # low quality
        '5536': '凤凰卫视中文台', # low quality
        '5673': 'CCTV-3综艺', # duplicate with G6716
        }
WORKS_TYPE_RELATED_GENOME_FATHER_BLACK_DICT['show'] = {
        '4875': '主持人', # precision too low
        }

def featureFilter(line, works_type):
    """
    # @Synopsis  filter genome in blacklist
    #
    # @Args line a line of rdd in in the form of (vid, feature)
    #
    # @Returns genome is in black list or not
    """
    feature = line[1]
    feature_type = feature[0]
    feature_id = feature[1:]
    if feature_type is not 'G':
        return True
    elif feature_id in GENOME_BLACK_DICT.keys():
        return False
    elif feature_id in WORKS_TYPE_RELATED_GENOME_BLACK_DICT[works_type].keys():
        return False
    elif feature_id not in genome_dict.keys():
        return False
    else:
        father_id = genome_dict[feature_id]['father_id']
        if father_id in FATHER_BLACK_DICT.keys():
            return False
        elif father_id in \
                WORKS_TYPE_RELATED_GENOME_FATHER_BLACK_DICT[works_type].keys():
            return False
        else:
            return True


def main():
    """
    # @Synopsis  main program
    #
    # @Returns succeeded or not
    """
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    sc = ps.SparkContext()
    works_type = 'tv'
    logger.debug('works_type is {0}'.format(works_type))
    LongVideoInfo.genVideoInfo(works_type)
    VIDEO_CNT_LOWER_BOUND = 10

    name_gen = TagNameGen()

    # (group_id, [vids])
    one_feature_group_rdd = \
            sc.textFile(EnvConfig.HDFS_LONG_VIDEO_INFO_PATH_DICT[works_type])\
            .flatMap(lambda x: RddFuncs.parseVideoFeature(x, works_type))\
            .distinct()\
            .filter(lambda x: x is not None)\
            .filter(lambda x: featureFilter(x, works_type))\
            .map(lambda x: (x[1], x[0]))\
            .groupByKey()\
            .filter(lambda x: len(x[1]) >= VIDEO_CNT_LOWER_BOUND)
    Util.debugRdd(one_feature_group_rdd, 'one_feature_group_rdd', logger)

    group_item_rdd = one_feature_group_rdd\
            .flatMap(lambda x: [(x[0], i) for i in x[1]])
    Util.debugRdd(group_item_rdd, 'group_item_rdd', logger)

    play_log = Util.genLogList(7, 1, works_type='tv', platform='PC')
    # play_log = Util.getOneHourSampleLog('comic', 'PC')
    user_item_rdd = sc.textFile(play_log)\
            .map(RddFuncs.parsePlayLog)\
            .map(lambda x: (x[0], x[1]))\
            .distinct()
    # user_item_rdd = sc.textFile(play_log)\
    #         .map(RddFuncs.parsePlayLog)\
    #         .filter(lambda x: x[2] == 'search')\
    #         .map(lambda x: (x[0], x[1]))\
    #         .distinct()
    Util.debugRdd(user_item_rdd, 'user_item_rdd', logger)

    item_uv_rdd = user_item_rdd\
            .map(lambda x: (x[1], 1))\
            .reduceByKey(lambda a, b: a + b)\
            .filter(lambda x: x[1] > 1)
    Util.debugRdd(item_uv_rdd, 'item_uv_rdd', logger)
    #(item, (group, uv))
    group_uv_rdd = group_item_rdd\
            .map(lambda x: (x[1], x[0]))\
            .join(item_uv_rdd)\
            .map(lambda x: (x[1][0], x[1][1]))\
            .reduceByKey(lambda a, b: a + b)
    Util.debugRdd(group_uv_rdd, 'group_uv_rdd', logger)


    output_rdd = one_feature_group_rdd\
            .leftOuterJoin(group_uv_rdd)\
            .map(RddFuncs.fillZeroUV)\
            .repartition(1)\
            .sortBy(lambda x: x[1][1], ascending=False)\
            .map(lambda x: (x[0], '$$'.join(x[1][0])))\
            .map(lambda x: (x[0], name_gen.genTagName(x[0], works_type), x[1]))\
            .filter(lambda x: x[1] is not None)\
            .map(lambda x: '{0}\t{1}\t{2}'.format(x[0], '\t'.join(x[1]), x[2]))
    hdfs_path = EnvConfig.HDFS_CHANNEL_PATH_DICT[works_type]
    local_path = os.path.join(EnvConfig.LOCAL_DATA_PATH_DICT[works_type],
            'channel_tmp')
    Util.save_rdd(output_rdd, hdfs_path, local_path)

    GroupFilter.similar_filter(works_type)
    succeeded = HDFS.overwrite(EnvConfig.LOCAL_CHANNEL_PATH_DICT[works_type],
            EnvConfig.HDFS_CHANNEL_PATH_DICT[works_type])

    return succeeded

if __name__ == '__main__':
    main()
