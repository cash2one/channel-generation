"""
# @file rdd_action_funcs.py
# @Synopsis  rdd functions
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-14
"""

import sys
sys.path.append('../..')
import os
from conf.env_config import EnvConfig
import math
import time
from datetime import datetime
import re
from urllib import unquote

class RddFuncs(object):
    """
    # @Synopsis  rdd operation functions
    """

    @staticmethod
    def parsePlayLog(line):
        """
        # @Synopsis  parse long video play log line
        #
        # @Args line
        #
        # @Returns   (uid, vid, playType), where uid and vid are string, playType
        # is one of ['search', 'browse', ...]
        """
        fields = line.strip().split('\t')
        if len(fields) is 5:
            try:
                uid = fields[0]
                sub_fields = fields[4].split(';')
                vid = sub_fields[0].split(':')[1]
                playType = sub_fields[2].split(':')[1]
                if len(vid) > 0 and len(vid) <= 10:
                    return (uid, vid, playType)
            except Exception as e:
                pass

    @staticmethod
    def parseShortPlayLog(line):
        """
        # @Synopsis  pass short video play log line
        #
        # @Args line
        #
        # @Returns   (uid, vid, playType)
        """
        fields = line.strip().split('\t')
        if len(fields) == 5:
            uid = fields[0]
            sub_fields = fields[4].split(';')
            if len(sub_fields) == 2:
                try:
                    playType = sub_fields[0].split(':')[1]
                    url = unquote(sub_fields[1].split(':')[1])
                    return (uid, url, playType)
                except Exception as e:
                    pass

    @staticmethod
    def parseSearchLog(line, platform='Mobile'):
        """
        # @Synopsis  parse search log
        #
        # @Args line   A line in log file
        # @Args platform   Mobile or PC
        #
        # @Returns   [uid, query]
        """
        # 0C651E4AD40172B36CB3BE2D9DFA0583|952935720980668 \t
        # %E5%A4%9A%E6%83%85%E6%B1%9F%E5%B1%B1
        fields = line.strip().split('\t')
        if len(fields) == 2:
            uid = fields[0]
            query = fields[1]
            return [uid, query]



    @staticmethod
    def parseVideoFeature(line, works_type='tv'):
        """
        # @Synopsis  parse line of long video info file, and return a list of
        # tuple((vid, feature))
        #
        # @Args line
        # @Args works_type
        #
        # @Returns  list of tuple((vid, feature))
        """
        fields = line.strip().split('\t')
        video_gene_list = []
        if len(fields) >= 5:
            vid = fields[0]
            year_str = fields[2]
            person_str = fields[3]
            genes_str = fields[4]
            try:
                if works_type == 'show':
                    max_episode_str = fields[2]
                    update_date = datetime.strptime(max_episode_str, '%Y%m%d')
                    year = update_date.year
                    today = datetime.today()
                    days_ago = (today - update_date).days
                    if days_ago >= 0:
                        if days_ago <= 7:
                            video_gene_list.append((vid, 'N')) # refers to newest

                elif works_type == 'movie':
                    year = int(year_str)
                    current_time = time.localtime()
                    current_year = current_time.tm_year
                    current_mon = current_time.tm_mon
                    years_before = current_year - year
                    if years_before >= 0:
                        if years_before == 0 or (years_before == 1 and
                                current_mon <= 2):
                            video_gene_list.append((vid, 'N')) #refers to new

                elif works_type in ['comic', 'tv']:
                    finished = fields[5]
                    if finished == '0':
                        video_gene_list.append((vid, 'N')) #refers to newest

                if year < 2000 and year != 1970:
                    decade = ((year % 100) / 10) * 10
                    # 'd' refers to 'decade'
                    video_gene_list.append((vid, 'd{0}'.format(decade)))

            except Exception as e:
                print e.message

            if person_str != '':
                people = person_str.split('+')
                for person in people:
                    video_gene_list.append((vid, person))
            if genes_str != '':
                genes = genes_str.split('+')
                for gene in genes:
                    video_gene_list.append((vid, 'G{0}'.format(gene)))
        return video_gene_list

    @staticmethod
    def parseGroupFile(line):
        """
        # @Synopsis  parse group_info file
        #
        # @Args line
        #
        # @Returns (group_id, group_name) group_id is the ascii code indicate
        # the features that define this group
        """
        fields = line.strip().split('\t')
        if len(fields) >= 2:
            group_id = fields[0]
            group_name = fields[1]
            return (group_id, group_name)

    @staticmethod
    def fillZeroSim(x):
        """
        # @Synopsis  replace None with 0
        #
        # @Args x
        #
        # @Returns   (x[0], sim), sim should be a non-negtive integer
        """
        if x[1][0] is None:
            return (x[0], 0)
        else:
            return (x[0], x[1][0])

    @staticmethod
    def fillZeroUV(x):
        """
        # @Synopsis  replace None with 0, so that channels with zero UV still
        # remains
        #
        # @Args x
        #
        # @Returns  (group, ([vids], uv))
        """
        if x[1][1] is None:
            return (x[0], (x[1][0], 0))
        else:
            return x

    @staticmethod
    def video_info_map_func(line, works_type='movie'):
        """
        # @Synopsis  map the rdd to the format needed
        #
        # @Args x
        # @Args works_type
        #
        # @Returns   (group_name, 'works_type_alias:vid:weight')
        """
        works_type_alias = EnvConfig.WORKS_TYPE_ALIAS_DICT[works_type]
        group = line[0]
        vid_weights = line[1]
        def map_func(x):
            """
            # @Synopsis  simple map file, needed docstring
            # @Args x
            # @Returns  string
            """
            return '{0}:{1}:{2}'.format(works_type_alias, x[0], x[1])
        vid_weight_strs = map(map_func, vid_weights)
        aggregeted_str = reduce(lambda a, b: '{0}$${1}'.format(a, b),
                vid_weight_strs)

        return (group, aggregeted_str)

if __name__ == '__main__':
    group_size = 2
    user_dict1 = dict({'a': 1, 'b': 1})
    user_dict2 = dict({'a': 1, 'c': 1})
    result_dict = rdd_action_funcs.user_dict_nomalized_sum(user_dict1, user_dict2)
    print result_dict
    print rdd_action_funcs.norm_cal(result_dict)/group_size
