"""
# @file group_filter.py
# @Synopsis  filter similar groups
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-15
"""
import sys
import os
sys.path.append('../..')
from conf.env_config import EnvConfig
from dao.hdfs import HDFS

class GroupFilter(object):
    """
    # @Synopsis  filter similar groups
    """

    @staticmethod
    def jacaard(movie_set_str1, movie_set_str2):
        """
        # @Synopsis  calculate jaccard similarity of two vid sets
        #
        # @Args movie_set_str1
        # @Args movie_set_str2
        #
        # @Returns  similarity
        """
        movie_set1 = set(movie_set_str1.split('$$'))
        movie_set2 = set(movie_set_str2.split('$$'))
        cup = movie_set1 | movie_set2
        cap = movie_set1 & movie_set2
        jacaard = float(len(cap))/len(cup)
        return jacaard

    @staticmethod
    def similar_filter(works_type):
        """
        # @Synopsis  main filter function, scan the input file from top, and
        # ignore the next line if its jacaard similarity with any existing line
        # is more than a specific threshold(0.33)
        #
        # @Args works_type
        #
        # @Returns  nothing
        """
        input_path = os.path.join(
                EnvConfig.LOCAL_DATA_PATH_DICT[works_type], 'channel_tmp')
        output_path = EnvConfig.LOCAL_CHANNEL_PATH_DICT[works_type]
        input_obj = open(input_path)
        output_obj = open(output_path, 'w')
        group_list = []
        for line in input_obj:
            fields = line.strip().split('\t')
            tag_id = fields[0]
            tag_name = fields[1]
            display_name = fields[2]
            image_name = fields[3]
            movie_set_str = fields[4]
            group_list.append((tag_id, tag_name, display_name,
                image_name, movie_set_str))
        start_index = 0
        while start_index < len(group_list) - 1:
            group_list = GroupFilter.filter(start_index, group_list)
            start_index += 1
            # print 'round %s, list_len %s' % (start_index, len(group_list))
        for line in group_list:
            output_obj.write('\t'.join(line[: -1]) + '\n')

    @staticmethod
    def filter(start_index, group_list):
        """
        # @Synopsis  filter lines from the start_index
        #
        # @Args start_index
        # @Args group_list
        #
        # @Returns  filtered group list 
        """
        start_group = group_list[start_index][0]
        start_movie_set_str = group_list[start_index][4]
        new_group_list = []
        new_group_list[: start_index + 1] = group_list[: start_index + 1]
        for line in group_list[start_index + 1:]:
            if GroupFilter.jacaard(start_movie_set_str, line[4]) < 0.2:
                new_group_list.append(line)
        return new_group_list

if __name__ == '__main__':
    GroupFilter.similar_filter('tv')

