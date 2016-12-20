"""
# @file long_video_info.py
# @Synopsis  generate long video info file
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-09
"""

import sys
sys.path.append('../..')
import os
from conf.env_config import EnvConfig
from bll.service.db_tables import DbTables
from dao.hdfs import HDFS

class LongVideoInfo(object):

    """
    # @Synopsis  generate long video info file
    """
    @staticmethod
    def genVideoInfo(works_type):
        """
        # @Synopsis  generate long video info file, lines of
        # 'works_id\ttitle\trelease_year\tpeople\tgenomes'
        # @Args works_type
        #
        # @Returns  succeeded or not(to be added)
        """
        #should be lines of 'works_id, title, year'
        file_path = EnvConfig.LOCAL_FINAL_TABLE_PATH_DICT[works_type]
        DbTables.updateDbFiles()
        input_obj = open(file_path, 'r')
        input_obj.readline()
        output_path = EnvConfig.LOCAL_VIDEO_INFO_PATH_DICT[works_type]
        output_obj = open(output_path, 'w')

        genome_dict = DbTables.getVideoGeneDict(works_type)
        people_dict = DbTables.getVideoPeopleDict(works_type)

        with input_obj, output_obj:
            for line in input_obj:
                fields = line.strip('\n').split('\t')
                try:
                    if len(fields) >= 3:
                        works_id = fields[0]
                        title = fields[1]
                        release_year = fields[2]
                        genomes = genome_dict.get(works_id, '')
                        people = people_dict.get(works_id, '')
                        if works_type in ['movie', 'show']:
                            output_obj.write('\t'.join([works_id, title,
                                release_year, people, genomes]) + '\n')
                        elif works_type in ['comic', 'tv'] and len(fields) == 4:
                            finished = fields[3]
                            output_obj.write('\t'.join([works_id, title,
                                release_year, people, genomes, finished]) + '\n')
                except Exception as e:
                    print e.message
        HDFS.overwrite(output_path,
                EnvConfig.HDFS_LONG_VIDEO_INFO_PATH_DICT[works_type])

if __name__ == '__main__':
    LongVideoInfo.genVideoInfo('comic')
    # LongVideoInfo.genVideoInfo('movie')
