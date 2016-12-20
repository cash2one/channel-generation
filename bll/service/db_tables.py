# -*- coding:utf-8 -*-
"""
# @file db_tables.py
# @Synopsis  dump table from db, and overwrite old files
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-08
"""
import sys
sys.path.append('../..')
import os
from conf.env_config import EnvConfig
from dao.mysql import Mysql
import logging
from itertools import groupby

from conf.init_logger import InitLogger

class DbTables(object):
    """
    # @Synopsis  used to dump tables from db and provide some comment dict(genome,
    # person, ...)
    """
    SQL_CMD_DICT = dict({
        'movie': ("select works_id, trunk, net_show_time"
            " from movie_final where source!=16 and big_poster!=\"\""
            " and sites!=\"\""),
        'tv': ("select works_id, trunk, al_date, finish"
            " from tv_final where title!=\"\""),
        'comic': ("select works_id, trunk, al_date, finish"
            " from comic_final where title!=\"\" and big_poster!=\"\""
            " and all_sites!= \"\""),
        'show': ("select works_id, trunk, max_episode"
            " from show_final where title!=\"\" and big_poster!=\"\""),
        'genome_video': ("select works_id, genome_id, genome_score, "
            " works_type from genome_video"),
        'movie_person_relation': ("select movie_id, person_id, role_type"
            " from movie_person_relation where role_type=1 or role_type=2"),
        'tv_person_relation': ("select tv_id, person_id, role_type"
            " from tvplay_person_relation where role_type=1 or role_type=2"),
        'comic_person_relation': ("select comic_id, person_id, role_type "
            " from comic_person_relation where role_type=1 or role_type=2"),
        'show_person_relation': ("select works_id, host"
            " from show_final where title!=\"\" and big_poster!=\"\""
            " and host!=\"\""),
        'genome': ("select genome_id, name_chs, level, father_id"
            " from genome"),
        'person': "select person_id, name_chs, name_eng from person_final"
        })
    SQL_DB_DICT = dict({
        'movie': 'Final',
        'tv': 'Final',
        'comic': 'Final',
        'show': 'Final',
        'genome_video': 'Attr',
        'movie_person_relation': 'Attr',
        'tv_person_relation': 'Attr',
        'comic_person_relation': 'Attr',
        'show_person_relation': 'Final',
        'genome': 'Attr',
        'person': 'Final',
        })

    WORKS_TYPE_DICT = dict({
        'movie': 0,
        'tv': 1,
        'show': 2,
        'comic': 3,
        })

    @staticmethod
    def dumpTable(table_name, output_path):
        """
        # @Synopsis  dump data from mysql database and write to specified file
        #
        # @Args table_name
        #
        # @Returns  scceeded or not
        """
        sql_cmd = DbTables.SQL_CMD_DICT.get(table_name, None)
        sql_db = DbTables.SQL_DB_DICT.get(table_name, None)
        if sql_cmd is None or sql_db is None:
            return False
        else:
            mysql = Mysql()
            mysql.getDbFile(sql_cmd, output_path, sql_db)

    @staticmethod
    def updateDbFiles():
        """
        # @Synopsis  dump db files and overwrite local version
        #
        # @Returns  succeeded or not
        """
        DbTables.dumpTable('genome_video', EnvConfig.LOCAL_GENOME_VIDEO_PATH)
        DbTables.dumpTable('genome', EnvConfig.LOCAL_GENOME_PATH)
        DbTables.dumpTable('person', EnvConfig.LOCAL_PERSON_PATH)
        for works_type in EnvConfig.WORKS_TYPE_ALIAS_DICT:
            DbTables.dumpTable(works_type,
                    EnvConfig.LOCAL_FINAL_TABLE_PATH_DICT[works_type])
            table_name = '{0}_person_relation'.format(works_type)
            file_path = EnvConfig.LOCAL_VIDEO_PERSON_RELATION_PATH_DICT[works_type]
            DbTables.dumpTable(table_name, file_path)

    @staticmethod
    def getVideoDict(works_type):
        """
        # @Synopsis  read the <works_type>_final file and load video dict
        #
        # @Args works_type
        #
        # @Returns   dict of {id: title}, where id is a string
        """
        input_file = EnvConfig.LOCAL_FINAL_TABLE_PATH_DICT[works_type]
        input_obj = open(input_file, 'r')
        input_obj.readline()
        video_dict = dict()
        with input_obj:
            for line in input_obj:
                fields = line.strip('\n').split('\t')
                if len(fields) >= 2:
                    works_id = fields[0]
                    title = fields[1]
                    video_dict[works_id] = title
        return video_dict

    @staticmethod
    def getVideoGeneDict(works_type):

        """
        # @Synopsis  read the genome_video file and generate a dict, in which the
        # key is works_id and value is genome_id joined by '+'
        #
        # @Args work_type
        #
        # @Returns  dict
        """
        input_file = EnvConfig.LOCAL_GENOME_VIDEO_PATH
        input_obj = open(input_file, 'r')
        input_obj.readline()
        video_genome_list = []
        for line in input_obj:
            try:
                fields = line.strip().split('\t')
                works_id = fields[0]
                genome_id = fields[1]
                genome_score = int(fields[2])
                works_type_enum = int(fields[3])
                if genome_score >= 2 and works_type_enum == \
                        DbTables.WORKS_TYPE_DICT[works_type]:
                    video_genome_list.append([works_id, genome_id])
            except Exception as e:
                pass

        video_genome_list.sort(key=lambda x: x[0])
        tmp = groupby(video_genome_list, key=lambda x: x[0])
        groups = [list(g) for k, g in tmp]
        reduce_func = lambda x, y: [x[0], '+'.join([x[1], y[1]])]
        map_func = lambda x: reduce(reduce_func, x)
        video_genomes_list = map(map_func, groups)

        return dict(video_genomes_list)

    @staticmethod
    def getVideoPeopleDict(works_type):

        """
        # @Synopsis  get the infomation of relatvie people of a video
        #
        # @Args works_type
        #
        # @Returns  a dict, key is works_id, value is person_id joined by '+'
        # preceeded by role type. e.g. 'D23+A34'
        """

        file_path = EnvConfig.LOCAL_VIDEO_PERSON_RELATION_PATH_DICT[works_type]
        input_obj = open(file_path, 'r')
        input_obj.readline()
        video_person_list = []
        PERSON_BLACK_DICT = {
                #这个人没中文名，看其视频又看不出是同一个人，我觉得是编辑瞎搞的
                '13435': 'Michi Sait.',
                }
        if works_type == 'show':
            person_dict = DbTables.getPersonDict()
            reverse_person_dict = dict({name: person_id for person_id, name
                in person_dict.items()})
            host_list = []
            # works_id \t hostname1$$hostname2..
            for line in input_obj:
                fields = line.strip('\n').split('\t')
                if len(fields) == 2:
                    works_id = fields[0]
                    host_names = fields[1].split('$$')
                    for host_name in host_names:
                        if host_name in reverse_person_dict:
                            person_id = reverse_person_dict[host_name]
                            host_list.append([works_id, person_id])
            video_person_list = map(lambda x: [x[0], 'H{0}'.format(x[1])],
                    host_list)
        else:
            video_director_list = []
            video_actor_list = []
            # works_id \t person_id \t role_type
            for line in input_obj:
                fields = line.strip().split('\t')
                tv_id = fields[0]
                person_id = fields[1]
                if person_id not in PERSON_BLACK_DICT.keys():
                    role_type = fields[2]
                    if role_type == '1':
                        video_director_list.append([tv_id, person_id])
                    elif role_type == '2':
                        video_actor_list.append([tv_id, person_id])
            video_director_list = map(lambda x: [x[0], 'D{0}'.format(x[1])],
                    video_director_list)
            video_actor_list = map(lambda x: [x[0], 'A{0}'.format(x[1])],
                    video_actor_list)
            video_person_list = video_director_list + video_actor_list

        video_person_list.sort(key=lambda x: x[0])
        tmp = groupby(video_person_list, key=lambda x: x[0])
        groups = [list(g) for k, g in tmp]
        reduce_func = lambda x, y: [x[0], '+'.join([x[1], y[1]])]
        map_func = lambda x: reduce(reduce_func, x)
        video_people_list = map(map_func, groups)

        return dict(video_people_list)

    @staticmethod
    def getGenomeDict():
        """
        # @Synopsis  load genome dict from local file
        #
        # @Returns  genome dict, in the form of {genome_id: {'name': name,
        # 'level': level, 'father_id': father_id}}
        """
        input_obj = open(EnvConfig.LOCAL_GENOME_PATH)
        input_obj.readline()
        genome_dict = dict()
        for line in input_obj:
            fields = line.strip().split('\t')
            try:
                genome_id = fields[0]
                name = fields[1]
                level = int(fields[2])
                father_id = fields[3]
                genome_dict[genome_id] = dict({
                    'name': name,
                    'level': level,
                    'father_id': father_id
                    })
            except Exception as e:
                pass
        # we don't need the direct parent infomation, we only care about which
        # level 1 genome is the ancestor of the current genome
        for genome_id, genome_info in genome_dict.items():
            father_id = genome_info['father_id']
            if father_id in genome_dict.keys():
                while genome_dict[father_id]['level'] > 1:
                    if father_id in genome_dict.keys():
                        father_id = genome_dict[father_id]['father_id']
                    else:
                        father_id = 1
                        break
                genome_dict[genome_id]['father_id'] = father_id
        return genome_dict


    @staticmethod
    def getPersonDict():
        """
        # @Synopsis  load person dict from local file
        #
        # @Returns  person dict, in the form of {person_id: name}, where name is
        # in Chinese if name_chs exists in database, in English otherwise
        """
        person_dict = dict()
        input_obj = open(EnvConfig.LOCAL_PERSON_PATH)
        input_obj.readline()
        for line in input_obj:
            fields = line.strip().split('\t')
            try:
                person_id = fields[0]
                name_chs = fields[1]
                name_eng = fields[2]
                name = name_chs
                if name == '' or name == '未知':
                    name = name_eng
                if not name == '':
                    person_dict[person_id] = name
            except Exception as e:
                pass
        return person_dict

if __name__ == '__main__':
    DbTables.dumpTable('movie', EnvConfig.LOCAL_FINAL_TABLE_PATH_DICT['movie'])
    # DbTables.dumpTable('person', EnvConfig.LOCAL_PERSON_PATH)
    # person_dict = DbTables.getPersonDict()
    # print person_dict.items()[:5]
    InitLogger()
    # DbTables.updateDbFiles()


