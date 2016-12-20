# -*- coding:utf-8 -*-
"""
# @file tag_name_gen.py
# @Synopsis  get the name of a tag(in natrual language)
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-13
"""

import sys
sys.path.append('../..')
from conf.env_config import EnvConfig
from bll.service.db_tables import DbTables

class TagNameGen(object):
    """
    # @Synopsis  translate tag id into natrual language, including name,
    # display_name, image_name
    """

    __genome_dict = dict()
    __person_dict = dict()

    def __init__(self):
        self.__genome_dict = DbTables.getGenomeDict()
        self.__person_dict = DbTables.getPersonDict()

    def genTagName(self, tag_id, works_type):
        """
        # @Synopsis  translate tag id into tag name
        #
        # @Args tag_id
        # @Args works_type
        #
        # @Returns  [tag_name, display_name, image_name]
        """
        genome_dict = self.__genome_dict
        person_dict = self.__person_dict
        FEATURE_TEMPLATE_DICT = {
                'G1': '%s',#father_id not in genome_dict
                'G2': '%s',#情感
                'G3': '关于%s',#情节
                'G4': '%s',#类型
                'G5': '%s',#时代
                'G6': '%s',#地点
                'G7': '含有%s',#警告
                'G8': '%s',#角色
                'G9': '%s',#风格
                'G10': '%s改编',#基于
                'G11': '%s',#赞誉
                'G12': '节奏%s',#节奏
                'G13': '%s观看',#受众
                'G14': '%s',#视觉
                'G15': '%s',#其他关键字
                'G16': '%s配乐',#配乐
                'G1328': '%s',#制片国家
                'G4894': '%s',#类型
                'G4876': '%s', #电视台
                'G4875': '%s主持', #主持人
                'G4878': '%s', #情感
                'G4929': '%s', #内容
                'G4912': '%s', #嘉宾
                'A': '%s主演',
                'D': '%s导演',
                'H': '%s主持',
                'C': '%s世纪',
                'd': '%s年代',
                'N': '%s',
                }
        FEATURE_ORDER_LIST = ['N', 'C', 'd', 'D', 'H', 'A', 'G2', 'G3', 'G4',
                'G5', 'G6', 'G7', 'G8', 'G9', 'G10', 'G11', 'G12', 'G13',
                'G14', 'G15', 'G16', 'G1328']

        WORKS_TYPE_NAME_DICT = {
                'movie': '影片',
                'tv': '剧',
                'comic': '动漫',
                'show': '秀'
                }
        features = tag_id.split('$$')
        format_features = []
        for feature in features:
            feature_type = feature[0]
            feature_id = feature[1:]
            if feature_type is 'G':
                if feature_id in genome_dict:
                    genome_name = genome_dict[feature_id]['name']
                    father_id = genome_dict[feature_id]['father_id']
                    format_feature = {'type': 'G{0}'.format(father_id),
                            'name': genome_name}
                    format_features.append(format_feature)
            elif feature_type in ['A', 'D', 'H']:
                if feature_id in person_dict:
                    person_name = person_dict[feature_id]
                    format_feature = {'type': feature_type, 'name': person_name}
                    format_features.append(format_feature)
            elif feature_type in ['C', 'd']:
                format_features.append({'type': feature_type, 'name': feature_id})
            elif feature_type is 'N':
                format_features.append({'type': feature_type, 'name': '新'})


        if (len(format_features) == 2 and
                format_features[0]['type'] == format_features[1]['type']):
            feature_type = format_features[0]['type']
            double_name = '{0}{1}'.format(format_features[0]['name'],
                    format_features[1]['name'])
            format_features = [{'type': feature_type, 'name': double_name}]

        if len(format_features) > 0:
            feature_strs = []
            for format_feature in format_features:
                feature_type = format_feature['type']
                name = format_feature['name']
                feature_str = ''
                # feature_str = FEATURE_TEMPLATE_DICT[feature_type] % name
                feature_str = FEATURE_TEMPLATE_DICT.get(feature_type, '%s_missing') % name
                feature_strs.append(feature_str)

            tag_name = '{0}{1}'.format(''.join(feature_strs),
                    WORKS_TYPE_NAME_DICT[works_type])
            # some genes contains type of video in the end, e.g. '大片', '喜剧'
            if tag_name[-3:] == tag_name[-6: -3]:
                tag_name = tag_name[: -3]

            image_strs = []
            for format_feature in format_features:
                feature_type = format_feature['type']
                feature_name = format_feature['name']
                if len(feature_name) == 3:
                    pass
                    # feature_name += WORKS_TYPE_NAME_DICT[works_type]
                elif feature_type is 'd':
                    # feature_name = FEATURE_TEMPLATE_DICT[feature_type] % \
                    #         feature_name
                    feature_name = FEATURE_TEMPLATE_DICT.get(feature_type, '%s_missing') % \
                            feature_name
                image_strs.append(feature_name)
            image_name = ''.join(image_strs)
            return [tag_name, tag_name, image_name]


if __name__ == '__main__':
    tag_name_obj = TagNameGen()
    print tag_name_obj.genTagName('G468$$G489', 'tv')


