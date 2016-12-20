"""
# @file gen_tag_content_title.py
# @Synopsis  generate a tag content file, with lines of 'tag_name \t
# title$$title$$..'
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-11-11
"""
import sys
import logging

sys.path.append('../..')
from conf.env_config import EnvConfig
from bll.service.db_tables import DbTables

class GenTagContentTitle(object):
    """
    # @Synopsis  generate tag content title file, for yuqin in tag recommendation
    """

    @staticmethod
    def replaceIdWithTitle(works_type):
        """
        # @Synopsis  generate file
        #
        # @Args works_type
        #
        # @Returns   nothing
        """
        input_obj = open(EnvConfig.LOCAL_CHANNEL_CONTENT_DICT[works_type])
        output_obj = open(EnvConfig.LOCAL_CHANNEL_CONTENT_TITLE_DICT[
            works_type], 'w')

        video_dict = DbTables.getVideoDict(works_type)
        with input_obj, output_obj:
            for line in input_obj:
                fields = line.strip('\n').split('\t')
                if len(fields) == 2:
                    channel = fields[0]
                    content_str = fields[1]
                    id_weights = content_str.split('$$')
                    map_func = lambda x: x.split(':')[1]
                    ids = map(map_func, id_weights)
                    map_func = lambda x: video_dict.get(x, '')
                    titles = map(map_func, ids)
                    titles_str = '$$'.join(titles)
                    output_obj.write('{0}\t{1}\n'.format(channel, titles_str))

if __name__ == '__main__':
    GenTagContentTitle.replaceIdWithTitle('tv')


