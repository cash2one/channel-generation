"""
# @file mysql_dal.py
# @Synopsis  mysql dal, connet to ns_video db and final db
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-06
"""

import os
import sys
sys.path.append('..')
from conf.env_config import EnvConfig
import shutil
import commands
import logging

logger = logging.getLogger(EnvConfig.LOG_NAME)

class Mysql(object):
    """
    # @Synopsis  mysql dal
    """
    DB_BNS_DICT = {
            'Final': 'dbbk-videodetail.xdb.all',
            'Attr': 'dbbk-videopc.xdb.all'}

    def __init__(self):
        if EnvConfig.DEBUG:
            self._DB_CONFIG_DICT ={
                    'Attr': {
                        'db_name': 'ns_video'
                        },
                    'Final':{
                        'db_name': 'video_final_product'
                        }
                    }
            for db_alias, bns in Mysql.DB_BNS_DICT.iteritems():
                self._DB_CONFIG_DICT[db_alias]['user'] = 'guming02'
                self._DB_CONFIG_DICT[db_alias]['password'] = 'video@123'
                host, port = Mysql.getInstanceByService(bns)
                self._DB_CONFIG_DICT[db_alias]['host'] = host
                self._DB_CONFIG_DICT[db_alias]['port'] = port

        else:
            self._DB_CONFIG_DICT ={
                    'Attr': {
                        'host': '10.36.3.207',
                        'port': '6043',
                        'user': 'ns_video_w',
                        'password': 'aLPpt59INnr37aKP',
                        'db_name': 'ns_video'
                        },
                    'Final':{
                        'host': '10.26.5.87',
                        'port': '6162',
                        'user': 'final_prod_r',
                        'password': 'QU811NUULzvwbqgHw2Ua0baE7Km26ALM',
                        'db_name': 'video_final_product'
                        }
                    }

    @staticmethod
    def getInstanceByService(db_bns):
        """
        # @Synopsis  get mysql host and port by BNS
        #
        # @Args db_bns
        #
        # @Returns   host, port
        """
        bash_cmd = 'get_instance_by_service -p {0}'.format(db_bns)
        status, output = commands.getstatusoutput(bash_cmd)
        logger.debug('Returned: {0}: {1}\t{2}'.format(status, bash_cmd, output))
        host, port = output.split(' ')
        return host, port

    def sqlExec(self, sql_cmd, output_file, db='Attr'):
        """
        # @Synopsis execute a sql command and dump the result to an output
        # file if output_file is not None
        #
        # @Args sql_cmd
        # @Args output_file, None for not write to file(for update and insert)
        # @Args db 'Attr' for ns_video database, 'Final' for final database
        #
        # @Returns succeeded or not
        """
        # Note: the following commented method is wrong. That would cause the
        # target file overwritten by an empty file if anything goes wrong
        # during the mysql command excuting, which would sure cause disaster!
        # bash_cmd = Mysql.sql_prefix_dict[db] + sql_cmd + '>' + output_file
        if not db in Mysql.DB_BNS_DICT:
            return False
        set_names_cmd = 'set names gbk;'
        connect_cmd = '{0} -h{1} -P{2} -u{3} -p{4} {5}'.format(
                EnvConfig.MYSQL_BIN, self._DB_CONFIG_DICT[db]['host'],
                self._DB_CONFIG_DICT[db]['port'],
                self._DB_CONFIG_DICT[db]['user'],
                self._DB_CONFIG_DICT[db]['password'],
                self._DB_CONFIG_DICT[db]['db_name'])
        bash_cmd = "echo '{0}{1}' | {2}".format(set_names_cmd, sql_cmd,
                connect_cmd)
        status, output = commands.getstatusoutput(
                bash_cmd.decode('utf8').encode('gbk'))
        if status == 0:
            if output_file is not None:
                output_obj = open(output_file, 'w')
                output_obj.write(output)
        else:
            logger.critical('Returned: {0}: {1}\t{2}'.format(status, bash_cmd,
                output))
        return status == 0

    @staticmethod
    def convertFile(file_path):
        """
        # @Synopsis  convert a gbk file to utf8
        #
        # @Args file_path
        #
        # @Returns succeeded or not
        """
        line_cnt = 0
        none_gbk_line_cnt = 0
        file_obj = open(file_path)

        tmp_file = os.path.join(EnvConfig.LOCAL_DATA_PATH, 'tmp')
        tmp_file_obj = open(tmp_file, 'w')
        with file_obj, tmp_file_obj:
            for line in file_obj:
                line_cnt += 1
                try:
                    utf_line = line.decode('gbk', errors='ignore')\
                            .encode('utf-8')
                except Exception as e:
                    none_gbk_line_cnt += 1
                tmp_file_obj.write(utf_line)

        os.remove(file_path)
        shutil.copy(tmp_file, file_path)
        os.remove(tmp_file)
        logger.debug('{0} converted, total line {1}, non-gbk line {2}'\
                .format(file_path, line_cnt, none_gbk_line_cnt))

        return 0 #success check to be done


    def getDbFile(self, sql_cmd, output_path, db='Attr'):
        """
        # @Synopsis get result of a sql command, dump into file and convert to
        # utf8
        """
        succeeded = Mysql.sqlExec(self, sql_cmd, output_path, db)
        if succeeded:
            try:
                Mysql.convertFile(output_path)
            except Exception as e:
                logger.critical('convert {0} failed'.format(output_path))
                succeeded = False
        return succeeded



if __name__ == '__main__':
    sql_cmd = "select works_id, title, title_eng, net_show_time from movie_final"
    output_file = EnvConfig.LOCAL_FINAL_TABLE_PATH_DICT['movie']
    mysql_dal = Mysql()
    mysql_dal.sqlExec(sql_cmd, output_file, 'Final')
    # print Mysql.getInstanceByService('dbbk-videodetail.xdb.all')
