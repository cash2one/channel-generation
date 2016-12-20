"""
# @file tags_gen.py
# @Synopsis  gen tags
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-20
"""

import sys
sys.path.append('..')
import commands
from conf.env_config import EnvConfig
from conf.init_logger import InitLogger
from dao.spark_submit import SparkSubmit
import logging

if __name__ == '__main__':
    InitLogger()
    logger = logging.getLogger(EnvConfig.LOG_NAME)
    main_program_path = '../bll/tags_gen/tags_gen.py'
    works_type = sys.argv[1]
    bash_cmd = 'sed -i "s/^\s*works_type = .*$/    works_type = \'{0}\'/" {1}'\
            .format(works_type, main_program_path)
    status, output = commands.getstatusoutput(bash_cmd)
    logger.debug('Returned {0}: {1}\n{2}'.format(status, bash_cmd, output))
    SparkSubmit.sparkSubmit(main_program_path, run_locally=False)
