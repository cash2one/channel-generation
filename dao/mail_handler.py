"""
# @file mail_handler.py
# @Synopsis  custom logging handler to send email
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-19
"""

import sys
sys.path.append('..')
import logging
from mail import Mail
from conf.env_config import EnvConfig

class MailHandler(logging.Handler):
    """
    # @Synopsis  customized handler, to email critical log
    """

    def emit(self, record):
        """
        # @Synopsis  override logging.Handler emit method, the action when receive
        # the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        Mail.sendMail(EnvConfig.MAIL_RECEIVERS, 'PROGRAM ALARM', msg)
