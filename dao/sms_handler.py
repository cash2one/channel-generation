"""
# @file sms_handler.py
# @Synopsis  custom logging handler to send short message
# @author Ming Gu(guming02@baidu.com))
# @version 1.0
# @date 2015-09-19
"""
import sys
sys.path.append('..')
import logging
from sms import SMS
from conf.env_config import EnvConfig

class SMSHandler(logging.Handler):
    """
    # @Synopsis  inherit logging.Handler to make a customized log handler to send
    # email, because I don't know how the provided logging.handlers.SMTPHandler
    # works with the linux system mail service
    """

    def emit(self, record):
        """
        # @Synopsis  override emit method, to deal with the logging record
        #
        # @Args record
        #
        # @Returns nothing
        """
        msg = self.format(record)
        SMS.sendMessage(EnvConfig.SMS_RECEIVERS, msg)

