#!/usr/bin/env python

''' <docs>
'''

#-- standard libs
import sqlite3, os, logging

#-- 3rd party libs
import numpy  as np
import pandas as pd

#-- my custom libs

#==============================================================================#
#------------------------- setup and such -------------------------------------#
#==============================================================================#

def _logger(fname):
    logger = logging.getLogger()

    file_log = logging.FileHandler(fname + '.log')
    fmt      = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    file_log.setFormatter(fmt)
    logger.addHandler(file_log)

    tty_log = logging.StreamHandler()
    tty_log.setFormatter(fmt)
    logger.addHandler(tty_log)

    logger.setLevel(logging.INFO)
    logger.info('\n'.join(('*','*','*','*')))
    return logger
#------------------------------------------------------------------------------#

logger = _logger(__file__)

#==============================================================================#
#------------------------- main -----------------------------------------------#
#==============================================================================#





#--------------------------------------#
#-- <....>
#--------------------------------------#
