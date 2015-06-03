'''
Created on Nov 8, 2011

@author: mraposa
'''
import socket
import os
from GeneralDbUtilities import connectToDatabase
from OperationsHelper import PackagePriorityQueue
from NetworkUtils import where_am_I

COMPUTER_NAME = socket.gethostname()

if COMPUTER_NAME == 'mjrws006':
    PROD = False
    ECHO_DEBUG = False
    JUKEBOX_DB = 'mssql+pyodbc://JukeBox_Dev'
    CONSOLE_LOGGING = True
    EMAIL_ADDR = "mraposa@indemand.com"
    OPERATIONS_DB = 'mssql+pyodbc://Operations_Dev'
    #    Directory for TAR outputs
    TAR_ROOT_DIR = "\\\\vc67\\vodstorage\\PrePROD\\tech\\mraposa_workspace\\Dev_Test\\PROD\\packages\\DropFolder\\fod\\"
    #    Directory for Opal Outputs
    JUKEBOX_DIR = "\\\\vc67\\vodstorage\\PrePROD\\tech\\mraposa_workspace\\Dev_Test\\GENERAL_PROCESSING\\JUKE_BOX"
    CMC_CATCHER_PATH = r"\\vc67\vodstorage\PrePROD\tech\mraposa_workspace\Dev_Test\CMC_CATCHER"
    CMC_CATCHER_UID = "vcnyc\mraposa" 
    CMC_CATCHER_PWD = "izod01"
    CMC_CATCHER_IP = "192.168.31.51"
    LOG_NAME = "Main_Dev"
    PREPROCESSING_MACHINES = ("mjrws006",)
    POSTPROCESSING_MACHINES = ("mjrws006",)
    CMC_CATCHER_DOWNLOAD_MACHINES = ("DENAUTO102", "DENAUTO03") 
else:
    PROD = True
    ECHO_DEBUG = False
    JUKEBOX_DB = 'mssql+pyodbc://JukeBox'
    CONSOLE_LOGGING = False
    EMAIL_ADDR = "Juke_Admin@indemand.com"
    OPERATIONS_DB = 'mssql+pyodbc://OperationsDB'
    LOG_NAME = "Main"
    CMC_CATCHER_DOWNLOAD_MACHINES = ("DENAUTO102", "AUTO107")
    POSTPROCESSING_MACHINES = ("AUTO107",)
    PREPROCESSING_MACHINES = ("AUTO107", "DENAUTO102") 
    if where_am_I() == "NYC":
        #    Directory for Opal Outputs
        JUKEBOX_DIR = "\\\\vc67\\vodstorage\\PreProd\\Phase_1\\GENERAL_PROCESSING\\JUKE_BOX\\"
        #    Directory for TAR outputs
        TAR_ROOT_DIR = r"\\vc67\vodstorage\PrePROD\Phase_1\GENERAL_PROCESSING\PYRITE\FROM_JUKEBOX"
        CMC_CATCHER_PATH = r"\\10.12.3.39\indemand"
        CMC_CATCHER_UID = "10.12.3.39\indemand2" 
        CMC_CATCHER_PWD = "indemand2"
        CMC_CATCHER_IP = "10.12.3.39"
    
    if where_am_I() == "DEN":
        #    Directory for TAR outputs
        TAR_ROOT_DIR = r"\\vcden02\vodstorage\packages\fod"
        #    Directory for Opal Outputs
        JUKEBOX_DIR = r"\\vcden02\vodstorage\Phase_1\GENERAL_PROCESSING\JUKE_BOX"
        CMC_CATCHER_PATH = r"\\192.168.31.51\indemand"
        CMC_CATCHER_UID = "192.168.31.51\indemand2" 
        CMC_CATCHER_PWD = "indemand2"
        CMC_CATCHER_IP = "192.168.31.51"
        
EMAIL_ADDR_EXCEPTIONS = "mraposa@indemand.com"
SESSION_JUKEBOX = connectToDatabase(JUKEBOX_DB)
SESSION_OPERATIONS = connectToDatabase(OPERATIONS_DB)
MAX_BACKUP_AGE_DAYS = 45
LOG_FILENAME = "\\\\emc03\\logs\\VOD\\JukeBox\\JukeBox_%s_log.txt" % COMPUTER_NAME
#    Original package delivered to indemand or extracted TAR file delivered to indemand
JUKEBOX_PACKAGE_DIR = os.path.join(JUKEBOX_DIR, "INCOMING_PACKAGES")
JUKEBOX_BACKUP_DIR = os.path.join(JUKEBOX_DIR, "BACKUP")
#    Original TARs delivered to indemand
JUKEBOX_TAR_DIR = os.path.join(JUKEBOX_DIR, "INCOMING_TARS")
JUKEBOX_CONFIG_DIR = os.path.join(JUKEBOX_DIR, "CONFIG_FILES")
CONFIG_INI_FILE = os.path.join(JUKEBOX_CONFIG_DIR, "config.ini")  
OPAL_ROOT_DIR = os.path.join(JUKEBOX_DIR, "OPAL_OUTPUT")
#    Directory for ADI Only TAR outputs
ADI_TAR_ROOT_DIR = os.path.join(JUKEBOX_DIR, 'PACKAGED_OUTPUT_ADI_TAR')
PACKAGE_Q = PackagePriorityQueue()


