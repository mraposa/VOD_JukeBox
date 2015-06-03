'''
Processes incoming transport based packages. Converts Metadata for output destinations. Creates
output packages.
Created on Nov 7, 2011

@author: mraposa
'''
import sys
import os
import thread
import random
from time import sleep

from GeneralUtilities import displayPID, handle_application_failure
from LogFileHelper2x import get_default_logger

from global_variables import LOG_FILENAME, LOG_NAME, CONSOLE_LOGGING, COMPUTER_NAME 
from global_variables import PREPROCESSING_MACHINES, EMAIL_ADDR_EXCEPTIONS, POSTPROCESSING_MACHINES
from preProcessing import executePreProcessing
from post_processing import execute_post_processing
from processing import executeProcessing
from NetworkUtils import where_am_I

log = get_default_logger(LOG_FILENAME, LOG_NAME, console_logging=CONSOLE_LOGGING)

def executeTask():
    log.info("Starting JukeBox")
    log.info("Task Running On Machine: %s" % COMPUTER_NAME)
    log.info("Task running in %s" % where_am_I())
    if COMPUTER_NAME in PREPROCESSING_MACHINES:
        log.info("Starting Pre-Processing...")
        executePreProcessing()
    else:
        sleepTime = random.randint(300, 600)
        log.info("Pausing for %s seconds to avoid collisions with other engines" % sleepTime)
        sleep(sleepTime)
    log.info("Starting Main Processing...")
    executeProcessing()
    if COMPUTER_NAME in POSTPROCESSING_MACHINES:
        log.info("Starting Post Processing...")
        execute_post_processing()

if __name__ == '__main__':
    try:
        thread.start_new_thread(displayPID, ())
        executeTask()
    except Exception:
        application = os.path.basename(__file__)
        handle_application_failure(application, LOG_NAME, LOG_FILENAME, email_addr=EMAIL_ADDR_EXCEPTIONS)
        
    print("\nAll done!")
    log.info("All done!")
    sys.exit(0)