'''
Created on Jun 18, 2012

@author: mraposa
'''
import ConfigParser
import os
import logging

from FileHelper2x import copyFileToDirectory, get_file_list, moveFileToDirectory, delete_empty_folders
from StringUtils import replace_case_insensitive

from global_variables import CONFIG_INI_FILE, OPAL_ROOT_DIR, LOG_NAME

log = logging.getLogger("%s.%s" % (LOG_NAME,__name__))

def distribute_opal_assets_to_opal_system():
    '''
    Moves files from the OPAL_OUTPUT folder in JukeBox to the appropriate folder in the Prod\Phase_1\Aspera
    system folders for Opal.
    
    In essence this fans out OPAL MSOs, e.g. OPAL_WOW fans out to OPAL_WOW_CHI, OPAL_WOW_DET, etc.
    '''
    log.info("Distributing Opal Assets into Opal Aspera folders")
    log.info("Reading configuration data from file: %s" % CONFIG_INI_FILE)
    config = ConfigParser.SafeConfigParser()
    config.read(CONFIG_INI_FILE)
    for (opal_dir, output_dirs) in config.items("OPAL_OUTPUT"):
        src_root_dir = os.path.join(OPAL_ROOT_DIR, opal_dir)
        log.info("Working on source folder: %s" % src_root_dir)
        for src_file in get_file_list(src_root_dir, recursive=True, includeDirNames=False, excludeHidden=True, patterns="*.*"):
            for idx, output_root_dir in enumerate(output_dirs.split(",")):
                #    Getting the final output path of the file by replacing the source root path with the output root path
                final_path = replace_case_insensitive(src_file, src_root_dir, output_root_dir)
                final_path = os.path.dirname(final_path)
                #    If the number of output folders is greater than 1 and you are not processing the last folder
                if len(output_dirs.split(",")) > 1 and (idx+1) < len(output_dirs.split(",")):
                    log.info("Copying file from: %s" % src_file)
                    log.info("To: %s" % final_path)
                    copyFileToDirectory(src_file, final_path, preserveFileDate=True)
                #    If you are the last folder, move the file instead of copying
                else:
                    log.info("Moving file from: %s" % src_file)
                    log.info("To: %s" % final_path)
                    moveFileToDirectory(src_file, final_path)                 
        log.info("Delete empty folders under: %s" % src_root_dir)
        delete_empty_folders(src_root_dir, recursive=True)
        
def execute_post_processing():
    distribute_opal_assets_to_opal_system()
            
if __name__ == '__main__':
    distribute_opal_assets_to_opal_system()
    
