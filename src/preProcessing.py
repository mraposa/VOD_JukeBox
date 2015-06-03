
from FileHelper2x import get_file_list, isFileLocked
from global_variables import JUKEBOX_PACKAGE_DIR, JUKEBOX_BACKUP_DIR, JUKEBOX_TAR_DIR, MAX_BACKUP_AGE_DAYS, LOG_NAME
from time import time
import os, logging, tarfile, shutil

log = logging.getLogger("%s.%s" % (LOG_NAME,__name__))

def cleanBackup():
    '''
    Cleanup the backup folder and delete old files.
    '''
    log.info("Starting cleanup of backup folder %s" % JUKEBOX_BACKUP_DIR)
    log.info("Deleting files older than %s days" % MAX_BACKUP_AGE_DAYS)
    fileList = get_file_list(JUKEBOX_BACKUP_DIR, recursive=True)
    curtime = time()
    for f in fileList:
        ftime = os.path.getmtime(f)
        difftime = curtime - ftime
        if difftime > (MAX_BACKUP_AGE_DAYS*60*60*24):
            log.info("Deleting backup file %s" % f)
            os.remove(f)
    
    # Delete Empty backup folders
    for dirName in get_file_list(JUKEBOX_BACKUP_DIR, recursive=False, includeDirNames=True):
        if os.path.isdir(dirName):
            if len(get_file_list(dirName, excludeHidden=True)) == 0:
                log.info("Removing empty directory: %s" % dirName)
                shutil.rmtree(dirName, ignore_errors=True)
                
    log.info("Finished cleanup of backup folder %s" % JUKEBOX_BACKUP_DIR)
    
def extractTarFiles():
    '''
    Extract Tar Files
    '''
    log.info("Starting to Extract Incoming Tar Files")
    for f in get_file_list(JUKEBOX_TAR_DIR, patterns="*.tar"):
        log.info("Working on file: %s" % f)
        if isFileLocked(f, readLockCheck=True):
            log.warning("File is locked. File will be skipped")
            continue
        #log.info("Copying file to backup folder: %s" % JUKEBOX_BACKUP_DIR)
        #copyFileToDirectory(f, JUKEBOX_BACKUP_DIR)
        pkgDir = os.path.join(JUKEBOX_PACKAGE_DIR, os.path.splitext(os.path.basename(f))[0])
        if not os.path.exists(pkgDir):
            os.mkdir(pkgDir)
        tar = tarfile.open(f)
        log.info("Extracting file to %s" % pkgDir)
        tar.extractall(pkgDir)
        tar.close()
        log.info("Deleting original file %s" % f)
        os.remove(f)
    log.info("Finished Extract Incoming Tar Files")

def executePreProcessing():
    cleanBackup()
    extractTarFiles()