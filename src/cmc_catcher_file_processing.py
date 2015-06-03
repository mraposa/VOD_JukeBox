'''
Downloads packages from the CMC Catcher in Denver. Creates a package and gives the package to JukeBox
for processing

Created on Apr 26, 2012

@author: mraposa
'''
import logging
import os
from time import sleep

from NetworkUtils import map_drive, ping, where_am_I
from OperationsHelper import PackageHelper, InvalidPackage
from FileHelper2x import copyFileToDirectory, get_file_list
from MailHelper2x import sendSimpleEmail, send_mail
from Xml2x import checkDtdFiles, InvalidAdiXmlError

from global_variables import CMC_CATCHER_PATH, CMC_CATCHER_PWD, CMC_CATCHER_UID, JUKEBOX_PACKAGE_DIR, LOG_NAME, EMAIL_ADDR
from global_variables import PACKAGE_Q, CMC_CATCHER_IP, JUKEBOX_DIR
from utilities import add_jobs_to_JukeBoxJobStatus

log = logging.getLogger("%s.%s" % (LOG_NAME,__name__))

def delete_CMC_test_assets(cmc_directory):
    '''
    CMC delivers test assets over the CMC catcher. These assets need to be deleted.
    '''
    log.info("Deleting CMC Test Assets")
    cmcTestFiles = [] 
    cmcTestFiles.extend(get_file_list(cmc_directory, patterns="Clean_Asset_CMC_TEST*.xml"))
    cmcTestFiles.extend(get_file_list(cmc_directory, patterns="CMCTestAsset_CMC_TEST_*.xml"))
    for f in cmcTestFiles:
        if os.path.exists(f):
            log.info("Deleting CMC Test file: %s" % f)
            os.remove(f)
            
def connect_to_CMC_catcher(cmc_drive_letter):
    log.info("Disconnecting %s" % cmc_drive_letter)
    map_drive(cmc_drive_letter, disconnect=True)
    log.info("Pinging Catcher IP: %s" % CMC_CATCHER_IP)
    ping_results = ping(CMC_CATCHER_IP)
    if not ping_results["exit_code"] == 0:
        log.warn("CMC Catcher is not responding to pings")
        log.info("Ping Output: %s" % ping_results["output_txt"])
        msg = """
        CMC Catcher appears to be down
        CMC Catcher is not responding to pings
        Unable to download assets from CMC Catcher
        CMC Catcher IP: %s
        Percent Ping Packet Loss: %s
        """ % (CMC_CATCHER_IP, ping_results["percent_lost"])
        log.warn(msg)
        log.info("Sending email to %s" % EMAIL_ADDR)
        sendSimpleEmail(EMAIL_ADDR, "JukeBox -- General Processing Error" , msg)
        log.warn("Exiting download_packages_from_cmc_catcher with return code 1")
        return 1
    else:
        log.info("CMC Catcher is responding to pings")
    log.info("Mapping to %s" % CMC_CATCHER_PATH)
    if not map_drive(cmc_drive_letter, CMC_CATCHER_PATH, uid=CMC_CATCHER_UID, pwd=CMC_CATCHER_PWD, disconnect=False) == 0:
        log.warn("Failed to map drive to %s" % CMC_CATCHER_PATH)
        msg = """
        Failed to map drive to CMC Catcher
        CMC Catcher is responding to pings
        Unable to download assets from CMC Catcher
        CMC Catcher IP: %s
        CMC Drive Path: %s
        """ % (CMC_CATCHER_IP, CMC_CATCHER_PATH)
        log.warn(msg)
        log.info("Sending email to %s" % EMAIL_ADDR)
        sendSimpleEmail(EMAIL_ADDR, "JukeBox -- General Processing Error" , msg)
        log.warn("Exiting download_packages_from_cmc_catcher with return code 2")
        return 2
    return 0


def check_content_checksums(new_adi_XML):
    log.info("Checking content file checksums")
    package_chksum = PackageHelper(new_adi_XML, "%s.%s" % (LOG_NAME, __name__))
    try:
        package_chksum.check_content_file_checksums()
    except InvalidPackage as e:
        msg = """
        CHECKSUM MISMATCH!!!
    
        Checksum in XML does not match Content File
     
        XML will be renamed with a .BAD extension
    
        Package: {}   
        XML: {}
        Error Details: 
        {}   
        """.format(package_chksum.get_package_name(), new_adi_XML, e)
        log.error("Checksums do NOT match")
        log.error(msg)
        log.info("Sending email")
        sendSimpleEmail("mraposa@indemand.com", "ERROR! -- CMC File Copy Checksum Mismatch", msg)
    log.info("Checksums match")   


def download_packages_from_cmc_catcher():
    '''
    Download packages from CMC Catcher. Create packages for Jukebox ingest.
    '''
    cmc_drive_letter = "R:"
    connect_results = connect_to_CMC_catcher(cmc_drive_letter)
    if not connect_results == 0:
        return connect_results
    delete_CMC_test_assets("%s\\" % cmc_drive_letter)
    for xml_file in get_file_list("%s\\" % cmc_drive_letter, patterns="*.xml"):
        log.info("Processing file: %s" % xml_file)
        checkDtdFiles("%s\\" % cmc_drive_letter, adiDTD=True)
        try:
            package = PackageHelper(xml_file, "%s.%s" % (LOG_NAME,__name__))
        except InvalidAdiXmlError as err:
            errString = "%s" % unicode(err)
            log.error("Unable to Parse XML: %s" % xml_file)
            log.error("Error Message: %s" % errString)
            msg = """
            Unable to parse CMC XML File.
            This file will be skipped
            XML: %s
            Error Message: %s
            
            XML File Attached
            """ % (xml_file, errString)
            subject = "JukeBox -- General Processing Error" 
            log.info("Sending email")
            send_mail(EMAIL_ADDR, subject, msg, files=[xml_file,])   
        log.info("Package Name: %s" % package.get_package_name())
        if not package.is_package_ready():
            log.warn("Package not ready for processing")
            log.warn("Package will be skipped")
            msg = """
            CMC Package not ready for packaging
            Either a content file is missing or is in use.
            This package will be skipped.
            Package Name: %s
            ADI.XML File: %s
            """ % (package.get_package_name(), xml_file)
            log.info("Sending Email...")
            sendSimpleEmail(EMAIL_ADDR, "CMC Package Not Ready", msg)
            continue
        # As of 9.20.13 CMC delivers the same assets in NYC and Denver
        # But these assets can only be processed once regardless of location
        # Logic below tells packager to put assets in a tmp folder if they are not 
        # going to be processed   
        if where_am_I() == "NYC":
            out_dir = os.path.join(JUKEBOX_PACKAGE_DIR, package.get_package_name())
            check_checksums = True
            update_status_and_priority_q = True
        elif where_am_I() == "DEN":
            tmp_dir = os.path.join(JUKEBOX_DIR, "TMP_CMC_PKG_DIR")
            out_dir = os.path.join(tmp_dir, package.get_package_name())
            check_checksums = True
            update_status_and_priority_q = False
        else:
            raise ValueError("Could not determine location. where_am_I(): {}".format(where_am_I()))
        if not os.path.exists(out_dir):
            log.info("Creating package dir: %s" % out_dir)
            os.makedirs(out_dir)
        for f in package.get_package_file_list():
            log.info("Copying %s to %s" % (os.path.basename(f), out_dir))
            try:
                if os.path.exists(f):
                    log.info("Confirmed that file exists: {}".format(f))
                else:
                    log.warning("File not found: {}".format(f))
                copyFileToDirectory(f, out_dir)
            except WindowsError as e:
                log.warning(e)
                log.warning("Retrying Copy")
                sleep(5)
                connect_results = connect_to_CMC_catcher(cmc_drive_letter)
                if not connect_results == 0:
                    return connect_results
                sleep(5)
                if os.path.exists(f):
                    log.info("Confirmed that file exists: {}".format(f))
                else:
                    log.warning("File not found: {}".format(f))
                copyFileToDirectory(f, out_dir)      
        for f in package.get_package_file_list():
            log.info("Deleting original file %s" % f)
            os.remove(f)
        new_adi_XML = os.path.join(out_dir, "ADI.XML")
        log.info("Renaming: %s to ADI.XML" % os.path.join(out_dir, os.path.basename(xml_file)))
        os.rename(os.path.join(out_dir, os.path.basename(xml_file)), new_adi_XML)
        if os.path.exists(os.path.join(out_dir, os.path.basename(xml_file))):
            os.remove(os.path.join(out_dir, os.path.basename(xml_file)))
        if check_checksums:
            check_content_checksums(new_adi_XML)
        if update_status_and_priority_q:
            log.info("Adding Package to queue: %s" % new_adi_XML)
            PACKAGE_Q.put(new_adi_XML)
            log.info("Updating JukeBox Job Status")
            add_jobs_to_JukeBoxJobStatus()
        msg = """
        Package successfully downloaded from Denver CMC Catcher
        Package: %s
        """ % package.get_package_name()
        sendSimpleEmail(EMAIL_ADDR, "CMC Catcher Delivery -- %s" % package.get_package_name(), msg)
    
    log.info("Finished getting packages from CMC catcher")  
    log.info("Disconnecting %s" % cmc_drive_letter)
    map_drive(cmc_drive_letter, disconnect=True)  

if __name__ == '__main__':
    download_packages_from_cmc_catcher()