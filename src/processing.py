import os
import logging
import shutil
import tempfile
from lxml import etree
from copy import deepcopy
from datetime import datetime

from sqlalchemy import or_

from FileHelper2x import copyFileToDirectory
from MailHelper2x import sendSimpleEmail
from tar_file import create_tar_package
from Xml2x import ADI_XML, CHANGE_XML, checkDtdFiles
from OperationsHelper import PackageHelper, InvalidPackage

from global_variables import OPAL_ROOT_DIR, TAR_ROOT_DIR, JUKEBOX_CONFIG_DIR, EMAIL_ADDR, LOG_NAME
from global_variables import SESSION_JUKEBOX, COMPUTER_NAME, PACKAGE_Q, CMC_CATCHER_DOWNLOAD_MACHINES, ADI_TAR_ROOT_DIR
from utilities import saveToPackagesTable, convertProducts, convertProviders, convertCategories, convertProviderContentTier, save_completed_destination
from utilities import put_packages_into_queue, copy_package_to_backup, isPkgReady, get_file_list_string_for_completed_destinations
from exception_classes import GeneralProcessingError, ProviderContentTierNotFoundError, DeliverySettingNotFoundError
from models import Destination, ContentTierDestinationMapping, MsoMapping, CompletedDestination, JukeBoxJobStatus
from cmc_catcher_file_processing import download_packages_from_cmc_catcher

log = logging.getLogger("%s.%s" % (LOG_NAME,__name__))
        
def getPkgFileList(adiXML):
    '''
    Gets a partial list of files for an ADI Package
    Will not include CHANGE.XML or CHANGE.DTD
    @param adiXML:
    '''
    fileList = []
    rootFolder = os.path.dirname(adiXML.xmlFile)
    for _assetClass, fileName in adiXML.getContentFileNames().items():
        fileList.append(os.path.join(rootFolder, fileName))   
    fileList.append(os.path.join(rootFolder, "ADI.DTD"))
    return fileList

def getPackageSize(pkgFileList):
    '''
    Get the size of all the files in a package
    @param pkgFileList:
    '''
    size = 0
    for f in pkgFileList:
        size += os.path.getsize(f)
    return size

def get_MSO_Mappings(contentTier):
    '''
    Get list of MSOs Mappings for a given incoming Provider_Content_Tier
    @param contentTier:
    '''
    msoList = set()
    #    Query for all the Content Tier Destinations for this Content Tier
    for contentTierDestinationMapping in SESSION_JUKEBOX.query(ContentTierDestinationMapping).filter(ContentTierDestinationMapping.contentTier.like(contentTier)).all():
        #    Query for the MSOs for this destID
        query = SESSION_JUKEBOX.query(MsoMapping).filter(MsoMapping.destID == contentTierDestinationMapping.destID) 
        if query.count():
            msoMapping = query.one()
            msoList.add(msoMapping)
    return msoList  

def processAdiOnlyDestinations(adiXML, destination, contentTier):
    '''
    Process destinations that have two requirements:
    1. The final package is a directory with package files. Not a Tar
    2. The final package is ADI.XML only. No Change.xml
    @param adiXML:
    @param destination:    Destination DB Object
    '''
    startTime = datetime.now()
    finalADI_File = ""
    log.info("Mapping Source Categories to Destination Categories")
    for categoryElement in adiXML.xmlTree.xpath("/ADI/Asset/Metadata/App_Data[@Name='Category']"):
        convertCategories(categoryElement, adiXML, destination=destination)
    
    log.info("Mapping Source Product to Destination Product")
    convertProducts(adiXML.root, adiXML, destination=destination)
    
    log.info("Mapping Source Provider to Destination Provider")
    convertProviders(adiXML.root, adiXML, destination=destination)
    
    log.info("Removing duplicate categories")
    adiXML.removeDuplicateCategories()
    
    deliverySetting = destination.deliverySetting
        
    if deliverySetting.deliveryMethod == "OPAL":
        outputDir = os.path.join(OPAL_ROOT_DIR, deliverySetting.opalFolder)
        outputDir = os.path.join(outputDir, "OUTBOUND")
        outputDir = os.path.join(outputDir, adiXML.getUniqueIdAsString())
        log.info("Output Directory: %s" % outputDir)
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        log.info("Saving ADI.XML to outputDir")
        finalADI_File = os.path.join(outputDir, "ADI.XML")
        adiXML.saveXML(finalADI_File)
        with open(finalADI_File, "r") as f:
            log.info("ADI.XML: %s" % f.read())
        pkgFileList = getPkgFileList(adiXML)  
        for f in getPkgFileList(adiXML):
            log.info("Copying file %s to outputDir" % f)
            copyFileToDirectory(f, outputDir)
        #    This append necessary for saving Completed Destination file_list correctly
        pkgFileList.append(finalADI_File)
        #    Saving to packages table with just ADI.XML information. Will try to save later if standard TAR file
        #    is created
        saveToPackagesTable(finalADI_File)
        log.info("Saving completed destination to CompletedDestination Table")
        save_completed_destination(adiXML=adiXML, 
                               changeXML=None, 
                               destination=destination, 
                               mso_mappings=None, 
                               file_list = get_file_list_string_for_completed_destinations(pkgFileList), 
                               contentTier=contentTier, 
                               startTime=startTime, 
                               pkgSize=getPackageSize(pkgFileList)
                               )

    if deliverySetting.deliveryMethod == "ADIOnlyTar":
        #    Provider Content Tier is only changed for TAR Outputs. It is unnecessary for OPAL
        #    destinations
        convertProviderContentTier(adiXML, destination)
        outputDir = ADI_TAR_ROOT_DIR
        log.info("Output Directory: %s" % outputDir)
        log.info("Saving ADI.XML to %s" % tempfile.gettempdir())
        finalADI_File = os.path.join(tempfile.gettempdir(), "ADI.XML")
        adiXML.saveXML(finalADI_File)
        with open(finalADI_File, "r") as f:
            log.info("ADI.XML: %s" % f.read())
        pkgFileList = getPkgFileList(adiXML)
        pkgFileList.append(finalADI_File)
        log.info("Files destined for Package TAR File:")
        for f in pkgFileList:
            log.info("%s" % f)
        newContentTier = adiXML.getAttributeValue("/ADI/Metadata/App_Data[@Name='Provider_Content_Tier']/@Value")
        #    It is necessary to create this long TAR name so that ADIOnlyTars don't have a name collision
        #    with standard Tars
        f = os.path.join(outputDir, "%s_%s.tar" % (adiXML.getUniqueIdAsString(), newContentTier))
        log.info("Output Tar File: %s" % f)
        log.info("Creating Tar File")
        create_tar_package(pkgFileList, output_file=f, logger_name="{}.processing".format(LOG_NAME), overwrite_existing=True)
        #    Saving to packages table with just ADI.XML information. Will try to save later if standard TAR file
        #    is created
        saveToPackagesTable(finalADI_File)
        log.info("Saving completed destination to CompletedDestination Table")
        save_completed_destination(adiXML=adiXML, 
                               changeXML=None, 
                               destination=destination, 
                               mso_mappings=None, 
                               file_list = get_file_list_string_for_completed_destinations([f,]), 
                               contentTier=contentTier, 
                               startTime=startTime, 
                               pkgSize=getPackageSize(pkgFileList)
                               )

def processTarDestinations(adiXML, contentTier):
    '''
    Process destinations that have two requirements
    1. The destination package is a standard TAR file
    2. The package includes a CHANGE.XML
    @param adiXML:    
    @param msoList:    Dictionary of MSOs for CHANGE.XML
    '''
    startTime = datetime.now()
    finalADI_File = ""
    finalCHANGE_File = ""
    log.info("Creating Blank Change.XML")
    changeXML = CHANGE_XML(os.path.join(JUKEBOX_CONFIG_DIR, "Base_Change.XML"))
    changeXML.parseXML()
    log.info("Getting list of MSOs for this TAR Package")
    mso_mappings = get_MSO_Mappings(contentTier)
    log.info("Adding MSO Elements to CHANGE.XML")
    for mso_mapping in mso_mappings:
        log.info("Adding MSO Element for %s" % mso_mapping.mso.lower())
        msoElement = etree.Element("MSO")
        msoElement.set("Name", mso_mapping.mso.lower())
        changeXML.root.append(msoElement)
    
    log.info("Adding ADI Elements from ADI.XML into each MSO section")
    for msoElement in changeXML.root.findall('.//MSO'):
        msoElement.append(deepcopy(adiXML.root))
    
    log.info("Mapping Source Categories to Destination Categories")
    for msoElement in changeXML.root.findall('.//MSO'):
        mso = msoElement.get("Name")
        log.info("Working on MSO: %s" % mso)
        for appDataElement in msoElement.findall(".//ADI//Asset//Metadata//App_Data"):
            if appDataElement.get("Name") == "Category":
                convertCategories(appDataElement, adiXML, mso=mso)
        log.info("Mapping Source Product to Destination Product")
        convertProducts(msoElement.find(".//ADI"), adiXML, mso=mso)
        
        log.info("Mapping Source Provider to Destination Provider")
        convertProviders(msoElement.find(".//ADI"), adiXML, mso=mso)
    
    log.info("Removing duplicate categories")
    changeXML.removeDuplicateCategories()
    
    outputDir = TAR_ROOT_DIR
    log.info("Output Directory: %s" % outputDir)
    log.info("Saving Temporary ADI.XML to %s" % tempfile.gettempdir())
    finalADI_File = os.path.join(tempfile.gettempdir(), "ADI.XML")
    adiXML.saveXML(finalADI_File)
    log.info("Saving Temporary CHANGE.XML to %s" % tempfile.gettempdir())
    finalCHANGE_File = os.path.join(tempfile.gettempdir(), "CHANGE.XML")
    changeXML.saveXML(finalCHANGE_File)
    #    These next two steps open, parse, and then re-save the CHANGE.XML
    #    This is done because the original saved XML although valid is all on a single line and hence not "people" readable
    #    I suspect that this formatting issue was due to creating the CHANGE.XML from a base blank change.xml
    #    Re-parsing and saving the XML resolves the problem
    tmp = CHANGE_XML(finalCHANGE_File)
    tmp.saveXML(addEditedBy=False)
    with open(finalADI_File, "r") as f:
        log.info("ADI.XML: %s" % f.read())
    with open(finalCHANGE_File, "r") as f:
        log.info("CHANGE.XML: %s" % f.read())
    pkgFileList = getPkgFileList(adiXML)
    pkgFileList.append(finalADI_File)
    pkgFileList.append(finalCHANGE_File)
    pkgFileList.append(os.path.join(JUKEBOX_CONFIG_DIR, "CHANGE.DTD"))
    
    log.info("Files destined for Package TAR File:")
    for f in pkgFileList:
        log.info("%s" % f)
    f = os.path.join(outputDir, "%s.tar" % adiXML.getUniqueIdAsString())
    log.info("Output Tar File: %s" % f)
    log.info("Creating Tar File")
    create_tar_package(pkgFileList, output_file=f, logger_name="{}.processing".format(LOG_NAME), overwrite_existing=True)
    
    log.info("Saving Package data to packages table")
    #    The save to the packages table is only done for the TAR file. The tar files is the most 
    #    all encompassing files as it contains the TAR size and the change.xml
    #    Saving the other deliveries, e.g. OPAL, would create duplicate entries in the Packages table
    checkDtdFiles(tempfile.gettempdir(), adiDTD=True, changeDTD=False)
    saveToPackagesTable(finalADI_File, finalCHANGE_File, os.path.getsize(f))
    log.info("Saving completed destination to CompletedDestination Table")
    #    No destID is saved to completed destination for a standard TAR. A standard TAR includes
    #    multiple destID, so it doesn't make sense to save the destID.
    #    This information for TAR files is saved for historical reporting only
    save_completed_destination(adiXML=None, 
                               changeXML=changeXML, 
                               destination=None, 
                               mso_mappings=mso_mappings, 
                               file_list=get_file_list_string_for_completed_destinations([f,]), 
                               contentTier=contentTier, 
                               startTime=startTime, 
                               pkgSize=os.path.getsize(f))
 
def create_packages(adiXmlFile):
    log.info("Working on ADI: %s" % adiXmlFile)
    createStandardTar = False         
    adiXML = ADI_XML(adiXmlFile)
    contentTier = adiXML.getAttributeValue("/ADI/Metadata/App_Data[@Name='Provider_Content_Tier']/@Value")
    log.info("Provider Content Tier: %s" % contentTier)
    query = SESSION_JUKEBOX.query(ContentTierDestinationMapping).filter(ContentTierDestinationMapping.contentTier.like(contentTier))
    if query.count() == 0:
        errMsg = """
        Error: Provider Content Tier was not found in the Content Tier Destination Mapping Table
        Source Provider Content Tier: %s
        Package Name: %s
        Source ADI.XML: %s
        """ % (contentTier, adiXML.getUniqueIdAsString(), adiXML.xmlFile)
        raise ProviderContentTierNotFoundError(errMsg)
    #    Query for all the Content Tier Destinations for this Content Tier
    for contentTierDestinationMapping in query.all():
        #    For each Content Tier destination get the destination
        destination = SESSION_JUKEBOX.query(Destination).filter(Destination.destID == contentTierDestinationMapping.destID).one()
        log.info("Analyzing Destination: %s" % destination)
        #    Find out if this destination for this ADI.XML has already been processed
        if SESSION_JUKEBOX.query(CompletedDestination).filter(CompletedDestination.destID == destination.destID).filter(CompletedDestination.pkgID == adiXML.getUniqueIdAsString()).count():
            log.info("Destination ID: %s was previously successfully processed for this ADI.XML  Delivery Method: %s" % (destination.destID, destination.deliverySetting.deliveryMethod))
            log.info("Packaging this Asset for the specified Destination ID will be skipped")  
            continue
        if destination.deliverySetting == None:
            errMsg = """
            Error: No Delivery Setting has been set for destination
            Delivery Setting: %s
            Destination: %s
            Package Name: %s
            Source ADI.XML: %s
            """ % (destination.deliverySetting, destination, adiXML.getUniqueIdAsString(), adiXML.xmlFile)
            raise DeliverySettingNotFoundError(errMsg)
        log.info("Delivery Method: %s" % destination.deliverySetting.deliveryMethod)
        if destination.deliverySetting.deliveryMethod == "OPAL":
            log.info("Creating Opal Pkg - Incoming PkgName: %s DestID: %s" % (adiXML.getUniqueIdAsString(), destination))
            adiXML = ADI_XML(adiXmlFile)
            adiXML.parseXML()
            processAdiOnlyDestinations(adiXML, destination, contentTier)
        if destination.deliverySetting.deliveryMethod == "ADIOnlyTar":
            log.info("Creating ADIOnlyTar Pkg - Incoming PkgName: %s DestID: %s" % (adiXML, destination))
            adiXML = ADI_XML(adiXmlFile)
            adiXML.parseXML()
            processAdiOnlyDestinations(adiXML, destination, contentTier)
        #    Determine if at least one destination for this ADI.XML requires a tar file to be created
        if destination.deliverySetting.deliveryMethod == "TAR" and createStandardTar == False:
            log.info("Setting createStandardTar flag to True for this ADI.XML")
            createStandardTar = True
            
    if createStandardTar:
        log.info("Creating Standard Tar Pkg - Incoming PkgName: %s " % (adiXML,))
        #    Get list of all the MSOs for this particular ADI.XML
        adiXML = ADI_XML(adiXmlFile)
        adiXML.parseXML()             
        processTarDestinations(adiXML, contentTier)
    else:
        log.info("This ADI.XML has no destinations that require a Standard Tar File. Standard Tar creation will be skipped")
            
    log.info("Deleting original package folder: %s" % os.path.dirname(adiXmlFile))
    shutil.rmtree(os.path.dirname(adiXmlFile), ignore_errors=True)
    
def executeProcessing():
    if COMPUTER_NAME in CMC_CATCHER_DOWNLOAD_MACHINES:
        log.info("Starting Download from CMC Catcher...")
        download_packages_from_cmc_catcher()
        log.info("Finished Download from CMC Catcher...")
    log.info("Putting packages in queue")
    put_packages_into_queue()
    while not PACKAGE_Q.empty():
        if COMPUTER_NAME in CMC_CATCHER_DOWNLOAD_MACHINES:
            log.info("Starting Download from CMC Catcher...")
            download_packages_from_cmc_catcher()
            log.info("Finished Download from CMC Catcher...")
        package = PACKAGE_Q.get()
        if not os.path.exists(package):
            log.warning("XML File no longer exists.")
            log.warning("May have been processed by another engine")
            continue
        package_helper = PackageHelper(package)
        log.info("Processing Package: %s" % package_helper.get_package_name())
        log.info("ADI File: %s" % package)
        log.info("Finding Job Status in status table...")
        query = SESSION_JUKEBOX.query(JukeBoxJobStatus).filter(JukeBoxJobStatus.path.like(package)).\
                                                             filter(or_(JukeBoxJobStatus.statusCode == 2, 
                                                                        JukeBoxJobStatus.statusCode == 3, 
                                                                        JukeBoxJobStatus.statusCode == 4))
        if query.count() > 1:
            log.warning("Multiple rows found in status table...")
            ojs = None
            for tmp in query:
                if not ojs:
                    log.info("Assigning Job Status to: {}".format(tmp))
                    ojs = tmp
                else:
                    log.warning("Deleting Entry: {}".format(tmp))
                    SESSION_JUKEBOX.delete(tmp)
                    SESSION_JUKEBOX.commit()
        else:
            ojs = query.one()
            
        if (ojs.statusCode == 3) and (ojs.engine != COMPUTER_NAME):
            log.info("Computer: %s is working on file: %s" % (ojs.engine, ojs.path))
            log.info("This file will be skipped by this engine")
            continue
        if (ojs.statusCode == 3) and (ojs.engine == COMPUTER_NAME):
            log.warning("JukeBox previously crashed while processing file: %s" % ojs.path)
        ojs.engine = COMPUTER_NAME
        ojs.statusCode = 3
        ojs.statusDetail = "N\A"
        ojs.modifiedDate = datetime.now()
        SESSION_JUKEBOX.commit()
        try:
            log.info("Making backup copy of package")
            copy_package_to_backup(package)
            log.info("Checking if package is valid and ready to process")
            if isPkgReady(package):
                log.info("Creating JukeBox packages for: %s" % package_helper.get_package_name())
                create_packages(package)
                log.info("Created JukeBox package for: %s" % package_helper.get_package_name())
                SESSION_JUKEBOX.refresh(ojs)
                ojs.statusCode = 1
                ojs.engine = None
                ojs.modifiedDate = datetime.now()
                ojs.statusDetail = "Completed Successfully on Engine: %s" % COMPUTER_NAME
                SESSION_JUKEBOX.commit()
                log.info("Finished JukeBox processing for %s" % package_helper.get_package_name())
            else:
                log.warning("Package is not ready and will be skipped")
        except (GeneralProcessingError, InvalidPackage) as err:
            errString = "%s" % unicode(err)
            log.error("A General Processing Error occurred while processing: %s" % package)
            log.error("Error Type: %s" % type(err))
            log.error("Error Message: %s" % errString)
            SESSION_JUKEBOX.refresh(ojs)
            ojs.statusCode = 4
            ojs.engine = None
            ojs.modifiedDate = datetime.now()
            ojs.statusDetail = "Error Message: %s" % errString
            SESSION_JUKEBOX.commit()
            log.info("Updated JukeBox Job Status Table")
            msg = """
            A general processing error occurred in JukeBox.
            JukeBox MetaFile: %s
            Error Message: %s
            """ % (os.path.basename(package), errString)
            subject = "JukeBox -- General Processing Error" 
            log.info("Sending email")
            sendSimpleEmail(EMAIL_ADDR, subject, msg)
        #    Want to move on if a problem with a file. Putting files into queue keeps updating
        #    queue with failures. Commenting out these lines so that failed assets are skipped
        #log.info("Putting packages in queue")
        #put_packages_into_queue() 
