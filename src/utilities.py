'''
Created on Dec 14, 2011

@author: mraposa
'''

import logging
import os
import tempfile
from datetime import datetime

from lxml import etree
from sqlalchemy import or_

from Xml2x import ADI_XML, InvalidAdiXmlError, checkDtdFiles
from FileHelper2x import readFileToString, get_file_list, sortFilesByModifiedTime, touch, copyFileToDirectory, get_checksum
from OperationsModel2x import Package
from OperationsHelper import PackageHelper, InvalidPackage
from NetworkUtils import where_am_I
from StringUtils import get_random_alphanumeric

from global_variables import LOG_NAME, SESSION_OPERATIONS, SESSION_JUKEBOX, JUKEBOX_PACKAGE_DIR, COMPUTER_NAME, PACKAGE_Q
from global_variables import JUKEBOX_BACKUP_DIR
from exception_classes import ProductNotFoundError, CategoryNotFoundError, ProviderContentTierNotFoundError, ProviderNotFoundError
from models import MsoMapping, Destination, ProductMapping, CategoryMapping, ProviderMapping, JukeBoxJobStatus, CompletedDestination

log = logging.getLogger("%s.%s" % (LOG_NAME,__name__))

def copy_package_to_backup(adiXmlFile):
    '''
    Make backup of tar file before processing
    @param adiXmlFile:
    '''
    adiXML = ADI_XML(adiXmlFile)
    dstDir = os.path.join(JUKEBOX_BACKUP_DIR, adiXML.getUniqueIdAsString())
    if not os.path.exists(dstDir):
        os.makedirs(dstDir)
    for f in get_file_list(os.path.dirname(adiXmlFile)):
        #    This step is done so that ADI.DTD are not immediately deleted when backup folders are pruned.
        log.info("Copying %s to %s" % (f, dstDir))
        copyFileToDirectory(f, dstDir)
        log.info("Setting modified date to now on backup file")
        touch(os.path.join(dstDir, os.path.basename(f)))

def save_completed_destination(adiXML, changeXML, destination, mso_mappings, file_list, contentTier, startTime, pkgSize):
    '''
    Save data to the completed destinations table
    @param adiXML:
    @param destination:
    @param contentTier:
    @param startTime:
    @param pkgSize:
    '''
    if changeXML:
        log.info("Saving Completed Destination for a TAR file")
        for mso_mapping in mso_mappings:
            destID = mso_mapping.destID
            mso = mso_mapping.mso.lower()
            log.info("Working on Destination ID: %s" % destID)
            log.info("MSO: %s" % mso)
            cd = CompletedDestination()
            cd.destID = destID
            tmp_file = os.path.join(tempfile.gettempdir(), "%s.xml" % get_random_alphanumeric(5))
            changeXML.createADI_XML(outputAdiFile=tmp_file, msoID=mso, addComment=False)
            checkDtdFiles(tempfile.gettempdir(), adiDTD=True, changeDTD=True)
            adiXML = ADI_XML(tmp_file)
            cd.pkgID = adiXML.getUniqueIdAsString()
            cd.startTime = startTime
            cd.endTime = datetime.now()
            cd.pkgSize = pkgSize
            cd.file_list = file_list
            ams_data = adiXML.getAmsData()
            cd.assetName = ams_data["Asset_Name"]
            cd.versionMajor = ams_data["Version_Major"]
            cd.versionMinor = ams_data["Version_Minor"]
            cd.contentTier = contentTier
            cd.adi_xml = readFileToString(tmp_file)
            SESSION_JUKEBOX.add(cd)
            SESSION_JUKEBOX.commit()
            log.info("Saved Completed Destination to table")
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
    else:
        log.info("Saving Completed Destination for an asset without a Change.XML file")
        cd = CompletedDestination()
        cd.destID = destination.destID
        cd.pkgID = adiXML.getUniqueIdAsString()
        cd.startTime = startTime
        cd.endTime = datetime.now()
        cd.pkgSize = pkgSize
        #    Strip out full path from file file_list and just store file names
        cd.file_list = ",".join([os.path.basename(f) for f in file_list.split(",")])
        ams_data = adiXML.getAmsData()
        cd.assetName = ams_data["Asset_Name"]
        cd.versionMajor = ams_data["Version_Major"]
        cd.versionMinor = ams_data["Version_Minor"]
        cd.contentTier = contentTier
        tmp_file = os.path.join(tempfile.gettempdir(), "%s.xml" % get_random_alphanumeric(5))
        adiXML.saveXML(outputFile=tmp_file, encoding="ISO-8859-1", addEditedBy=False)
        cd.adi_xml = readFileToString(tmp_file)
        SESSION_JUKEBOX.add(cd)
        SESSION_JUKEBOX.commit()
        log.info("Saved Completed Destination to table")
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

def isPkgReady(adiXmlFile):
    '''
    Checks if package is ready
    Checks that files aren't locked and that all content files are present
    @param adiXmlFile:
    '''
    pkg_helper = PackageHelper(adiXmlFile, logger_name="%s.%s" % (LOG_NAME,__name__))
    if not pkg_helper.are_package_files_present():
        msg = """
        Package Content Files are Missing
        At least one content file has been specified in the ADI.XML that was not
        found in the package folder
        Package: %s
        ADI.XML: %s
        """ % (pkg_helper.get_package_name(), adiXmlFile)
        raise InvalidPackage(msg)
    else:
        log.info("All package content files are present")
    if pkg_helper.are_package_files_locked():
        return False
    adi_xml = ADI_XML(adiXmlFile)
    contentTier = adi_xml.getAttributeValue("/ADI/Metadata/App_Data[@Name='Provider_Content_Tier']/@Value")
    if contentTier == None:
        msg = """
        ADI.XML is invalid
        Provider Content Tier is missing or is not in the correct place in the XML
        ADI.XML: %s
        """ % adiXmlFile
        raise InvalidPackage(msg)
    #    check_content_file_checksums will raise an exception on failure
    log.info("Calculating checksums on content files and comparing to value in XML")
    if pkg_helper.check_content_file_checksums() == 0:
        log.info("Content file checksums match values in original ADI.XML")
    return True

def put_packages_into_queue():
    '''
    Put packages into package queue
    '''
    #    First add all the package to the JukeBox Status Table
    add_jobs_to_JukeBoxJobStatus()
    #    Query the table to get all the packages that need processing
    for ojs in SESSION_JUKEBOX.query(JukeBoxJobStatus).\
        filter(or_(JukeBoxJobStatus.statusCode == 2, JukeBoxJobStatus.statusCode == 3, JukeBoxJobStatus.statusCode == 4)).all():
        
        adiXmlFile = ojs.path
        if not os.path.exists(adiXmlFile):
            #    The metaFile file may not exist if the job failed or completed successfully by another engine
            if ojs.statusCode == 5 or ojs.statusCode == 1:
                log.info("XML File was processed by another engine")
                continue
            else:
                log.warning("XML File no longer exists.")
                log.warning("Current Status code: %s" % ojs.statusCode)
                ojs.statusCode = 6
                SESSION_JUKEBOX.commit()
                log.info("Updated JukeBox Job Status Table. Set status code to '6 -- Unknown'")
                continue
        if "vcden02" in adiXmlFile.lower() and where_am_I() == "DEN":
            log.info("Adding package to queue: %s" % PackageHelper(adiXmlFile).get_package_name())
            PACKAGE_Q.put(adiXmlFile)
        elif "vc67" in adiXmlFile.lower() and where_am_I() == "NYC":
            log.info("Adding package to queue: %s" % PackageHelper(adiXmlFile).get_package_name())
            PACKAGE_Q.put(adiXmlFile)

def add_jobs_to_JukeBoxJobStatus():
    '''
    Add jobs to JukeBox Job Status table. Table is used by Engines to get a list of jobs to be processed
    '''
    log.info("Getting file list")
    #    Get list of .meta files. 
    #    Process oldest files first
    metaFileList = get_file_list(JUKEBOX_PACKAGE_DIR, recursive=True, patterns="*.xml")
    metaFileList = sortFilesByModifiedTime(metaFileList)   
    
    for f in metaFileList:
        log.info("Processing file: %s" % f)
        if not os.path.exists(f):
            log.warning("XML no longer exists. Skipping XML")
            log.warning("XML: %s" % f)
            continue
        if os.path.getsize(f) == 0:
            log.warning("XML is 0KB in size. XML is Blank")
            log.warning("XML: %s" % f)
            tmp = "%s.File_Is_Blank.BAD" % os.path.basename(f)
            log.warning("Renaming XML to %s" % tmp)
            os.rename(f, os.path.join(os.path.dirname(f), tmp))
            continue
        try:
            log.info("Attempting to Parse: %s" % f)
            adi_xml = ADI_XML(f)
        except InvalidAdiXmlError:
            log.warn("Unable to parse ODOL XML.")
            log.warning("XML: %s" % f)
            tmp = "%s.Parse_Error.BAD" % os.path.basename(f)
            log.warning("Renaming XML to %s" % tmp)
            os.rename(f, os.path.join(os.path.dirname(f), tmp))
            continue
        #    Skip files already in table and being worked on, e.g. Waiting, Processing, Error
        if SESSION_JUKEBOX.query(JukeBoxJobStatus).filter(JukeBoxJobStatus.path == f).\
            filter(or_(JukeBoxJobStatus.statusCode == 2, JukeBoxJobStatus.statusCode == 3, JukeBoxJobStatus.statusCode == 4)).all():
            log.info("Job already in Job Status table")  
            continue
            
        ojs = JukeBoxJobStatus()
        ojs.path = f
        #    Set statusCode to "Waiting"
        ojs.statusCode = 2
        ojs.modifiedDate = datetime.now()
        ojs.provider = adi_xml.getAmsData()["Provider"]
        ojs.pkgID = adi_xml.getUniqueIdAsString()
        log.info("Adding Job: %s" % ojs)
        SESSION_JUKEBOX.add(ojs)
        SESSION_JUKEBOX.commit()

def saveToPackagesTable(adiXmlFile, changeXmlFile=None, tarFileSize=None):
    adiXML = ADI_XML(adiXmlFile)
    amsData = adiXML.getAmsData()
    assetName = amsData["Asset_Name"]
    assetID = amsData["Asset_ID"]
    versionMajor = amsData["Version_Major"]
    versionMinor = amsData["Version_Minor"]
    package = ""
    log.info("Querying to see if package is already in Packages table")
    if SESSION_OPERATIONS.query(Package).filter(Package.assetName.like(assetName)).filter(Package.assetID.like(assetID)).filter(Package.versionMajor.like(versionMajor)).filter(Package.versionMinor.like(versionMinor)).count():
        log.info("Package already found in packages table")
        package = SESSION_OPERATIONS.query(Package).filter(Package.assetName.like(assetName)).filter(Package.assetID.like(assetID)).filter(Package.versionMajor.like(versionMajor)).filter(Package.versionMinor.like(versionMinor)).first()
        if package.changeXML == None and changeXmlFile:
            log.info("Updating change.xml information in packages table")
            package.changeXML = readFileToString(changeXmlFile)
            SESSION_OPERATIONS.commit()
        if package.tarFileSize == None and tarFileSize:
            log.info("Updating tar file size  information in packages table")
            package.tarFileSize = tarFileSize
            SESSION_OPERATIONS.commit()
    else:
        log.info("Package not in packages table. Saving package to packages table")
        processingID = ""
        if where_am_I() == "NYC":
            processingID = 'JukeBox-v1.0'
        if where_am_I() == "DEN":
            processingID = 'NYC_Repackaging_v1.0'
        package = Package().createPackage(adiFile=adiXmlFile, processingID=processingID, changeFile=changeXmlFile, tarFileSize=tarFileSize)
        # truncate lic start date or end date that has hours in value
        # RJ OKed this - 11.6.2013
        for idx, licdate in enumerate([package.licStartDate, package.licEndDate]):
            if 'T' in licdate:
                new_date = licdate.split('T')[0]
                if idx == 0:
                    package.licStartDate = new_date
                else:
                    package.licEndDate = new_date
        SESSION_OPERATIONS.add(package)
        SESSION_OPERATIONS.commit()
    
    return package.pkID
    
def convertProducts(adiElement, adiXML, mso=None, destination=None):
    '''
    Convert source product into destination product
    @param adiElement:    ADI Element from either ADI.XML or MSO section of CHANGE.XML
    @param adiXML:
    @param mso:
    '''
    ams_data = adiXML.getAmsData()
    sourceProduct = ams_data["Product"]
    sourceProvider = ams_data["Provider"]
    
    #    If processing a TAR file, then destination will be None
    #    Need to get a destination by translating MSO into a destination
    if destination == None:
        if mso.lower() == "iso":
            destID = "ISO"
        else:
            #    Query the MSO Mapping Table for the destID
            destID = SESSION_JUKEBOX.query(MsoMapping).filter(MsoMapping.mso.like(mso)).first().destID
        #    Query to get the destination object
        destination = SESSION_JUKEBOX.query(Destination).filter(Destination.destID == destID).one()
    #    Query to get the source Product from the ProductMapping table
    query = SESSION_JUKEBOX.query(ProductMapping).\
            filter(ProductMapping.destID == destination.destID).\
            filter(ProductMapping.sourceProduct == sourceProduct).\
            filter(ProductMapping.sourceProvider == sourceProvider)
             
    if query.count() >= 1:
        productMapping = query.one()
        log.info("Incoming Product: %s  New Product: %s" % (sourceProduct, productMapping.destProduct))
        log.info("Editing Product attributes with new values")
        #    Change the /ADI/Metadata/AMS/@Product
        amsElement = adiElement.find(".//Metadata//AMS")
        amsElement.set("Product", productMapping.destProduct)
        #    Change the /ADI/Asset/Metadata/AMS/@Product
        amsElement = adiElement.find(".//Asset//Metadata//AMS")
        amsElement.set("Product", productMapping.destProduct)
        #    Change the /ADI/Asset/<<Asset_List>>/Metadata/AMS/@Product
        for assetElement in adiElement.findall(".//Asset//Asset"):
            amsElement = assetElement.find(".//Metadata//AMS")
            amsElement.set("Product", productMapping.destProduct)
    else:
        errMsg = """
        Error: The Source Product was not found in the Product Mapping Table
        Incoming Product: %s
        Incoming Provider: %s
        Destination ID: %s 
        Package Name: %s
        Source ADI.XML: %s
        MSO: %s
        """ % (sourceProduct, sourceProvider, destination.destID, adiXML.getUniqueIdAsString(), adiXML.xmlFile, mso)
        raise ProductNotFoundError(errMsg)
    
def convertProviders(adiElement, adiXML, mso=None, destination=None):
    '''
    Convert source Provider into destination Provider
    @param adiElement:    ADI Element from either ADI.XML or MSO section of CHANGE.XML
    @param adiXML:
    @param mso:
    '''
    sourceProvider = adiXML.getAttributeValue("/ADI/Metadata/AMS/@Provider")
    
    #    If processing a TAR file, then destination will be None
    #    Need to get a destination by translating MSO into a destination
    if destination == None:
        if mso.lower() == "iso":
            destID = "ISO"
        else:
            #    Query the MSO Mapping Table for the destID
            destID = SESSION_JUKEBOX.query(MsoMapping).filter(MsoMapping.mso.like(mso)).first().destID
        #    Query to get the destination object
        destination = SESSION_JUKEBOX.query(Destination).filter(Destination.destID == destID).one()
    #    Query to get the source Provider from the ProviderMapping table
    query = SESSION_JUKEBOX.query(ProviderMapping).filter(ProviderMapping.destID == destination.destID).filter(ProviderMapping.sourceProvider == sourceProvider) 
    if query.count() >= 1:
        providerMapping = query.one()
        log.info("Incoming Provider: %s  New Provider: %s" % (sourceProvider, providerMapping.destProvider))
        log.info("Editing Provider attributes with new values")
        #    Change the /ADI/Metadata/AMS/@Provider
        amsElement = adiElement.find(".//Metadata//AMS")
        amsElement.set("Provider", providerMapping.destProvider)
        #    Change the /ADI/Asset/Metadata/AMS/@Provider
        amsElement = adiElement.find(".//Asset//Metadata//AMS")
        amsElement.set("Provider", providerMapping.destProvider)
        #    Change the /ADI/Asset/<<Asset_List>>/Metadata/AMS/@Provider
        for assetElement in adiElement.findall(".//Asset//Asset"):
            amsElement = assetElement.find(".//Metadata//AMS")
            amsElement.set("Provider", providerMapping.destProvider)
    else:
        errMsg = """
        Error: The Source Provider was not found in the Provider Mapping Table
        Source Provider: %s
        Destination ID: %s 
        Package Name: %s
        Source ADI.XML: %s
        MSO: %s
        """ % (sourceProvider, destination.destID, adiXML.getUniqueIdAsString(), adiXML.xmlFile, mso)
        raise ProviderNotFoundError(errMsg)

def convertCategories(element, adiXML, mso=None, destination=None):
    '''
    Convert source categories into destination categories
    @param element:    Category App_Data Element
    @param adiXML:
    @param mso:
    '''
    srcCategory = element.get("Value")
    #    If processing a TAR file, then destination will be None
    #    Need to get a destination by translating MSO into a destination
    if destination == None:
        if mso.lower() == "iso":
            destID = "ISO"
        else:
            destID = SESSION_JUKEBOX.query(MsoMapping).filter(MsoMapping.mso.like(mso)).first().destID
        #    Query to get the destination object
        destination = SESSION_JUKEBOX.query(Destination).filter(Destination.destID == destID).one()
    #    Query to get the source Category from the CategoryMapping table
    query = SESSION_JUKEBOX.query(CategoryMapping).filter(CategoryMapping.destID == destination.destID).filter(CategoryMapping.sourceCategory == srcCategory) 
    if query.count() >= 1:
        if query.count() > 1:
            log.info("This srcCategory: %s maps to multiple destination categories. New Category elements will be created" % srcCategory)
        loopCount = 1
        for categoryMapping in query.all():
            log.info("Incoming Category: %s  New Category: %s" % (srcCategory, categoryMapping.destCategory))
            if loopCount == 1:
                #    Overwrite the existing source category
                log.info("Overwriting existing category element with destination category: %s" % categoryMapping.destCategory)
                element.set("Value", categoryMapping.destCategory)
            else:
                #    Create a new element for each subsequent category
                log.info("Creating new category element with destination category: %s" % categoryMapping.destCategory)
                newCategoryElement = etree.SubElement(element.getparent(), "App_Data")
                newCategoryElement.set("App", element.get("App"))
                newCategoryElement.set("Name", "Category")
                newCategoryElement.set("Value", categoryMapping.destCategory)
            loopCount += 1
    else:
        errMsg = """
        Error: The Source Category was not found in the Category Mapping Table
        Source Category: %s
        Destination ID: %s 
        Package Name: %s
        Source ADI.XML: %s
        MSO: %s
        """ % (srcCategory, destination.destID, adiXML.getUniqueIdAsString(), adiXML.xmlFile, mso)
        raise CategoryNotFoundError(errMsg)
     

def convertProviderContentTier(adiXML, destination):
    '''
    A new provider content tier is set on the outgoing ADI.XML
    This is only done for ADI.XML only assets
    @param adiXML:
    @param destination:
    '''
    if not destination.contentTierMapping == None:
        log.info("Mapping Provider Content Tier")
        providerContentTierElement = adiXML.xmlTree.xpath("/ADI/Metadata/App_Data[@Name='Provider_Content_Tier']")[0]
        log.info("Incoming Tier: %s  New Tier: %s" % (providerContentTierElement.get("Value"), destination.contentTierMapping.destContentTier))
        providerContentTierElement.set("Value", destination.contentTierMapping.destContentTier)
    else:
        errMsg = """
        Error: The Outgoing Provider Content Tier was not found in the Provider Content Tier Mapping Table
        Destination ID: %s
        Package Name: %s
        Source ADI.XML: %s
        """ % (destination.destID, adiXML.getUniqueIdAsString(), adiXML.xmlFile)
        raise ProviderContentTierNotFoundError(errMsg)

def get_file_list_string_for_completed_destinations(file_list):
    '''
    Returns a formated list of files used to save to Completed Destinations model and the file_list
    attribute. This data is used to query Opal by the FOD Dashboard
    
    @param file_list:
    @return String in form file_name:checksum,file_name:checksum,etc.
    '''
    file_data = []
    for f in file_list:
        log.info("Calculating checksum for {}".format(os.path.basename(f)))
        file_data.append("{}:{}".format(os.path.basename(f), get_checksum(f)))
    return ",".join(file_data)
