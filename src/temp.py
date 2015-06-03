'''
Created on Nov 9, 2012

@author: mraposa
'''
from sqlalchemy import and_, Date, Time, cast, DateTime
import datetime

from GeneralDbUtilities import connectToDatabase

from models import Destination, ContentTierDestinationMapping, DeliverySetting, CategoryMapping, MsoMapping, ContentTierMapping, ProductMapping
from models import ProviderMapping, CatcherMapping, CompletedDestination
import csv

JUKEBOX_DB = 'mssql+pyodbc://JukeBox'
SESSION_JUKEBOX = connectToDatabase(JUKEBOX_DB)

def download_categories(destID=None):
    rows = []
    if destID:
        query = SESSION_JUKEBOX.query(CategoryMapping).filter(CategoryMapping.destID == destID)
    else:
        query = SESSION_JUKEBOX.query(CategoryMapping)
    for category_mapping in query:
        row = []
        row.append(category_mapping.destID)
        row.append(category_mapping.sourceCategory)
        row.append(category_mapping.destCategory)
        rows.append(row)
        print row
    with open(r"c:\delete\categories.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        
def cmc_packaged_asset_report(start_date, end_date, output_file):
    import csv
    import tempfile
    import os
    from Xml2x import ADI_XML, checkDtdFiles
    from StringUtils import convert_datetime_to_excel_datetime
    from lxml.etree import XMLSyntaxError
    rows = []
    row = ["Package Date", "Asset Name", "Version Major", "Version Minor", "Asset ID", 
           "Tar Size (bytes)", "LWSD", "LWED", "Provider", "Run Time", "Content Tier"]
    rows.append(row)
    tmpfile = os.path.join(tempfile.gettempdir(), "tmpadi.xml")
    checkDtdFiles(tempfile.gettempdir(), adiDTD=True, changeDTD=False)
    errors = 0
    for cd in SESSION_JUKEBOX.query(CompletedDestination).filter(cast(CompletedDestination.startTime, DateTime) > start_date).filter(cast(CompletedDestination.startTime, DateTime) < end_date).order_by(CompletedDestination.startTime).all():
        with open(tmpfile, 'wb') as f:
            f.write(cd.adi_xml)
        try:
            adi_xml = ADI_XML(tmpfile, parseOnInitialization=True)
        except XMLSyntaxError:
            errors += 1
        ams = adi_xml.getAmsData()
        row = [convert_datetime_to_excel_datetime(cd.startTime), 
               cd.assetName,
               cd.versionMajor, 
               cd.versionMinor,  
               ams['Asset_ID'],
               cd.pkgSize,
               ams['Licensing_Window_Start'],
               ams['Licensing_Window_End'],
               ams['Provider'],
               adi_xml.getAttributeValue("/ADI/Asset/Metadata/App_Data[@Name='Run_Time']/@Value"),
               cd.contentTier]
        rows.append(row)
    print("ADIs with Syntax Errors: {}".format(errors))
    with open(output_file, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
        
        
if __name__ == '__main__':
    bad_char = chr(160)
    cmc_packaged_asset_report(start_date=datetime.date(2014, 4, 1), 
                          end_date=datetime.date(2014, 6, 30), 
                          output_file=r'c:\delete\jukebox_package_report_twc.csv')
    #download_categories()