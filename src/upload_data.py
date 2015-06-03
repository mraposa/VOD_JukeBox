'''
Created on Nov 8, 2011

@author: mraposa
'''
from GeneralDbUtilities import connectToDatabase

from models import Destination, ContentTierDestinationMapping, DeliverySetting, CategoryMapping, MsoMapping, ContentTierMapping, ProductMapping
from models import ProviderMapping, CatcherMapping
import csv

JUKEBOX_DB = 'mssql+pyodbc://JukeBox'
SESSION_JUKEBOX = connectToDatabase(JUKEBOX_DB)

def upload_destID_catcher_mapping(csv_file):
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            destID = row[0].strip()
            catcher = row[1].strip()
            print destID, catcher
            cm = CatcherMapping()
            cm.destID = destID
            cm.catcher_name = catcher
            SESSION_JUKEBOX.add(cm)
            SESSION_JUKEBOX.commit()
    

def upload_old_CMC_category_data(category_csv, destID):
    with open(category_csv, 'r') as f:
        reader = csv.reader(f)
        src_max = 6
        row_count = 0
        for row in reader:
            if row_count == 0:
                row_count += 1
                continue
            src_category = ""
            dst_category = ""
            src_count = 0
            for column in row:
                if src_count < src_max and column:
                    src_category = "%s/%s" % (src_category, column.strip())
                else:
                    if column:
                        dst_category = "%s/%s" % (dst_category, column.strip())
                src_count += 1
            d = CategoryMapping()
            d.destID = destID
            d.sourceCategory = src_category.strip("/")
            d.destCategory = dst_category.strip("/")
            print src_category.strip("/"), dst_category.strip("/")
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def upload_old_CMC_product_data(product_csv, destID):
    with open(product_csv, 'r') as f:
        reader = csv.reader(f)
        row_count = 0
        for row in reader:
            if row_count == 0:
                row_count += 1
                continue
            d = ProductMapping()
            d.destID = destID
            d.sourceProvider = row[0].strip()
            d.sourceProduct = row[1].strip()
            d.destProduct = row[2].strip()
            print ("%s\t%s\t%s" % (row[0].strip(), row[1].strip(), row[2].strip()))
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadDestination(f):
    with open(f, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = Destination()
            d.destID = row[0].strip()
            if not row[1] == "":
                d.description = row[1].strip()
            if not row[2] == "":
                d.contactName = row[2].strip()
            if not row[3] == "":
                d.contactEmail = row[3].strip()
            if not row[4] == "":
                d.contactPhone = row[4].strip()
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadCategoryMapping(f, session):
    with open(f, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = CategoryMapping()
            d.destID = row[0].strip()
            d.sourceCategory = row[1].strip()
            d.destCategory = row[2].strip()
            print d.destID, d.sourceCategory, d.destCategory
            session.add(d)
            session.commit()

def uploadProductMapping(f):
    with open(f, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = ProductMapping()
            d.destID = row[0].strip()
            d.sourceProduct = row[1].strip()
            d.destProduct = row[2].strip()
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadProviderMapping(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = ProviderMapping()
            d.destID = row[0].strip()
            d.sourceProvider = row[1].strip()
            d.destProvider = row[2].strip()
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadContentTierDestMapping(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = ContentTierDestinationMapping()
            d.contentTier = row[0].strip()
            d.destID = row[1].strip()
            
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadContentTierMapping(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = ContentTierMapping()
            d.destID = row[0].strip()
            d.destContentTier = row[1].strip()
            
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadDeliverySetting(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = DeliverySetting()
            d.destID = row[0].strip()
            d.deliveryMethod = row[1].strip()
            if not row[2] == "":
                d.opalFolder = row[2].strip()            
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()

def uploadMsoMapping(file):
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) == 0:
                continue
            d = MsoMapping()
            d.destID = row[0].strip()
            d.mso = row[1].strip()          
            SESSION_JUKEBOX.add(d)
            SESSION_JUKEBOX.commit()
            
def upload_dest_opal_uids(f, session):
    with open(f, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            uids = []
            if len(row) == 0:
                continue
            destID = row[0].strip()
            for x in xrange(1,6):
                if row[x].strip():
                    uids.append(row[x].strip())
            print destID, ",".join(uids)
            ds = session.query(DeliverySetting).filter(DeliverySetting.destID == destID).one()
            ds.opal_uids = ",".join(uids)
            session.commit()
            

def testRelationships():
    d = SESSION_JUKEBOX.query(Destination).first()
    cm = SESSION_JUKEBOX.query(CategoryMapping).first()
    for catMap in  d.categoryMappings:
        print catMap
    print cm.destination
    pass

if __name__ == '__main__':
    #TODO: next 4 for JB2. Last 2 functions are run only once 
    #upload_old_CMC_category_data(category_csv = r"X:\PrePROD\Phase_1\GENERAL_PROCESSING\NYC_Repackaging\CategoryMapping_TWC.csv", destID = "TWC")
    #upload_old_CMC_product_data(r"X:\PrePROD\Phase_1\GENERAL_PROCESSING\NYC_Repackaging\ProductMapping_TWC.csv", destID = "TWC")
    #uploadProviderMapping(r"C:\delete\musicchoice\CMC_provider_mapping.csv")
    #uploadContentTierDestMapping(r"X:\PrePROD\tech\mraposa_workspace\ContentTierDestination.csv")
    
    #uploadDestination(r"c:\delete\musicchoice\destination.csv")
    #uploadCategoryMapping(r"c:\delete\musicchoice\category.csv")
    #uploadProductMapping(r"c:\delete\musicchoice\ProductMapping.csv")
    #uploadProviderMapping(r"c:\delete\musicchoice\ProviderMapping.csv")
    #uploadMsoMapping(r"c:\delete\musicchoice\MsoMapping.csv")
    #uploadContentTierMapping(r"c:\delete\musicchoice\ContentTierMapping.csv")
    #uploadDeliverySetting(r"c:\delete\musicchoice\DeliverySetting.csv")
    
    #    Convert opalFolder to all uppercase
    uploadCategoryMapping("c:\delete\categories.csv", connectToDatabase('mssql+pyodbc://JukeBox'))
    #upload_dest_opal_uids(r"c:\delete\OPAL_UIDs_Dashboard.csv", connectToDatabase('mssql+pyodbc://JukeBox'))
    #upload_destID_catcher_mapping(r"c:\delete\catcher_codes.csv")
    """
    for deliverySetting in SESSION_JUKEBOX.query(DeliverySetting).all():
        if not deliverySetting.opalFolder == None:
            deliverySetting.opalFolder = deliverySetting.opalFolder.upper()
    SESSION_JUKEBOX.commit()
    testRelationships()
    """
    """
    destination = Destination()
    destination.destID = "MJR_TAR_TWC"
    destination.description = "MJR TAR TWC Destination"
    SESSION_JUKEBOX.add(destination)
    SESSION_JUKEBOX.commit()
    """
    """
    dm = DeliveryMethod()
    dm.deliveryMethod = "ADIOnlyTar"
    SESSION_JUKEBOX.add(dm)
    SESSION_JUKEBOX.commit()
    dm = DeliveryMethod()
    dm.deliveryMethod = "TAR"
    SESSION_JUKEBOX.add(dm)
    SESSION_JUKEBOX.commit()
    dm = DeliveryMethod()
    dm.deliveryMethod = "OPAL"
    SESSION_JUKEBOX.add(dm)
    SESSION_JUKEBOX.commit()
    """
    """
    ctdm = ContentTierDestinationMapping()
    ctdm.destID = "MJR_TAR_TWC"
    ctdm.contentTier = "MusicChoice_45_35"
    SESSION_JUKEBOX.add(ctdm)
    SESSION_JUKEBOX.commit()
    ds = DeliverySetting()
    ds.destID = "MJR_TAR_TWC"
    ds.deliveryMethod = "TAR"
    SESSION_JUKEBOX.add(ds)
    cm = CategoryMapping()
    cm.destID = "MJR_TAR_TWC"
    cm.sourceCategory = "Music Choice/Rock/All Artists"
    cm.destCategory = "Music Choice/Rock/All Artists/MJR Category TimeWarner"
    SESSION_JUKEBOX.add(cm)
    SESSION_JUKEBOX.commit()
    cm = CategoryMapping()
    cm.destID = "MJR_TAR_TWC"
    cm.sourceCategory = "Music Choice/Rock/Greatest Hits"
    cm.destCategory = "Music Choice/Rock/Greatest Hits/MJR Category TimeWarner"
    SESSION_JUKEBOX.add(cm)
    msoMap = MsoMapping()
    msoMap.destID = "MJR_TAR_TWC"
    msoMap.mso = "TWC"
    SESSION_JUKEBOX.add(msoMap)
    ctm = ContentTierMapping()
    ctm.destID = "MJR_OPAL"
    ctm.destContentTier = "MJR_TEST_Content_Tier"
    SESSION_JUKEBOX.add(ctm)
    """
    pass