from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Text, Integer, UniqueConstraint, ForeignKey, DateTime, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mssql import NVARCHAR

Base = declarative_base()

class JukeBoxStatusCode(Base):
    '''
    Table of JukeBox status codes
    '''
    __tablename__ = 'jukeboxStatusCode'
    statusCode = Column(Integer, primary_key=True)
    description = Column(Text)
   
class JukeBoxJobStatus(Base):
    '''
    Status of a particular JukeBox job
    '''
    __tablename__ = 'jukeboxJobStatus'
    path = Column(String(256))
    engine = Column(String(256), nullable=True)
    statusCode = Column(Integer, ForeignKey("jukeboxStatusCode.statusCode"))
    statusDetail = Column(String(2048), nullable=True) #    Used to store Error information 
    modifiedDate = Column(DateTime)
    provider = Column(String(256), nullable=True)  #    Incoming provider for incoming package
    pkgID = Column(String(256), nullable=True)     #    Usually just the Package Unique Identifier for the incoming package
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'<JukeBoxJobStatus: %s>' % (self.path)

class Destination(Base):
    '''
    Stores all the JukeBox Destinations. A destination is as its name implies
    a generic name for JukeBox output. It could be a individual site or an entire MSO
    '''
    __tablename__ = 'Destination'
    destID = Column(String(256), primary_key=True)
    description = Column(Text(), nullable=True)
    contactName = Column(String(256), nullable=True)
    contactEmail = Column(String(256), nullable=True)
    #output_directory = Column(String(1024), nullable=True)
    contactPhone = Column(String(128), nullable=True)
    deliverySetting = relationship("DeliverySetting", uselist=False, backref="destination")
    #    This is the new provider value set in the ADI.XML
    contentTierMapping = relationship("ContentTierMapping", uselist=False, backref="destination")
    
    def __repr__(self):
        return u'%s' % (self.destID,)

class ContentTierDestinationMapping(Base):
    '''
    Maps Provider_Content_Tier in the ADI.XML into a list of destinations.
    This table tells JukeBox where a package should go
    '''
    __tablename__ = 'ContentTierDestinationMapping'
    destID = Column(String(256), ForeignKey("Destination.destID"))
    contentTier = Column(String(512))
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'%s     %s' % (self.destID, self.contentTier)
        
class CategoryMapping(Base):
    '''
    Maps a source category to a destination category for a specific destination.
    Each destination gets its own mapping
    '''
    __tablename__ = 'CategoryMapping'
    destID = Column(String(256), ForeignKey("Destination.destID"))
    sourceCategory = Column(String(1024))
    destCategory = Column(String(1024))
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'%s     %s     %s' % (self.destID, self.sourceCategory, self.destCategory)
    
class ProductMapping(Base):
    '''
    Maps a source product to a destination product for a specific destination.
    Each destination gets its own mapping
    '''
    __tablename__ = 'ProductMapping'
    __table_args__ = (
            UniqueConstraint("destID", "sourceProduct", "sourceProvider"),
            )
    destID = Column(String(256), ForeignKey("Destination.destID"), nullable=False)
    sourceProduct = Column(String(1024), nullable=False)
    sourceProvider = Column(String(1024), nullable=False)
    destProduct = Column(String(1024), nullable=False)
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'%s     %s     %s' % (self.destID, self.sourceProduct, self.destProduct)
    
class ProviderMapping(Base):
    '''
    Maps a source Provider to a destination Provider for a specific destination.
    Each destination gets its own mapping
    '''
    __tablename__ = 'ProviderMapping'
    __table_args__ = (
            UniqueConstraint("destID", "sourceProvider"),
            )
    destID = Column(String(256), ForeignKey("Destination.destID"), nullable=False)
    sourceProvider = Column(String(1024), nullable=False)
    destProvider = Column(String(1024), nullable=False)
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'%s     %s     %s' % (self.destID, self.sourceProvider, self.destProvider)
             
class ContentTierMapping(Base):
    '''
    Maps Provider_Content_Tier on the outgoing newly created ADI.XML. This table
    tell JukeBox what the new Provider_Content_Tier should be based on the destination 
    '''
    __tablename__ = 'ContentTierMapping'
    destID = Column(String(256), ForeignKey("Destination.destID"), primary_key=True)
    destContentTier = Column(String(512))
    
    def __repr__(self):
        return u'%s     %s' % (self.destID, self.destContentTier)

class DeliveryMethod(Base):
    '''
    A simple table that maps delivery methods. Currently, that is just three rows,
    TAR, OPAL. ADIOnlyTar
    '''
    __tablename__ = 'DeliveryMethod'
    deliveryMethod = Column(String(128), primary_key=True)
    
    def __repr__(self):
        return u'%s' % (self.deliveryMethod)

class DeliverySetting(Base):
    '''
    Delivery Setting for each destination. Tell Jukebox how a package should be delivered\created
    for each destination
    '''
    __tablename__ = 'DeliverySetting'
    destID = Column(String(256), ForeignKey("Destination.destID"), primary_key=True)
    deliveryMethod = Column(String(128), ForeignKey("DeliveryMethod.deliveryMethod"))
    opalFolder = Column(String(512), nullable=True)        #    Opal delivery folder, e.g. OPAL_PENCOR
    #    Comma separated list of Opal UIDs
    opal_uids = Column(String(1024), nullable=True)
    
    def __repr__(self):
        return u'%s     %s' % (self.destID, self.deliveryMethod)
        
class MsoMapping(Base):
    '''
    When a destination is part of a larger MSO, this table maps a destinationID into a
    specific MSO. This table is used to aggregate destinations where appropriate into a
    single large MSO in order to create the change.xml
    '''
    __tablename__ = 'MsoMapping'
    destID = Column(String(256), ForeignKey("Destination.destID"), primary_key=True)
    mso = Column(String(128))
    
    def __repr__(self):
        return u'%s     %s' % (self.destID, self.mso)

class CatcherMapping(Base):
    '''
    Links DestIDs to MediaPath catchers
    '''
    __tablename__ = 'CatcherMapping'
    destID = Column(String(256), ForeignKey("Destination.destID"), nullable=False)
    catcher_name = Column(String(128), nullable=False)
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'%s     %s' % (self.destID, self.catcher_name)
    
class CompletedDestination(Base):
    '''
    Keeps track of destinations that have been processed for a given package. Prevents
    JukeBox from repeat processing of packages that have already been processed for a 
    destination
    '''
    __tablename__ = 'CompletedDestination'
    
    destID = Column(String(256), ForeignKey("Destination.destID"), nullable=True)
    pkgID = Column(String(256), nullable=True)     #    Usually just the Package Unique Identifier
    startTime = Column(DateTime(), nullable=True)
    endTime = Column(DateTime(), nullable=True)
    pkgSize = Column(BigInteger(), nullable=True)
    assetName = Column(String(255), nullable=True)
    versionMajor = Column(String(127), nullable=True)
    versionMinor = Column(String(127), nullable=True)
    contentTier = Column(String(255), nullable=True)
    #    Stores ADI.XML data. For a change.xml stores just the appropriate MSO section, i.e. MSO specific ADI
    adi_xml = Column(Text(), nullable=True)
    #    Stores a comma separated list of files and checksums associated with this destination
    #    file_name:checksum,file_name:checksum, etc.
    file_list = Column(String(1024), nullable=True)
    pkID = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return u'%s     %s' % (self.destID, self.pkgID)

def deployForProduction(DB):
    from sqlalchemy import create_engine
    engine = create_engine(DB, echo=True)
    Base.metadata.create_all(engine)
    
if __name__ == '__main__':
    from sqlalchemy import create_engine
    from global_variables import JUKEBOX_DB
    engine = create_engine(JUKEBOX_DB, echo=True)
    Base.metadata.create_all(engine)



