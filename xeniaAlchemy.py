"""

"""
import logging
from datetime import datetime

from sqlalchemy import MetaData, create_engine, exc, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .XeniaTables import (
    m_scalar_type,
    m_type,
    multi_obs,
    obs_type,
    organization,
    platform,
    platform_status,
    platform_type,
    sensor,
    sensor_status,
    uom_type,
)

logger = logging.getLogger(__name__
                           )
class xeniaAlchemy(object):
    def __init__(self):
        self.dbEngine = None
        self.metadata = None
        self.session = None
        self.connection = None
        self.logger = logger

    def connect_db(self, connection_string, printSQL = False):

      try:
          # Connect to the database
          self.dbEngine = create_engine(connection_string, echo=printSQL)

          # metadata object is used to keep information such as datatypes for our table's columns.
          self.metadata = MetaData()
          self.metadata.bind = self.dbEngine

          Session = sessionmaker(bind=self.dbEngine)
          self.session = Session()

          self.connection = self.dbEngine.connect()

          return True
      except exc.OperationalError as e:
          self.logger.exception(e)
      return False


    def disconnect(self):
        self.session.close()
        self.connection.close()
        self.dbEngine.dispose()

    def build_minimal_platform(self, platform_name, observation_list):
        name_parts = platform_name.split('.')
        org_id = self.organizationExists(name_parts[0])
        row_entry_date = datetime.now()
        if org_id is None:
            self.logger.debug("Adding organization name: %s" % (name_parts[0]))
            org_id = self.addOrganization(row_entry_date, name_parts[0])

        if self.platformExists(platform_name) is None:
            self.logger.debug("Adding platform handle: %s" % (platform_name))
            plat_rec = platform(row_entry_date=row_entry_date,
                                organization_id=org_id,
                                platform_handle=platform_name,
                                short_name=name_parts[1],
                                active=1)
            self.addRec(plat_rec, True)
        for obs_info in observation_list:

            self.logger.debug(
                "Platform: %s adding sensor: %s(%s)" % (platform_name, obs_info['obs_name'], obs_info['uom_name']))
            try:
                if self.addNewSensor(obs_info['obs_name'], obs_info['uom_name'],
                                     platform_name,
                                     1,
                                     0,
                                     obs_info['s_order'],
                                     None,
                                     True) is None:
                    self.logger.error("Error platform: %s sensor: %s(%s) not added" % (
                        platform_name, obs_info['obs_name'], obs_info['uom_name']))
            except Exception as e:
                self.logger.exception(e)

    """
    Function: platformExists  
    """


    def platformExists(self, platformHandle):
        try:
            platRec = self.session.query(platform.row_id) \
                .filter(platform.platform_handle == platformHandle) \
                .one()
            return platRec.row_id
        except NoResultFound as e:
            self.logger.debug(e)
        except exc.InvalidRequestError as e:
            self.logger.exception(e)
        return None


    """
    Function: addPlatform
    Purpose: Adds a new platform into the platform table.
    Parameters: 
      platformInfo is a dictionary keyed on the column names of the table. The only required key/values are:
        organization_id is the associated organization id.
        platform_handle is the handle for the platform.
      Optional columns are:
        type_id         
        short_name      
        fixed_longitude 
        fixed_latitude  
        active          
        begin_date      
        end_date        
        project_id      
        app_catalog_id  
        long_name       
        description     
        url             
        metadata_id     
    Returns:
      The row_id if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def newPlatform(self, rowEntryDate, platformHandle, fixedLongitude, fixedLatitude, active=1, url="", description=""):
        platformRec = None
        platformHandleParts = platformHandle.split('.')
        # Check to make sure the organization exists:
        orgId = self.organizationExists(platformHandleParts[0])
        if orgId is None:
            self.logger.debug("Organization: %s does not exist. Adding." % (platformHandleParts[0]))
            orgId = self.addOrganization(rowEntryDate, platformHandleParts[0])
            if orgId is None:
                self.logger.error("Could not add organization, cannot continue adding platform.")
                return None
        """    
        #Get platform type id.
        platTypeId = self.platformTypeExists(platformHandleParts[2])
        if(platTypeId is None):
          if(self.logger):
            self.logger.error("Platform type: %s does not exist." % (platformHandleParts[2]))
        """
        try:
            platformRec = platform(row_entry_date=rowEntryDate,
                                   organization_id=orgId,
                                   short_name=platformHandleParts[1],
                                   platform_handle=platformHandle,
                                   fixed_longitude=fixedLongitude,
                                   fixed_latitude=fixedLatitude,
                                   active=active,
                                   url=url,
                                   description=description)

            self.addRec(platformRec, True)
            self.logger.debug("Platform: %s(%d) added to database." % (platformRec.platform_handle, platformRec.row_id))
        except Exception as e:
            self.logger.exception(e)

        return platformRec.row_id


    """
    Function: addOrganization
    """


    def addOrganization(self, rowEntryDate, organizationName, active=1, longName="", description="", url=""):
        orgRec = organization(row_entry_date=rowEntryDate,
                              short_name=organizationName,
                              active=active,
                              long_name=longName,
                              description=description,
                              url=url)
        rowId = self.addRec(orgRec, True)
        return rowId


    """
    Function: organizationExists
    """


    def organizationExists(self, organizationName):
        try:
            orgRec = self.session.query(organization.row_id) \
                .filter(organization.short_name == organizationName) \
                .one()
            return orgRec.row_id
        except NoResultFound as e:
              self.logger.debug(e)
        except exc.InvalidRequestError as e:
              self.logger.exception(e)
        return None


    """
    Function: sensorExists
    Purpose: Checks to see if the passed in obsName on the platform.
    Parameters: 
      obsName is the sensor(observation) we are testing for.
      platform is the platform on which we search for the obsName.
      sOrder, if provided specifies the specific sensor if there are multiples of the same on a platform.
    Returns:
      The sensor id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def sensorExists(self, obsName, uom, platformHandle, sOrder=1):
        try:

            rec = self.session.query(sensor.row_id) \
                .join(platform, platform.row_id == sensor.platform_id) \
                .join(m_type, m_type.row_id == sensor.m_type_id) \
                .join(m_scalar_type, m_scalar_type.row_id == m_type.m_scalar_type_id) \
                .join(obs_type, obs_type.row_id == m_scalar_type.obs_type_id) \
                .join(uom_type, uom_type.row_id == m_scalar_type.uom_type_id) \
                .filter(sensor.s_order == sOrder) \
                .filter(platform.platform_handle == platformHandle) \
                .filter(obs_type.standard_name == obsName) \
                .filter(uom_type.standard_name == uom).one()
            return rec.row_id
        except NoResultFound as e:
            self.logger.debug(e)
        except exc.InvalidRequestError as e:
            self.logger.exception(e)

        return None


    def newSensor(self, rowEntryDate, obsName, uom, platformId, active=1, fixedZ=0, sOrder=1, mTypeId=None,
                  addObsAndUOM=False):
        self.logger.debug("Adding sensor: %s(%s) sOrder: %d on platform: %d" % (obsName, uom, sOrder, platformId))
        sensorId = None
        if mTypeId is None:
            mTypeId = self.mTypeExists(obsName, uom)
            if mTypeId is None:
                # If we want to add the obs type and uom type, we have to add them to add to tables: obs_type, uom_type, m_scalar_type
                # before we can add the m_type.
                if addObsAndUOM:
                    # Does obs_type exist? If not, we attempt to add.
                    obsId = self.obsTypeExists(obsName)
                    if obsId is None:
                        # Add the obs to the obs_type table.
                        obsId = self.addObsType(obsName)
                        # Cannot continue if we were unable to add.
                        if obsId is None:
                            return None

                    # Does the uom type exist? If not, we attempt to add.
                    uomId = self.uomTypeExists(uom)
                    if uomId is None:
                        uomId = self.addUOMType(uom)
                        # Cannot continue if we were unable to add.
                        if uomId is None:
                            return None

                    # Does the scalar_id exist?
                    mScalarId = self.scalarTypeExists(obsId, uomId)
                    if mScalarId is None:
                        mScalarId = self.addScalarType(obsId, uomId)
                        if mScalarId is None:
                            return None

                    # Now we can add the m_type
                    mTypeId = self.addMType(mScalarId)
                    if mTypeId is None:
                        return None
                else:
                    self.logger.error(
                        "m_type does not exist, cannot add sensor: %s(%s) platform: %d" % (obsName, uom, platformId))
                    return None

            sensorRec = sensor(row_entry_date=rowEntryDate,
                               platform_id=platformId,
                               m_type_id=mTypeId,
                               short_name=obsName,
                               fixed_z=fixedZ,
                               active=active,
                               s_order=sOrder)
            sensorId = self.addRec(sensorRec, True)
            if sensorId is None:
                  self.logger.error("Unable to add sensor: %s(%s)." % (obsName, uom))
            else:
                  self.logger.debug(
                      "Added sensor: %s(%s) sOrder: %d on platform: %d" % (obsName, uom, sOrder, platformId))
        return sensorId


    """
    Function: mTypeExists
    Purpose: Checks to see if the passed in obsName with the given units of measurement exists in the m_type table.
    Parameters: 
      obsName is the sensor(observation) we are testing for.
    Returns:
      The m_type id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def mTypeExists(self, obsName, uom):
        try:
            rec = self.session.query(m_type.row_id) \
                .join(m_scalar_type, m_scalar_type.row_id == m_type.m_scalar_type_id) \
                .join(obs_type, obs_type.row_id == m_scalar_type.obs_type_id) \
                .join(uom_type, uom_type.row_id == m_scalar_type.uom_type_id) \
                .filter(obs_type.standard_name == obsName) \
                .filter(uom_type.standard_name == uom).one()
            return rec.row_id
        except NoResultFound:
            self.logger.debug("m_type %s(%s) does not exist." % (obsName, uom))
        except exc.InvalidRequestError as e:
            self.logger.exception(e)

        return None


    """
    Function: addMType
    Purpose: Adds a new m_type into the m_type table. This function is not
    "user friendly" since it requires knowledge of the obs type id and uom type id. Most likely you wouldn't call this directly
    but would be using the addSensor function to do it automagically.
    Parameters: 
      scalarID is the row_id of the scalar_type to add.
    Returns:
      The m_type_id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def addMType(self, scalarID, description=""):
        rowId = None
        # At the moment the row_id columns are not autoincrement, so we need to get the max value first.
        try:
            nextRowId = self.session.query(func.max(m_type.row_id)).one()[0]
            nextRowId += 1
        except Exception as e:
            if (self.logger):
                self.logger.exception(e)
        else:
            mTypeRec = m_type(row_id=nextRowId, num_types=1, m_scalar_type_id=scalarID, description=description)
            rowId = self.addRec(mTypeRec, True)
            if (rowId is None):
                if (self.logger):
                    self.logger.error("Unable to add scalarID: %d to m_type table." % (scalarID))
            else:
                if (self.logger):
                    self.logger.debug("Added scalarID: %d to m_type table." % (scalarID))
        return rowId


    """
    Function: obsTypeExists
    Purpose: Checks to see if the passed in obsName exists in the obs_type table.
    Parameters: 
      obsName is the sensor(observation) we are testing for.
    Returns:
      The obs_type(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def obsTypeExists(self, obsName):
        rowId = None
        try:
            rec = self.session.query(obs_type.row_id) \
                .filter(obs_type.standard_name == obsName) \
                .one()
            rowId = rec.row_id
        except NoResultFound:
            self.logger.debug("Observation: %s does not exist in obs_type table." % (obsName))
        except exc.InvalidRequestError as e:
            self.logger.exception(e)

        return rowId


    """
    Function: addObsType
    Purpose: Adds the given obsName into the obs_type table.
      obsName is the sensor(observation) we are adding.
    Returns:
      The obs_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def addObsType(self, obsName):
        rowId = None
        # At the moment the row_id columns are not autoincrement, so we need to get the max value first.
        try:
            nextRowId = self.session.query(func.max(obs_type.row_id)).one()[0]
            nextRowId += 1
        except Exception as e:
            if (self.logger):
                self.logger.exception(e)
        else:
            obsTypeRec = obs_type(row_id=nextRowId, standard_name=obsName)
            rowId = self.addRec(obsTypeRec, True)
            if (rowId is None):
                if (self.logger):
                    self.logger.error("Unable to add obs: %s to obs_type table." % (obsName))
            else:
                if (self.logger):
                    self.logger.debug("Added obs: %s to obs_type table." % (obsName))
        return rowId


    """
    Function: uomTypeExists
    Purpose: Checks to see if the passed in uomName exists in the uom_type table.
    Parameters: 
      uomName is the unit of measurement  we are testing for.
    Returns:
      The uom_type(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def uomTypeExists(self, uomName):
        rowId = None
        try:
            rec = self.session.query(uom_type.row_id) \
                .filter(uom_type.standard_name == uomName) \
                .one()
            rowId = rec.row_id
        except NoResultFound:
            self.logger.debug("UOM: %s does not exist in obs_type table." % (uomName))
        except exc.InvalidRequestError as e:
            self.logger.exception(e)
        return rowId


    """
    Function: addUOMType
    Purpose: Adds the given obsName into the obs_type table.
      obsName is the sensor(observation) we are adding.
    Returns:
      The uom_type(row_id) if it is successfully created, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def addUOMType(self, uomName):
        rowId = None
        # At the moment the row_id columns are not autoincrement, so we need to get the max value first.
        try:
            nextRowId = self.session.query(func.max(uom_type.row_id)).one()[0]
            nextRowId += 1
        except Exception as e:
            if (self.logger):
                self.logger.exception(e)
        else:
            uomTypeRec = uom_type(row_id=nextRowId, standard_name=uomName)
            rowId = self.addRec(uomTypeRec, True)
            if (rowId is None):
                if (self.logger):
                    self.logger.error("Unable to add uom: %s to uom_type table." % (uomName))
            else:
                if (self.logger):
                    self.logger.debug("Added uom: %s to obs_type table." % (uomName))
        return rowId


    """
    Function: existsScalarType
    Purpose: Checks to see if the passed in obsTypeID and uomTypeID exists in the scalar_type table. This function is not
    "user friendly" since it requires knowledge of the obs type id and uom type id. Most likely you wouldn't call this directly
    but would be using the addSensor function to do it automagically.
    Parameters: 
      obsTypeID is the row_id of the observation from the obs_type table to check.
      uomTypeID is the row_id of the unit of measure from the uom_type table to check.
    Returns:
      The m_scalar_type_id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def scalarTypeExists(self, obsTypeID, uomTypeID):
        rowId = None
        try:
            rec = self.session.query(m_scalar_type.row_id) \
                .filter(m_scalar_type.obs_type_id == obsTypeID) \
                .filter(m_scalar_type.uom_type_id == uomTypeID) \
                .one()
            rowId = rec.row_id
        except NoResultFound:
            self.logger.debug(
                "Scalar type for obs_type_id: %d uom_type_id: %d does not exist in m_scalar_type table." % (
                obsTypeID, uomTypeID))
        except exc.InvalidRequestError as e:
            self.logger.exception(e)
        return rowId


    """
    Function: addScalarType
    Purpose: Adds a new scalar type into the scalar_type table. This function is not
    "user friendly" since it requires knowledge of the obs type id and uom type id. Most likely you wouldn't call this directly
    but would be using the addSensor function to do it automagically.
    Parameters: 
      obsTypeID is the row_id of the observation from the obs_type table to add.
      uomTypeID is the row_id of the unit of measure from the uom_type table.
    Returns:
      The m_scalar_type_id(row_id) if it exists, -1 if it does not exists, or None if an error occured. If there was an error
      lastErrorMsg can be checked for the error message.
    """


    def addScalarType(self, obsTypeID, uomTypeID):
        rowId = None
        # At the moment the row_id columns are not autoincrement, so we need to get the max value first.
        try:
            nextRowId = self.session.query(func.max(m_scalar_type.row_id)).one()[0]
            nextRowId += 1
        except Exception as e:
            if (self.logger):
                self.logger.exception(e)
        else:
            scalarRec = m_scalar_type(row_id=nextRowId, obs_type_id=obsTypeID, uom_type_id=uomTypeID)
            rowId = self.addRec(scalarRec, True)
            if (rowId is None):
                if (self.logger):
                    self.logger.error(
                        "Unable to add m_scalar_type: obs_type_id: %d  uom_type_id: %d to m_scalar_type table." % (
                        obsTypeID, uomTypeID))
            else:
                if (self.logger):
                    self.logger.debug("Added m_scalar_type: obs_type_id: %d  uom_type_id: %d to m_scalar_type table." % (
                    obsTypeID, uomTypeID))
        return rowId


    def getCurrentPlatformStatus(self, platformHandle):
        try:
            rec = self.session.query(sensor_status.status) \
                .filter(platform_status.platform_handle == platformHandle).one()
            return rec.status
        except NoResultFound as e:
            self.logger.debug(e)
        except exc.InvalidRequestError as e:
            self.logger.exception(e)
        return None


    def getCurrentSensorStatus(self, obsName, platformHandle):
        try:
            rec = self.session.query(platform_status.status) \
                .filter(platform.platform_handle == platformHandle) \
                .filter(sensor_status.sensor_name == obsName).one()
            return rec.status
        except NoResultFound as e:
            self.logger.debug(e)
        except exc.InvalidRequestError as e:
            self.logger.exception(e)
        return None


    def platformTypeExists(self, platformType):
        try:
            platRec = self.session.query(platform_type.row_id) \
                .filter(platform_type.type_name == platformType) \
                .one()
            return platRec.row_id
        except NoResultFound as e:
            self.logger.debug(e)
        except exc.InvalidRequestError as e:
            self.logger.exception(e)
        return None


    def addPlatformType(self, typeName, description="", commit=False):
        platType = None
        try:
            platType = platform_type(typeName, description)
            self.session.add(platType)
            if (commit):
                self.session.commit()
        # Trying to add record that already exists.
        except exc.IntegrityError as e:
            self.session.rollback()
            self.logger.exception(e)
        return platType


    def addRec(self, rec, commit=False):
        try:
            self.session.add(rec)
            if (commit):
                self.session.commit()
        # Trying to add record that already exists.
        except exc.IntegrityError:
            self.session.rollback()
            self.logger.error("Record already exists.")
        return rec.row_id


    def add_or_update_record(self, rec, update_if_exists=True, commit=False):
        row_id = None
        try:
            self.session.add(rec)
            if (commit):
                self.session.commit()
            row_id = rec.row_id

        # Trying to add record that already exists.
        except exc.IntegrityError:
            self.session.rollback()
            if update_if_exists:
                self.logger.info("Record already exists, updating it.")
                try:
                    # Pull the record from the DB
                    current_rec = self.session.query(multi_obs) \
                        .where(multi_obs.m_date == rec.m_date) \
                        .where(multi_obs.platform_handle == rec.platform_handle) \
                        .one()
                    current_rec.m_value = rec.m_value
                    self.session.commit()
                    row_id = current_rec.row_id

                except Exception as e:
                    self.session.rollback()
                    self.logger.exception(e)
            else:
                self.logger.warning("Record already exists.")
        return row_id


    def addPlatform(self, platformRec, commit=False):
        return self.addRec(platformRec, commit)


    def addSensor(self, sensorRec, commit=False):
        return self.addRec(sensorRec, commit)

    '''
    def build_minimal_platform(self, platform_name, observation_list):
        name_parts = platform_name.split('.')
        org_id = self.organizationExists(name_parts[0])
        row_entry_date = datetime.now()
        if org_id is None:
            self.logger.debug("Adding organization name: %s" % (name_parts[0]))
            org_id = self.addOrganization(row_entry_date, name_parts[0])

        if self.platformExists(platform_name) is None:
            self.logger.debug("Adding platform handle: %s" % (platform_name))
            plat_rec = platform(row_entry_date=row_entry_date,
                                organization_id=org_id,
                                platform_handle=platform_name,
                                short_name=name_parts[1],
                                active=1)
            self.addRec(plat_rec, True)
        for obs_info in observation_list:
            self.logger.debug(
                "Platform: %s adding sensor: %s(%s)" % (platform_name, obs_info['obs_name'], obs_info['uom_name']))
            try:
                if self.addNewSensor(obs_info['obs_name'], obs_info['uom_name'],
                                     platform_name,
                                     1,
                                     0,
                                     obs_info['s_order'],
                                     None,
                                     True) is None:
                      self.logger.error("Error platform: %s sensor: %s(%s) not added" % (
                          platform_name, obs_info['obs_name'], obs_info['uom_name']))
            except Exception as e:
                self.logger.exception(e)
    '''

    def addNewSensor(self, obs_name, uom, platform_handle, active=1, fixed_z=0, s_order=1, m_type_id=None,
                     add_obs_and_uom=False):
        # If the sensor already exists, we're done.
        id = self.sensorExists(obs_name, uom, platform_handle, s_order)
        if id is not None:
            return id

        row_entry_date = datetime.now()
        # If the mTypeID is passed in, we already have a complete set of obs ids, uoms, scalar types.
        if m_type_id is None:
            obs_type_id = self.obsTypeExists(obs_name)
            if obs_type_id is None:
                if add_obs_and_uom:
                    obs_type_id = self.addObsType(obs_name)
                    # Error occured so return.
                    if obs_type_id is None:
                        raise Exception("Unable to add obs_type: %s" % (obs_name))
                # If we do not want to add a missing observation type, we must error out.
                else:
                    raise Exception("obs_type: %s does not exist. Must be added to obs_type table." % (obs_name))
            elif obs_type_id is None:
                raise Exception("obs_type.standard_name: %s does not exist." % (obs_name))

            # Now let's check if our UOM exists.
            uom_type_id = self.uomTypeExists(uom)
            if uom_type_id is None:
                if add_obs_and_uom:
                    uom_type_id = self.addUOMType(uom)
                    # Error occured so return.
                    if uom_type_id is None:
                        raise Exception("Unable to add uom_type: %s" % (uom))
                # If we do not want to add a missing uom type, we must error out.
                else:
                    raise Exception("uom_type: %s does not exist. Must be added to uom_type table." % (uom))
            elif uom_type_id is None:
                raise Exception("uom_type.standard_name: %s does not exist." % (uom))

            # Now check the scalar type.
            scalar_id = self.scalarTypeExists(obs_type_id, uom_type_id)
            if scalar_id is None:
                scalar_id = self.addScalarType(obs_type_id, uom_type_id)
                # Error occured so return.
                if scalar_id is None:
                    raise Exception("Unable to add scalar_type with obs_type_id: %d and uom_type_id: %d" % (
                        scalar_id, uom_type_id))
            elif scalar_id is None:
                return None

            # Now we need to add a new m_type
            m_type_id = self.mTypeExists(obs_name, uom)
            if m_type_id is None:
                m_type_id = self.addMType(scalar_id)
                # Error occured so return.
                if m_type_id is None:
                    raise Exception("Unable to add m_type with scalar_type_id: %d" % (scalar_id))
            elif m_type_id is None:
                return None

        # Now we can finally add the sensor to the sensor table.
        platform_id = self.platformExists(platform_handle)
        if platform_id is not None:
            sensor_rec = sensor(row_entry_date=row_entry_date,
                                platform_id=platform_id,
                                m_type_id=m_type_id,
                                short_name=obs_name,
                                fixed_z=fixed_z,
                                active=active,
                                s_order=s_order)
            sensor_id = self.addRec(sensor_rec, True)
            if sensor_id is not None:
                self.logger.debug("Added sensor: %s(%s) sOrder: %d on platform: %d" % (obs_name, uom, s_order, platform_id))
                return sensor_id
            else:
                raise Exception("Unable to add sensor: %s(%s)." % (obs_name, uom))


        else:
            raise Exception("Platform: %s does not exist. Cannot add sensor." % (platform_handle))
    '''
    def calcAvgWindSpeedAndDir(self, platName, wind_speed_obsname, wind_speed_uom, wind_dir_obsname, wind_dir_uom,
                               start_date, end_date):
        wind_components = []
        dir_components = []
        vect_obj = vectorMagDir()
        spd_avg = dir_avg = scalar_spd_avg = vectordir_avg = None
        # Get the wind speed and direction so we can correctly average the data.
        # Get the sensor ID for the obs we are interested in so we can use it to query the data.
        # windSpdId = xeniaSQLite.sensorExists(self, wind_speed_obsname, wind_speed_uom, platName)
        # windDirId = xeniaSQLite.sensorExists(self, wind_dir_obsname, wind_dir_uom, platName)
        m_wind_speed_id = self.sensorExists(wind_speed_obsname, wind_speed_uom, platName)
        m_wind_dir_id = self.sensorExists(wind_dir_obsname, wind_dir_uom, platName)
        if m_wind_speed_id is not None and \
                m_wind_dir_id is not None:

            try:
                wnd_spd_recs = self.session.query(multi_obs) \
                    .filter(multi_obs.sensor_id == m_wind_speed_id) \
                    .filter(multi_obs.m_date >= start_date) \
                    .filter(multi_obs.m_date < end_date) \
                    .order_by(multi_obs.m_date) \
                    .all()
                wnd_dir_recs = self.session.query(multi_obs) \
                    .filter(multi_obs.sensor_id == m_wind_dir_id) \
                    .filter(multi_obs.m_date >= start_date) \
                    .filter(multi_obs.m_date < end_date) \
                    .order_by(multi_obs.m_date) \
                    .all()
            except Exception as e:
                self.logger.exception(e)
            else:
                scalar_spd = None
                spd_cnt = 0
                for spd_row in wnd_spd_recs:
                    if scalar_spd is None:
                        scalar_spd = 0
                    scalar_spd += spd_row.m_value
                    spd_cnt += 1
                    for dir_row in wnd_dir_recs:
                        if spd_row.m_date == dir_row.m_date:
                            self.logger.debug("Calculating vector for Speed(%s): %f Dir(%s): %f" % (
                            spd_row.m_date, spd_row.m_value, dir_row.m_date, dir_row.m_value))
                            # Vector using both speed and direction.
                            wind_components.append(vect_obj.calcVector(spd_row.m_value, dir_row.m_value))
                            # VEctor with speed as constant(1), and direction.
                            dir_components.append(vect_obj.calcVector(1, dir_row.m_value))
                            break
                # Get our average on the east and north components of the wind vector.
                spd_avg = None
                dir_avg = None
                scalar_spd_avg = None
                vectordir_avg = None

                # If we have the direction only components, this is unity speed with wind direction, calc the averages.
                if len(dir_components):
                    east_comp_avg = 0
                    north_comp_avg = 0
                    scalar_spd_avg = scalar_spd / spd_cnt

                    for vectorTuple in dir_components:
                        east_comp_avg += vectorTuple[0]
                        north_comp_avg += vectorTuple[1]

                    east_comp_avg = east_comp_avg / len(dir_components)
                    north_comp_avg = north_comp_avg / len(dir_components)
                    spd_avg, vectordir_avg = vect_obj.calcMagAndDir(east_comp_avg, north_comp_avg)
                    self.logger.debug(
                        "Platform: %s Scalar Speed Avg: %f Vector Dir Avg: %f" % (platName, scalar_spd_avg, vectordir_avg))

                # 2013-11-21 DWR Add check to verify we have components. Also reset the east_comp_avg and north_comp_avg to 0
                # before doing calcs.
                # If we have speed and direction vectors, calc the averages.
                if len(wind_components):
                    east_comp_avg = 0
                    north_comp_avg = 0
                    for vectorTuple in wind_components:
                        east_comp_avg += vectorTuple[0]
                        north_comp_avg += vectorTuple[1]

                    east_comp_avg = east_comp_avg / len(wind_components)
                    north_comp_avg = north_comp_avg / len(wind_components)
                    # Calculate average with speed and direction components.
                    spd_avg, dir_avg = vect_obj.calcMagAndDir(east_comp_avg, north_comp_avg)
                    self.logger.debug("Platform: %s Vector Speed Avg: %f Vector Dir Avg: %f" % (platName, spd_avg, dir_avg))

        else:
            self.logger("Wind speed or wind direction id is not valid.")
        return (spd_avg, dir_avg), (scalar_spd_avg, vectordir_avg)
    '''

if __name__ == '__main__':
    xeniaDB = xeniaAlchemy()
