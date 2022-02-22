library(RMySQL)

source('config/parameters.R')
source('config/db-connection.R')
source('helper_functions.R')
source('sql_queries.R')


# clear log file
log_file <- 'logs/patient_log_file.txt'
close( file( log_file, open="w" ) )


# Busca o nome de todas BD openmrs no servidor de backups
vec_db_names <- readLines('data/unidades_sanitarias.txt')


# Connect to Master Patient Index
con_mpi <- getDbConnection(openmrs.user = mpi_user,openmrs.password = mpi_password,openmrs.db.name = mpi_db.name,
                           openmrs.host = mpi_host,openmrs.port = mpi_port)

if(class(con_mpi)[1]=="MySQLConnection"){
  
  if(dbIsValid(con_mpi)){

    load(file = 'data/temp_patients.RData')
    # Try connection to openmrs DBs

    for (db in vec_db_names) {
      
      con_openmrs <- getDbConnection( openmrs.user = openmrs_user,openmrs.password = openmrs_password,openmrs.db.name = db,
                                 openmrs.host = openmrs_host, openmrs.port = openmrs_port )
     
       if(class(con_openmrs)[1]=="MySQLConnection"){
        if(dbIsValid(con_openmrs)){
          
          #Get Location info
          location <- getOpenmrsDefaultLocation(openmrs.con = con_openmrs,db.name = db)
          curr_date <- Sys.Date()
          curr_datetime <-Sys.time()
          if(nrow(location)==1){
            location_id   <- location$location_id[1]
            location_uuid <- location$uuid[1]
            # Get Patient data
            sql_query_openmrs_patients  <- createSqlQueryPatient(param.location = location_id,param.end.date = curr_date)
            #TODO - remove later
            #sql_query_mpi_patients  <- createSqlQueryPatientMpi(param.location = location_uuid)
            patients_openmrs    <- getOpenmrsData(con.openmrs = con_openmrs,query = sql_query_openmrs_patients)
            #TODO - remove later
            #patients_mpi        <- getMasterPatientIndexData(con.openmrs = con_mpi, query = sql_query_mpi_patients)
            
            
        
            if(nrow(patients_openmrs) > 0){
              patients_openmrs$NomeCompleto    <- removeSpecialCharacters(patients_openmrs$NomeCompleto)
              patients_openmrs$given_name      <- removeSpecialCharacters(patients_openmrs$given_name)
              patients_openmrs$middle_name     <- removeSpecialCharacters(patients_openmrs$middle_name)
              patients_openmrs$family_name     <- removeSpecialCharacters(patients_openmrs$family_name)
              patients_openmrs$Bairro          <- removeSpecialCharacters(patients_openmrs$Bairro)
              patients_openmrs$PontoReferencia <- removeSpecialCharacters(patients_openmrs$PontoReferencia)
              patients_openmrs$telefone        <- removeSpecialCharacters(patients_openmrs$telefone)
              patients_openmrs$Padministrativo <- removeSpecialCharacters(patients_openmrs$Padministrativo)
              patients_openmrs$Distrito        <- removeSpecialCharacters(patients_openmrs$Distrito)
              patients_openmrs$Localidade      <- removeSpecialCharacters(patients_openmrs$Localidade)
              # patients_openmrs$birthdate     <- as.Date(temp_patients_openmrs$birthdate, "%d/%m/%Y")
              # patients_openmrs$death_date    <- as.Date(temp_patients_openmrs$death_date, "%d/%m/%Y")
              # patients_openmrs$data_inicio   <- as.Date(temp_patients_openmrs$data_inicio, "%d/%m/%Y")
              patients_openmrs$location <- location_uuid
              assign(paste0("patients_", db), patients_openmrs)
              temp_patients <- plyr::rbind.fill(temp_patients,get(paste0("patients_", db)))
              dbGetQuery(conn = con_mpi, "drop table if exists temp_patients; ")
              dbSendQuery(conn = con_mpi,statement = "SET GLOBAL local_infile=1;")
              status = dbWriteTable(conn = con_mpi, name = 'temp_patients', value = temp_patients , row.names = F, append = F)
              if(status){
                dbGetQuery(conn = con_mpi,statement = paste0("delete from patient where location_uuid = '",location_uuid, "' ;"))
                dbGetQuery(conn = con_mpi,statement = sql_post_insert_patient)
                temp_patients <- temp_patients[0,]
              }
              
              rm(patients_openmrs)
              
            }
            else {
              # Error getting patient data
              writeLog(file = log_file,msg = paste0(Sys.time()," ",db, "- Error  getting patient data, skipping ...."))
              saveErrorLog(mpi.con = con_mpi,process.date = curr_datetime,process.type = 'Load Patient Data',affected.rows = 0,
                             process.status ='Failed',error.msg = 'Unknown' ,table = 'patient',location.uuid = location_uuid)
              print(paste0(Sys.time()," ",db, "- Error  getting patient data, skipping ...."))
            }
 
            
          }
          else {
            
            writeLog(file = log_file,msg = paste0(Sys.time()," ", db," - Unable to get location info from table Location, aborting ...."))
            saveErrorLog( mpi.con = con_mpi,process.date = curr_datetime,process.type = 'get location info',affected.rows = 0,
                           process.status ='Failed', error.msg = 'Unable to get location info from table Location, aborting' ,
                           table = 'location',location.uuid = db )
            print(paste0(Sys.time()," ", db," - Unable to get location info from table Location, aborting ...."))
            
          }
          
          dbDisconnect(con_openmrs)
          rm(con_openmrs)
   
          
          }
 
       }
      
      
      
    }
    # if(nrow(temp_patients)> 0){
    #   
    #   #dbSendQuery(conn = con_mpi,statement = "ALTER TABLE patient ADD PRIMARY KEY (patientid, uuid); ")
    #   dbGetQuery(conn = con_mpi, "drop table if exists temp_patients; ")
    #   dbSendQuery(conn = con_mpi,statement = "SET GLOBAL local_infile=1;")
    #   status = dbWriteTable(conn = con_mpi, name = 'temp_patients', value = temp_patients , row.names = F, append = F)
    #   if(status){
    #    
    #      dbGetQuery(conn = con_mpi,statement = sql_post_insert_patient)
    #   }
    #   
    #   
    # }

    dbDisconnect(con_mpi)
    rm(con_mpi) 
  }
  } else{
  
  # Connection failed. Log has already been written
  # Do nothing
  
  
  }


