
library(RMySQL)

#Set working dir
wd <- '/home/agnaldo/Git/ccs-mpi-etl'
setwd(wd)
source('db-connection.R')
source('helper_functions.R')
source('sql_queries.R')


# clear log file
log_file <- 'patient_log_file.txt'
close( file( log_file, open="w" ) )


# Busca o nome de todas BD openmrs no servidor de backups

vec_db_names <- readLines('unidades_sanitarias.txt')


# Connect to Master Patient Index

con_mpi <- getDbConnection(openmrs.user = mpi_user,openmrs.password = mpi_password,openmrs.db.name = mpi_db.name,
                           openmrs.host = mpi_host,openmrs.port = mpi_port)

if(class(con_mpi)[1]=="MySQLConnection"){
  
  if(dbIsValid(con_mpi)){

    load(file = 'temp_patients.RData')
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
            sql_query_mpi_patients  <- createSqlQueryPatientMpi(param.location = location_uuid)
            patients_openmrs    <- getOpenmrsData(con.openmrs = con_openmrs,query = sql_query_openmrs_patients)
            patients_mpi        <- getMasterPatientIndexData(con.openmrs = con_mpi, query = sql_query_mpi_patients)
            
            
        
            if(nrow(patients) > 0){
              patients$NomeCompleto    <- removeSpecialCharacters(patients$NomeCompleto)
              patients$given_name      <- removeSpecialCharacters(patients$given_name)
              patients$middle_name     <- removeSpecialCharacters(patients$middle_name)
              patients$family_name     <- removeSpecialCharacters(patients$family_name)
              patients$Bairro          <- removeSpecialCharacters(patients$Bairro)
              patients$PontoReferencia <- removeSpecialCharacters(patients$PontoReferencia)
              patients$telefone        <- removeSpecialCharacters(patients$telefone)
              patients$Padministrativo <- removeSpecialCharacters(patients$Padministrativo)
              patients$Distrito        <- removeSpecialCharacters(patients$Distrito)
              patients$Localidade      <- removeSpecialCharacters(patients$Localidade)
              # patients$birthdate     <- as.Date(temp_patients$birthdate, "%d/%m/%Y")
              # patients$death_date    <- as.Date(temp_patients$death_date, "%d/%m/%Y")
              # patients$data_inicio   <- as.Date(temp_patients$data_inicio, "%d/%m/%Y")
              patients$location <- location_uuid
              assign(paste0("patients_", db), patients)
              temp_patients <- plyr::rbind.fill(temp_patients,get(paste0("patients_", db)))
              rm(patients)
              
            } else {
              # Error getting patient data
              writeLog(file = log_file,msg = paste0(Sys.time()," ",db, "- Error  getting patient data, skipping ...."))
              saveProcessLog(mpi.con = con_mpi,process.date = curr_datetime,process.type = 'Load Patient Data',affected.rows = 0,
                             process.status ='Failed',error.msg = 'Unknown' ,table = 'patient',location.uuid = location_uuid)
            }
 
            
          } else {
            
            writeLog(file = log_file,msg = paste0(Sys.time()," ", db," - Unable to get location info from table Location, aborting ...."))
            saveErrorLogLog(mpi.con = con_mpi,process.date = curr_datetime,process.type = 'get location info',affected.rows = 0,
                           process.status ='Failed',error.msg = 'Unable to get location info from table Location, aborting' ,table = 'location',location.uuid = db)
            
          }
          dbDisconnect(con_openmrs)
          rm(con_openmrs)
   
          
          }
 
       }
      
      
    }
    #  dbWriteTable(conn = con_mpi, name = 'temp_patients', value = temp_patients , row.names = F, append = F)
    #  dbGetQuery(mydb, "insert into table select * from temp_table")
    
  }
  } else{
  
  # Connection failed. Log has already been written
  # Do nothing
  
  
}
