
library(RMySQL)
library(dplyr)
#Set working dir

wd <- '/home/agnaldo/Git/ccs-mpi-etl'
setwd(wd)
source('db-connection.R')
source('helper_functions.R')
source('sql_queries.R')


# clear log file
log_file <- 'log_file.txt'
close( file( log_file, open="w" ) )
curr_date <- Sys.Date()
curr_datetime <-Sys.time()

# Connect to Master Patient Index

con_mpi <- getDbConnection(openmrs.user = mpi_user,openmrs.password = mpi_password,openmrs.db.name = mpi_db.name,
                           openmrs.host = mpi_host,openmrs.port = mpi_port)

# Not Run
# location <- getMasterPatientIndexData(con.openmrs = con_mpi,query = "select * from location;")
# save(location,file = 'location.RData')

if(class(con_mpi)[1]=="MySQLConnection"){
  
  if(dbIsValid(con_mpi)){
    
    load(file = 'location.RData')
    
    for ( k in 1:dim(location) ) {
      
      location_uuid <- location$uuid[k]
      location_id   <- location$location_id[k]
      db_name       <- location$db_name[k]
      patients      <- getMasterPatientIndexData(con.openmrs = con_mpi,query = paste0("select patientid,uuid ,location_uuid from patient where location_uuid='",location_uuid,"' ;") )
      
      con_openmrs <- getDbConnection(openmrs.user = openmrs_user,openmrs.password = openmrs_password,openmrs.db.name = db_name,
                                     openmrs.host = openmrs_host,openmrs.port = openmrs_port )
      
      if(class(con_openmrs)[1]=="MySQLConnection"){
        if(dbIsValid(con_openmrs)){
          
          if(nrow(patients)>0){
            
            saveProcessLog(mpi.con = con_mpi,process.date = curr_datetime,process.type = 'Fetch Drug info',affected.rows = 0,
                           process.status ='Iniated',error.msg = '' ,table = paste0(db_name,'.obs'),location.uuid = location_uuid)
            before <-  Sys.time()
            
            for (i in 496:nrow(patients)) {
              
              sql_openmrs_patient_pickups <- createSqlQueryGetOpenMRSDrugPickups(param.patientid = patients$patientid[i],param.location =location_id )
              sql_mpi_patient_pickups     <- createSqlQueryGetMPIDrugPickups(param.patientid = patients$patientid[i],param.location =location_id )
              df_mpi_patient_drugs        <- getMasterPatientIndexData(con.openmrs = con_mpi ,query = sql_mpi_patient_pickups )
              df_openmrs_patient_drugs    <- getOpenmrsData(con.openmrs = con_openmrs ,query = sql_openmrs_patient_pickups )
          
                if(nrow(df_openmrs_patient_drugs) >0 ) {
                  df_openmrs_patient_drugs$location_uuid <- location_uuid
                  df_openmrs_patient_drugs$patient_uuid <- patients$uuid[i]
                  
                
                df_update_drug <-  left_join(x = df_openmrs_patient_drugs,y = df_mpi_patient_drugs, by="uuid" ) %>%   
                  select ("pickup_date.x","next_scheduled.x","uuid","location_uuid.x","patient_uuid.x") %>% 
                  rename(pickup_date=pickup_date.x,next_scheduled=next_scheduled.x , location_uuid=location_uuid.x,patient_uuid=patient_uuid.x)
                if(nrow(df_update_drug)> 0){
                  
                  
                  UpdateDrugPickups( df = df_update_drug,table.name = "drug_pickup",con.sql = con_mpi)
                  
                  
                }
                
                
                
              }
            
              
              
              
            } 
            
            after <- Sys.time()
            elapsed_time <- after -before
            
          }
          
          
          
        }
      }

      
    }
  }
  
  
  }