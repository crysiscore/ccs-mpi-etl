
library(RMySQL)
library(dplyr)
#Set working dir

wd <- '/home/agnaldo/Git/ccs-mpi-etl'
setwd(wd)
source('config/db-connection.R')
source('helper_functions.R')
source('sql_queries.R')


# clear log file
log_file <- 'log_file_viral_load.txt'
close( file( log_file, open="w" ) )
curr_date <- Sys.Date()
curr_datetime <-Sys.time()

# Connect to Master Patient Index

con_mpi <- getDbConnection(openmrs.user = mpi_user,openmrs.password = mpi_password,openmrs.db.name = mpi_db.name,
                           openmrs.host = mpi_host,openmrs.port = mpi_port)

# Not Run
# location <- getMasterPatientIndexData(con.openmrs = con_mpi,query = "select * from location;")
# save(location,file = 'data/location.RData')

if(class(con_mpi)[1]=="MySQLConnection"){
  
  if(dbIsValid(con_mpi)){
    
    load(file = 'data/location.RData')
    
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
            
            
            before <-  Sys.time()
            
            for (i in 1:nrow(patients)) {
              
              sql_openmrs_patient_carga_viral  <- createSqlQueryGetOpenMRSViraLoad(param.patientid = patients$patientid[i],param.location =location_id )
              sql_mpi_patient_carga_viral      <- createSqlQueryGetMPIDViraLoad(param.patientid = patients$patientid[i],param.location =location_id )
              df_mpi_patient_carga_viral       <- getMasterPatientIndexData(con.openmrs = con_mpi ,query = sql_mpi_patient_carga_viral )
              df_openmrs_patient_carga_viral   <- getOpenmrsData(con.openmrs = con_openmrs ,query = sql_openmrs_patient_carga_viral )
          
                if(nrow(df_openmrs_patient_carga_viral) >0 ) {
                  df_openmrs_patient_carga_viral$location_uuid <- location_uuid
                  df_openmrs_patient_carga_viral$patient_uuid <- patients$uuid[i]
                  df_openmrs_patient_carga_viral[is.na(df_openmrs_patient_carga_viral)] <- "2000/01/01"
                
                df_update_viral_load <-  left_join(x = df_openmrs_patient_carga_viral,y = df_mpi_patient_carga_viral, by="uuid" ) %>%   
                  select("uuid","viral_load_value.x","viral_load_type.x","data_cv.x","origem_result.x","location_uuid.x","patient_uuid.x") %>% 
                  rename(viral_load_value=viral_load_value.x,viral_load_type=viral_load_type.x , origem_result=origem_result.x,data_cv=data_cv.x,
                         location_uuid=location_uuid.x,patient_uuid=patient_uuid.x)
                if(nrow(df_update_viral_load)> 0){
                  
                  
                  UpdateMpiData(df = df_update_viral_load,table.name = "viral_load",con.sql = con_mpi)
                  
                  
                }
                
                
                
              }
            
              
              
              
            } 
            
            # If process finished sucessfully
            if(i==nrow(patients)){
              after <- Sys.time()
              elapsed_time <- after -before
              saveProcessLog(mpi.con = con_mpi,process.date = curr_datetime,process.type = 'Fetch Viral load info',affected.rows = 0,
                             process.status ='Iniated',error.msg = '' ,table = paste0(db_name,'.obs'),location.uuid = location_uuid,elapsed.time=as.character(elapsed_time))
              
              writeLog(file = log_file,msg =paste0("-------------------------------------------------------------------------------------------"))
              writeLog(file = log_file,msg =paste0("-- Fetch Drug info for DB: ", db_name, " took ", elapsed_time))
              writeLog(file = log_file,msg =paste0("-------------------------------------------------------------------------------------------"))
              print(paste0("-------------------------------------------------------------------------------------------"))
              print(paste0("-- Fetch Drug info for DB: ",db_name, " took ", elapsed_time))
              print(paste0("-------------------------------------------------------------------------------------------"))
              }

          }
          
          
          
        }
      }

      
    }
  }
  
  
  }
