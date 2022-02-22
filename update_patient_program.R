
library(RMySQL)
library(dplyr)

#Set working dir
source('config/parameters.R')
source('config/db-connection.R')
source('helper_functions.R')
source('sql_queries.R')


# clear log file
log_file <- 'logs/log_file_patient_program.txt'
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
 
      con_openmrs <- getDbConnection(openmrs.user = openmrs_user,openmrs.password = openmrs_password,openmrs.db.name = db_name,
                                     openmrs.host = openmrs_host,openmrs.port = openmrs_port )
      
      if(class(con_openmrs)[1]=="MySQLConnection"){
        if(dbIsValid(con_openmrs)){
          

            before <-  Sys.time()
            sql_openmrs_patient_program  <- createSqlQueryGetOpenMRSPatProgram(param.location =location_id )
            df_openmrs_patient_program   <- getOpenmrsData(con.openmrs = con_openmrs ,query = sql_openmrs_patient_program )
            
            if(nrow(df_openmrs_patient_program) >0 ) {
              
              # drop existing data
              dbGetQuery(conn = con_mpi,statement = paste0("delete from patient_program where location_uuid = '",location_uuid,"' ;" ))
              
              df_openmrs_patient_program$location_uuid <- location_uuid
              df_openmrs_patient_program[is.na(df_openmrs_patient_program)] <- "2000/01/01"
              UpdateMpiData(df = df_openmrs_patient_program,table.name = "patient_program",con.sql = con_mpi)
              
                # If process finished sucessfully
                
              after <- Sys.time()
              elapsed_time <- round((after -before)/60,digits = 2)
              saveProcessLog(mpi.con = con_mpi,process.date = curr_datetime,process.type = 'Fetch patient program info',affected.rows = 0,
                               process.status ='Finished',error.msg = '' ,table = paste0(db_name,'.obs'),location.uuid = location_uuid,elapsed.time=as.character(elapsed_time))
                
              writeLog(file = log_file,msg =paste0("-------------------------------------------------------------------------------------------"))
              writeLog(file = log_file,msg =paste0("-- Fetch patient program info for DB: ", db_name, " took ", elapsed_time))
              writeLog(file = log_file,msg =paste0("-------------------------------------------------------------------------------------------"))
              print(paste0("-------------------------------------------------------------------------------------------"))
              print(paste0("-- Fetch patient program info for DB: ",db_name, " took ", elapsed_time))
              print(paste0("-------------------------------------------------------------------------------------------"))
              dbDisconnect(conn = con_openmrs)
              rm(con_openmrs)
                
              

            } 
            
          }
          
          
          
        }
      }

      
    }
    
    #  close the connection when finished
    dbDisconnect(con_mpi)
    rm(con_mpi)
  }
