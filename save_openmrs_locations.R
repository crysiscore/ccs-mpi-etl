library(RMySQL)

#Set working dir
wd <- '/home/agnaldo/Git/ccs-mpi-etl'
setwd(wd)
source('config/db-connection.R')
source('helper_functions.R')
source('sql_queries.R')

# clear log file
log_file <- 'logs/location_log_file.txt'
close( file( log_file, open="w" ) )


# Busca o nome de todas BD openmrs no servidor de backups

vec_db_names <- readLines('data/unidades_sanitarias.txt')


# Connect to Master Patient Index

con_mpi <- getDbConnection(openmrs.user = mpi_user,openmrs.password = mpi_password,openmrs.db.name = mpi_db.name,
                           openmrs.host = mpi_host,openmrs.port = mpi_port)

if(class(con_mpi)[1]=="MySQLConnection"){
  
  if(dbIsValid(con_mpi)){
    
    # Try connection to openmrs DBs
    
    for (db in vec_db_names) {
      
      con_openmrs <- getDbConnection(openmrs.user = openmrs_user,openmrs.password = openmrs_password,openmrs.db.name = db,
                                     openmrs.host = openmrs_host,openmrs.port = openmrs_port)
      
      if(class(con_openmrs)[1]=="MySQLConnection"){
        if(dbIsValid(con_openmrs)){
          
          #Get Location info
          tryCatch({
            
            location <- getOpenmrsDefaultLocation(openmrs.con = con_openmrs,db.name = db)
            
            if(nrow(location)==1){
              location_name <- location$name[1]
              location_description <- location$description[1]
              Encoding(location_name) <- "latin1"
              Encoding(location_description) <- "latin1"
              location_name <- iconv(location_name, "latin1", "UTF-8",sub='')
              location_description <- iconv(location_description, "latin1", "UTF-8",sub='')
              
              status <- saveOpenmrsLocation(mpi.con = con_mpi,location.id = location$location_id[1],uuid = location$uuid[1],name = location_name,
                                  description =location_description,db.name = db )
              if(status==1){
                writeLog(file = log_file,msg = paste0(Sys.time()," ", db ," - Saving location info ...."))
                writeLog(file = log_file,msg = paste0(Sys.time()," ", db ," - Sucessfully saved !") )
                
              } else {print(paste0(Sys.time()," ", db ," -Error  Saving location info ...."))}
            } else {
              
              writeLog(file = log_file,msg = paste0(Sys.time(), " ", db ," - Unable to get location info from table Location, aborting ...."))
              print(paste0(Sys.time()," ", db ," Unable to get location info from table Location, aborting ...."))
            }
            
          },
          error = function(cond) {
            error_msg <- paste0( Sys.time(), "  MySQL - Nao foi possivel obter os dados das Localion: ",  '  db:',db, "...")
            print(paste0(Sys.time()," ", db ," Unable to get location info from table Location, aborting ...."))
            writeLog(file = log_file,msg = error_msg)
            writeLog(file = log_file,msg = as.character(cond))
            # Choose a return value in case of error
            return(FALSE)
          },
          finally = {
            # NOTE:
            # Here goes everything that should be executed at the end,
            # regardless of success or error.
            # If you want more than one expression to be executed, then you
            # need to wrap them in curly brackets ({...}); otherwise you could
            # just have written 'finally=<expression>'
            
          })
          

          dbDisconnect(con_openmrs)
          rm(con_openmrs)
          
        }

      }
      
      
    }
    

    dbDisconnect(con_mpi)
    rm(con_mpi)
    
  }
  
  
} else {
  
  # Connection failed. Log has already been written
  # Do nothing
  
}
