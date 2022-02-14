# Packages que contem algumas funcoes a serem usadas. Deve-se  garantir que tem todos os packages instalados.
# Para instalar deve: ligar o pc a net e  na consola digitar a instrucao -  ex: install.packages("plyr")
require(RMySQL)
require(stringr)
require(DBI)

#' writeLog - Escreve texto (append) no ficheiro de logs 
#' 
#' @param file  log file
#' @param msg  msg to be written
#' @return 
#' @examples
#' writeLog(file, msg)
#' 
writeLog <- function (file,msg){
  #write("\n",file=file,append=TRUE)
  write(msg,file=file,append=TRUE)
}


#' getDbConnection - Busca uma conexao com BD Mysql
#' 
#' @param openmrs.user  
#' @param openmrs.password  
#' @param openmrs.db.name 
#' @param openmrs.host  
#' @param openmrs.port  
#' @return conn object
#' @examples
#' getDbConnection(openmrs.user,openmrs.password,openmrs.db.name, openmrs.host,openmrs.port )
#' 
getDbConnection <- function(openmrs.user,openmrs.password,openmrs.db.name, openmrs.host,openmrs.port )
{
  
  db_conn <- tryCatch({
    
    log_msg <- paste0( Sys.time(), "  MySQL Conectando-se a host:", openmrs.host, ' , db: ',openmrs.db.name, "...")
    print(paste0( Sys.time(), "  MySQL Conectando-se a host:", openmrs.host, ' , db: ',openmrs.db.name, "...") )
    writeLog(file = log_file,msg = log_msg)
    
    # Objecto de connexao com a bd 
    con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)
    print(paste0( Sys.time(), "ok, got connection to ", openmrs.host, ' , db: ',openmrs.db.name, "...") )
    con_openmrs
    
  },
  error = function(cond) {
    error_msg <- paste0(Sys.time(), "  MySQL - Nao foi possivel connectar-se a host: ", openmrs.host, '  db:',openmrs.db.name, "...",'user:',openmrs.user, ' passwd: ', openmrs.password)
    writeLog(file = log_file,msg = error_msg)
    writeLog(file = log_file,msg = as.character(cond))
    print(as.character(cond))
    print(paste0(Sys.time(), "  MySQL - Nao foi possivel connectar-se a host: ", openmrs.host, '  db:',openmrs.db.name, "...",'user:',openmrs.db.name, ' passwd: ', openmrs.password))
    saveErrorLog(mpi.con = con_mpi,process.date = Sys.time(),process.type = 'Get Database Conection',affected.rows = 0,
                   process.status ='Failed',error.msg = as.character(cond)  ,table = openmrs.db.name,location.uuid = openmrs.db.name)
    return(FALSE)
  },
  warning = function(cond) {
    writeLog(file = log_file,msg = as.character(cond))
    print(as.character(cond))
    # Choose a return value in case of warning
    return(TRUE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  
  db_conn
  
}



#' Busca dados de uma query enviada a MySQL
#'
#' @param con.mysql obejcto de conexao com BD 
#' @return query sql query
#' @examples patients <- getOpenmrsData(con_openmrs,query)
getOpenmrsData <- function(con.openmrs, query) {
  rs  <- dbSendQuery(con.openmrs,query)
  data <- fetch(rs, n = -1)
  dbClearResult(rs)
  return(data)
  
}




#' Verifica se os ficheiros necessarios para executar as operacoes existem
#' 
#' @param files  nomes dos ficheiros
#'  @param dir  directorio onde ficam os files
#' @return TRUE/FALSE
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs)
checkFileExists <- function (files, dir){
  for(i in 1:length(files)){
    f <- files[i]
    if(!file.exists(paste0(dir,f))){
      message(paste0('Erro - Ficheiro ', f, ' nao existe em ',dir))
      return(FALSE)
    }
  }
  return(TRUE)
}


#' Busca detalhes da location  da  openmrs
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return location info
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs,db)
getOpenmrsDefaultLocation <- function (openmrs.con, db.name){
  resut_set <- dbSendQuery(openmrs.con, paste0("select location_id, name, description, uuid from ", db.name,".location where name =(select property_value from ",db.name, ".global_property where property= 'default_location') "))
  data <- fetch(resut_set,n=1)
  RMySQL::dbClearResult(resut_set)
  #detach("package:RMySQL", unload = TRUE)
  return (data)
  
}


#' Grava logs erros de cada operacao na MPI
#' 
#' @param openmrs.con objecto de conexao com mysql  
#' @param process.type tipo de operacao
#' @param record.count affected rows
#' @return  NA
#' @examples saveErrorLog(mpi.con,process.date,process.type,affected.rows,process.status,error.msg,location.uuid)
#' 
saveErrorLog <- function(mpi.con, process.date,process.type,affected.rows,process.status,error.msg,table,location.uuid){
  
  insert_query <- paste0( "insert into ccs_mpi.error_logs(process_date,process_type,record_count,process_status,error_message,table_name,location_uuid) values( ", paste0("'",as.character(process.date),"' , "),
                          paste0("'",process.type,"' , "), affected.rows," ,",   paste0("'",process.status,"' ,"),
                          paste0("'",error.msg,"' ,"),   paste0("'",table,"' ,"),    paste0("'",location.uuid,"' ) ;"  )   )
  
  affected_rows <- dbExecute(conn = mpi.con, statement = insert_query)
  return (affected_rows)
  
}


#' Grava logs de cada operacao na MPI
#' 
#' @param openmrs.con objecto de conexao com mysql  
#' @param process.type tipo de operacao
#' @param record.count affected rows
#' @return  NA
#' @examples saveProcessLog(mpi.con,process.date,process.type,affected.rows,process.status,error.msg,location.uuid)
#' 
saveProcessLog <- function(mpi.con, process.date,process.type,affected.rows,process.status,error.msg,table,location.uuid,elapsed.time){
  
  insert_query <- paste0( "insert into ccs_mpi.data_transfer_logs(process_date,process_type,record_count,process_status,error_message,table_name,location_uuid,elapsed_time) values( ", paste0("'",as.character(process.date),"' , "),
                          paste0("'",process.type,"' , "), affected.rows," ,",   paste0("'",process.status,"' ,"),
                          paste0("'",error.msg,"' ,"),   paste0("'",table,"' ,"),  paste0("'",location.uuid,"' , "),   paste0("'",elapsed.time,"' ) ;"  )   )
  
  affected_rows <- dbExecute(conn = mpi.con, statement = insert_query)
  return (affected_rows)
  
}

#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return location info
#' @examples
#' query = createSqlQueryPatient(con_openmrs)
createSqlQueryPatient <- function( param.location,param.end.date){
  
 sql_tmp <- sql_query_mpi_patients
 sql_tmp <- gsub(x =   sql_tmp, pattern = '@endDate', replacement = paste0("'",as.character(param.end.date),"'" ))
 sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
 sql_tmp
  
}


#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return location info
#' @examples
#' query = createSqlQueryPatientMpi(con_openmrs)
createSqlQueryPatientMpi <- function( param.location){
  
  sql_tmp <- "select * from ccs_mpi.patient where location_uuid= @location ;"
  sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =paste0("'",as.character(param.location),"'" ) )
  sql_tmp
  
}


#' Grava uma location na tabela ccs_mpi.
#' 
#' @param mpi.con objecto de conexao com mysql  
#' @param location.id location_id
#' @param uuid location uuid
#' @param name location name
#' @param description location description
#' @return  NA
#' @examples saveOpenmrsLocation(location_id,uuid,name,description)
#' 
saveOpenmrsLocation <- function(mpi.con,location.id,uuid,name,description,db.name) {
  
  insert_query <- paste0( "INSERT INTO ccs_mpi.location(location_id,uuid,name,description,db_name) VALUES( ", location.id," , ",
                          paste0("'",uuid,"' , ") , paste0("'",name,"' ,"),   paste0("'",description,"' ,"), paste0("'",db.name,"' ); ")   )
  
  resut_set <- dbSendStatement(conn = mpi.con, statement = insert_query)
  affected_rows <- dbGetRowsAffected(resut_set)
  dbClearResult(resut_set)
  return(affected_rows)
}


#' Remove caracteres especiais das strings
#' 
#' @param vector.string vector of strings
#' @return  NA
#' @examples removeSpecialCharacters(c("Benzilda Jo\xe3o  Maura" ))
#' 
removeSpecialCharacters <- function(vector.string) {
  
  Encoding(vector.string) <- "latin1"
  vector.string <- iconv(vector.string, "latin1", "UTF-8",sub='')
  vector.string
}






#' Busca dados de uma query enviada a CCS MPI
#'
#' @param con.mysql obejcto de conexao com BD 
#' @return query sql query
#' @examples patients <- getMasterPatientIndexData(con_openmrs,query)
getMasterPatientIndexData <- function(con.openmrs, query) {
  rs  <- dbSendQuery(con.openmrs,query)
  data <- fetch(rs, n = -1)
  dbClearResult(rs)
  return(data)
  
}




#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetOpenMRSDrugPickups <- function( param.patientid,param.location){
  
  sql_tmp <- sql_query_openmrs_levant_info
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}

#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetOpenMRSConsultas <- function( param.patientid,param.location){
  
  sql_tmp <- sql_query_openmrs_consulta_info
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}

createSqlQueryGetOpenMRSViraLoad <- function( param.patientid,param.location){
  
  sql_tmp <- sql_query_openmrs_viral_load_info
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}

#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetMPIDrugPickups <- function( param.patientid,param.location){
  
  sql_tmp <- sql_query_mpi_drug_pickups
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  #sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}



#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetMPIDConsultas <- function( param.patientid,param.location){
  
  sql_tmp <- sql_query_mpi_consulta_info
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  #sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}

#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetMPIDViraLoad  <- function( param.patientid,param.location){
  
  sql_tmp <- sql_query_mpi_viral_load_info
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  #sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}


#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetOpenMRSPatProgram <- function( param.patientid,param.location){
  
  sql_tmp <- sql_openmrs_patient_program
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  sql_tmp <- gsub(x = sql_tmp,pattern = '@location',replacement =as.character(param.location) )
  sql_tmp
  
}


#' Cria query com parametros de entrada
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return sql query
#' @examples
#' sql_drug_pickup = createSqlQueryDrugPickup(con_openmrs)
createSqlQueryGetMPIDPatProgram <- function( param.patientid,param.location){
  
  sql_tmp <- sql_mpi_patient_program
  sql_tmp <- gsub(x =   sql_tmp, pattern = '@patient_id', replacement = as.character(param.patientid ) )
  sql_tmp
  
}

#' Create MySQL insert query
#' 
#' @param df data_frame to insert  
#' @return result
#' @examples
#'  createInsertQuery(df_drugs, 'drug_pickup')
#'  

UpdateMpiData <- function( df,table.name, con.sql){

 if(table.name=="drug_pickup"){
   
    for (i in 1:nrow(df)) {
      pickup_date    <- df$pickup_date[i]
      next_scheduled <- df$next_scheduled[i]
      patient_uuid   <- df$patient_uuid[i]
      location_uuid  <- df$location_uuid[i]
      uuid           <- df$uuid[i]
      
      insert_string <- paste0("INSERT INTO ccs_mpi.drug_pickup (drug_pickup_id,pickup_date,next_scheduled,patient_uuid,location_uuid,uuid)  VALUES( null  , '", pickup_date, "' , '" , next_scheduled,"' , '" , patient_uuid,"' , '",location_uuid, "' , '"  , uuid ,"' );" )
      
      result <- tryCatch({
        
        dbExecute(con.sql, insert_string)
        
      },
      error = function(cond) {
        error_msg <- paste0(Sys.time(), "  MySQL - Nao foi possivel inserir  a info. de levantamento do paciente: ", patient_uuid, '  db:',location_uuid, "...",'data:',pickup_date, ' uuid : ', uuid)
        writeLog(file = log_file,msg = error_msg)
        writeLog(file = log_file,msg = as.character(cond))
        print(as.character(cond))
        print(error_msg)
        saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table drug_pickup ',affected.rows = 0,
                       process.status ='Failed',error.msg = as.character(cond)  , table = table.name ,location.uuid = location_uuid)
        saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table patient_visit',affected.rows = 0,
                     process.status ='Failed', error.msg = error_msg  , table = table.name ,location.uuid = location_uuid)
      },
      warning = function(cond) {
        writeLog(file = log_file,msg = as.character(cond))
        # Choose a return value in case of warning
        print(as.character(cond))
      },
      finally = {
        # NOTE:
        # Here goes everything that should be executed at the end,
        
      })
      #TODO comment this when running in production
      
      print(result)
      
      
  
      
      
    }
 }
 else if(table.name=="patient_visit"){
   
   for (i in 1:nrow(df)) {
     
     date_visit     <- df$date_visit[i]
     next_scheduled <- df$next_scheduled[i]
     patient_uuid   <- df$patient_uuid[i]
     location_uuid  <- df$location_uuid[i]
     uuid           <- df$uuid[i]
     
     insert_string <- paste0("INSERT INTO ccs_mpi.patient_visit (date_visit,next_scheduled,patient_uuid,location_uuid,uuid)  VALUES( '", date_visit, "' , '" , next_scheduled,"' , '" , patient_uuid,"' , '",location_uuid, "' , '"  , uuid ,"' );" )
     
     result <- tryCatch({
       
       dbExecute(con.sql, insert_string)
       
       
     },
     error = function(cond) {
       error_msg <- paste0(Sys.time(), "  MySQL - Nao foi possivel inserir  o seguimento de levantamento do paciente: ", patient_uuid, '  db:',location_uuid, "...",'data:',date_visit, ' uuid : ', uuid)
       writeLog(file = log_file,msg = error_msg)
       writeLog(file = log_file,msg = as.character(cond))
       print(as.character(cond))
       print(error_msg)
       saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table patient_visit',affected.rows = 0,
                      process.status ='Failed', error.msg = as.character(cond)  , table = table.name ,location.uuid = location_uuid)
       saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table patient_visit',affected.rows = 0,
                    process.status ='Failed', error.msg = error_msg  , table = table.name ,location.uuid = location_uuid)
       
     },
     warning = function(cond) {
       writeLog(file = log_file,msg = as.character(cond))
       # Choose a return value in case of warning
       print(as.character(cond))
       
     },
     finally = {
       # NOTE:
       # Here goes everything that should be executed at the end,
       
     })
     #TODO comment this when running in production
     #print(result)
     
   }
 }
 else if(table.name=="viral_load"){
    
    for (i in 1:nrow(df)) {
      
      data_cv    <- df$data_cv[i]
      viral_load_type <- df$viral_load_type[i]
      viral_load_value   <- df$viral_load_value[i]
      location_uuid  <- df$location_uuid[i]
      origem_result   <- df$origem_result[i]
      patient_uuid  <- df$patient_uuid[i]
      uuid           <- df$uuid[i]
      
      insert_string <- paste0("INSERT INTO ccs_mpi.viral_load (uuid,viral_load_value,viral_load_type,data_cv,origem_result,location_uuid,patient_uuid) VALUES( '", uuid, "' , '" , viral_load_value,"' , '" , viral_load_type,"' , '", data_cv,"' , '", origem_result,"' , '",location_uuid, "' , '"  , patient_uuid ,"' );" )
      
      result <- tryCatch({
        
        dbExecute(con.sql, insert_string)
        
        
      },
      error = function(cond) {
        error_msg <- paste0(Sys.time(), "  MySQL - Nao foi possivel inserir  a carga virla do paciente: ", patient_uuid, '  db:',location_uuid, "...", ' uuid : ', uuid)
        writeLog(file = log_file,msg = error_msg)
        writeLog(file = log_file,msg = as.character(cond))
        print(as.character(cond))
        print(error_msg)
        saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table viral_load',affected.rows = 0,
                     process.status ='Failed', error.msg = as.character(cond)  , table = table.name ,location.uuid = location_uuid)
        saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table viral_load',affected.rows = 0,
                     process.status ='Failed', error.msg = error_msg  , table = table.name ,location.uuid = location_uuid)
        
      },
      warning = function(cond) {
        writeLog(file = log_file,msg = as.character(cond))
        # Choose a return value in case of warning
        print(as.character(cond))
        
      },
      finally = {
        # NOTE:
        # Here goes everything that should be executed at the end,
        
      })
      #TODO comment this when running in production
      #print(result)
      
    }
 }
 else if(table.name=="patient_program"){
    
    for (i in 1:nrow(df)) {
      
      estado    <- df$estado[i]
      data_admissao <- df$data_admissao[i]
      data_inscricao   <- df$data_inscricao[i]
      data_fim_tratamento  <- df$data_fim_tratamento[i]
      patient_uuid   <- df$patient_uuid[i]
      location_uuid  <- df$location_uuid[i]
      uuid           <- df$uuid[i]
      
      insert_string <- paste0("INSERT INTO ccs_mpi.patient_program (estado, data_admissao,  data_fim_tratamento, patient_uuid, location_uuid, uuid) VALUES( '", estado, "' , '" , data_admissao,"' , '" , data_fim_tratamento,"' , '", patient_uuid,"' , '",location_uuid, "' , '"  , uuid ,"' );" )
      
      result <- tryCatch({
        
        dbExecute(con.sql, insert_string)
        
        
      },
      error = function(cond) {
        error_msg <- paste0(Sys.time(), "  MySQL - Nao foi possivel inserir  o programa  do paciente: ", patient_uuid, '  db:',location_uuid, "...", ' uuid : ', uuid)
        writeLog(file = log_file,msg = error_msg)
        writeLog(file = log_file,msg = as.character(cond))
        print(as.character(cond))
        print(error_msg)
        saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table patient program',affected.rows = 0,
                     process.status ='Failed', error.msg = as.character(cond)  , table = table.name ,location.uuid = location_uuid)
        saveErrorLog(mpi.con = con_mpi, process.date = Sys.time(),process.type = 'Insert on table patient program',affected.rows = 0,
                     process.status ='Failed', error.msg = error_msg  , table = table.name ,location.uuid = location_uuid)
        
      },
      warning = function(cond) {
        writeLog(file = log_file,msg = as.character(cond))
        # Choose a return value in case of warning
        print(as.character(cond))
        
      },
      finally = {
        # NOTE:
        # Here goes everything that should be executed at the end,
        
      })
      #TODO comment this when running in production
      #print(result)
      
    }
  }
 else{
   # Do nothing
   }
}
