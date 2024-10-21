import json
import logging
import uuid
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_timestamp, to_date,lit
import traceback
import inspect
import dvelt_functions as dvf
import dvelt_mdvalidation as mdv
import dvelt_nabujobaudit as nja
import dvelt_srcfilemanager as sfmgr
#from ...common.scripts import dvelt_srcfilemanager as sfmgr
#**************************************************************************************************
# Script/Module Name - raw_sa_load_csv_file_v1.py
# Purpose - Load a CSV/Delimited file into ICEBERG based on metadata configuration 
# Change History:
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Lachhaman Moharana  -           Initial version
#
#**************************************************************************************************
# Runtime Parameters, passed through spark submit
#  1) spark.finopsENV.src_sys_key - Source system key as defined in elt_src_sys_load_def table
#  2) spark.finopsENV.src_entity_key - Source file identifier as defined in elt_src_entity_load_def
#  3) spark.finopsENV.config_name  - Name of the environment configuration file
#  4) spark.finopsENV.config_path  - Configuration file path name
#  5) spark.finopsENV.mapping_path - Name of the path where the mapping JSON is located"
#  6) spark.finopsENV.mapping_name - Source to Target Mapping JSON file name
#  7) spark.finopsENV.mapping_type - Type of mapping name (file - single mapping, list - list of 
#                                    mappings)
#--------------------------------------------------------------------------------------------------
# Global Variables
#   --spark - Used all across functions, initialized from spark session 
#   --g_config_name_with_path - Name of the JSON file holding all the environment variable values.                                
#   --g_mapping_name_with_path -  Name of the mapping JSON file holding source to target def
#   --g_mapping_type - Mapping type [fle/list] passed through spark-shell command --conf variable
#   --g_mapping_path - Github relative path name where the mapping JSON is located (i.e. mappings)
#   --g_logger - Logger object customized to pretty print messages into the spark run log
#--------------------------------------------------------------------------------------------------
spark:SparkSession
g_config_name_with_path:str
g_mapping_name_with_path:str
g_mapping_type:str
g_mapping_path:str
g_src_file_path:str
g_logger:logging.Logger = dvf.get_logger()
g_dvaudit:nja.NabuJobAudit = nja.NabuJobAudit()
g_filemgr:sfmgr.SourceFileManager = sfmgr.SourceFileManager()
#------------------------------ Metadata Repository Database/Tables -----------------------------
def logInfo(p_message,p_target = "both"):
    g_logger.info(p_message)
def logCritical(p_message):
        g_logger.critical(p_message)
def logWarning(p_message):
        g_logger.warn(p_message)
#**************************************************************************************************
# Initialize Runtime Global variables and Create spark session, Logger Objects
#**************************************************************************************************
def init_environment():
    
    global g_config_name_with_path
    global g_mapping_name_with_path
    global g_mapping_type
    global g_mapping_path
    global g_env_name
    #
    l_req_vars = [
        "spark.finopsENV.config_name",
        "spark.finopsENV.config_path",
        "spark.finopsENV.mapping_name",
        "spark.finopsENV.mapping_path",
        "spark.finopsENV.mapping_type",
        "spark.finopsENV.environment"
    ]
    #
    l_spark_vars = {sv[0]:sv[1] for sv in spark.sparkContext.getConf().getAll()}
    #
    l_vars_present = dvf.check_values_present(l_req_vars,l_spark_vars)    
    #
    if l_vars_present != "yes":
        logInfo(f"Required spark runtime variables "+ ",".join(l_req_vars) +" not found {l_vars_present}")
        return False
    #
    l_config_name  = l_spark_vars["spark.finopsENV.config_name"]
    l_config_path  = l_spark_vars["spark.finopsENV.config_path"]
    l_mapping_name = l_spark_vars["spark.finopsENV.mapping_name"]
    g_mapping_path = l_spark_vars["spark.finopsENV.mapping_path"]
    g_mapping_type = l_spark_vars["spark.finopsENV.mapping_type"]
    g_env_name = l_spark_vars["spark.finopsENV.environment"]
    #
    logInfo(f"Config name - {l_config_name}")
    logInfo(f"Config path - {l_config_path}")
    logInfo(f"Mapping path - {g_mapping_path}")
    logInfo(f"Mapping name - {l_mapping_name}")
    logInfo(f"Mapping type - {g_mapping_type}")
    logInfo(f"Environment name - {g_env_name}")
    #
    if l_config_path[-1] == "/":
        g_config_name_with_path = l_config_path+l_config_name
    else:
        g_config_name_with_path = l_config_path + "/" + l_config_name
    #
    if g_mapping_path[-1] == "/":
        g_mapping_name_with_path = g_mapping_path+l_mapping_name
    else:
        g_mapping_name_with_path = g_mapping_path + "/" + l_mapping_name
    #    
    if not (dvf.is_valid_file(g_mapping_name_with_path)):
        logCritical(f"{g_mapping_name_with_path} is not a valid file/does not exist")
        return False
    logInfo(f"Mapping File Name => {g_mapping_name_with_path}")    
    #
    if not (dvf.is_valid_file(g_config_name_with_path)):
        logCritical(f"{g_mapping_name_with_path} is not a valid file/does not exist")
        return False
    logInfo(f"Environment variables file => ({g_config_name_with_path})")
    #
    if g_mapping_type not in ["file","list"]:
        logCritical(f"Invalid mapping type specified {g_mapping_type}, must be 'file' or 'list'")
        return False
    #
    return True
#**************************************************************************************************
# Initialize Environment Variables
#**************************************************************************************************
def get_config_variables()->bool:
    global g_src_file_path
    global g_dvaudit
    global g_filemgr
    try:
        l_jfile = open(g_config_name_with_path,"r")
        l_json_obj = json.load(l_jfile)
        # Find out the environment to be used and select variables for that environment
        l_env_name = l_json_obj.get("Environment")
        l_env_json = l_json_obj.get(g_env_name)
        #
        g_src_file_path = l_env_json.get("SourcePath")
        g_dvaudit.DomainName = l_json_obj.get("DomainName")
        g_dvaudit.LayerName = l_json_obj.get("LayerName")
        g_dvaudit.ApplicationID = spark.sparkContext.applicationId
        #
        l_aud_connection_name = l_env_json.get("NabuAuditConnection").get("ConnectionName")
        g_dvaudit.AuditDatabaseName = l_env_json.get("NabuAuditConnection").get("DatabaseName")
        g_dvaudit.AuditDatabaseHost = l_env_json.get("NabuAuditConnection").get("DatabaseHostName")
        g_dvaudit.AuditDatabasePort = l_env_json.get("NabuAuditConnection").get("MaintenanceTableName")
        #
        g_dvaudit.AuditTableName = l_env_json.get("NabuAuditConnection").get("AuditTableName")
        g_dvaudit.MaintenanceTableName = l_env_json.get("NabuAuditConnection").get("MaintenanceTableName")
        g_dvaudit.AuditDatabasePort = l_env_json.get("NabuAuditConnection").get("DatabasePortNo")
        #
        l_resp = dvf.get_credential_from_nabu(spark,l_aud_connection_name)
        if (l_resp.status_code != 200):
            logCritical(f"Unable to retrieve password from nabu for audit connection {l_aud_connection_name}, error -{l_resp.reason}")
            return False
        #
        l_respjson = json.loads(l_resp.text)
        g_dvaudit.AuditDatabaseUser = l_respjson.get("data").get("username")
        g_dvaudit.AuditDatabasePassword =  l_respjson.get("data").get("password")      
        #
        g_filemgr.MetdataDbHost = g_dvaudit.AuditDatabaseHost
        g_filemgr.MetdataDbName = l_env_json.get("NabuAuditConnection").get("EltDatabaseName")
        g_filemgr.MetdataDbPort = g_dvaudit.AuditDatabasePort
        g_filemgr.MetdataDbUsername = g_dvaudit.AuditDatabaseUser
        g_filemgr.MetdataDbPassword = g_dvaudit.AuditDatabasePassword
        #        
        l_jfile.close()
        #              
        return True    
    except Exception as err:    
        frame = inspect.currentframe()
        logCritical(f"Exception at {frame.f_code.co_name}")
        logCritical(f"Exception occurred while validating runtime parameters","file")
        logCritical(f"Unexpected {err=}, {type(err)=}","file")
        traceback.print_exc()
        return False
    
def load_file_to_iceberg_ext(p_src_file_rec:Row,p_stm_json_obj:dict)->bool:
    global g_dvaudit
    #All validations are complete before this step
    l_target_db_name = p_stm_json_obj.get("TargetDatabaseName")
    l_target_tbl_name = p_stm_json_obj.get("TargetTableName")
    #
    g_dvaudit.TargetDatabaseName = l_target_db_name
    g_dvaudit.TargetTableName = l_target_tbl_name
    g_dvaudit.JobID = p_src_file_rec["src_entity_load_run_id"]
    g_dvaudit.JobName = p_stm_json_obj.get("MappingName").replace("_map","")
    #
    l_src_file_name = p_src_file_rec["src_entity_path"] + "/" + p_src_file_rec["src_entity_name"]
    #
    g_dvaudit.JobStarted()
    #
    l_tgt_src_col_dict = {f.get("TargetColumnName"): f.get("SourceColumnName") for f in p_stm_json_obj.get("SourceToTargetMapping")}
    l_src_col_list = [f"`{f.get('SourceColumnName')}` {f.get('TargetDataType')}" for f in p_stm_json_obj.get("SourceToTargetMapping")]    
    l_sch_str = ",".join(l_src_col_list)
    l_options = mdv.get_csv_reader_options(p_stm_json_obj,False)    
    #    
    logInfo(f"Source reading options used -> {l_options}")
    logInfo(f"Schema definition used to read file -> {l_sch_str}")
    l_options.update({'schema':l_sch_str})
    #
    try:
        df_src1 = spark.read.csv(l_src_file_name,**l_options)  
        logInfo("Source File Schema")
        df_src1.printSchema()
        l_src_tgt_cols_expr = ["`{x}` as {y}".format(x=l_tgt_src_col_dict.get(stc),y=stc)  
                                  for stc in l_tgt_src_col_dict]
        df_src2 = df_src1.selectExpr(l_src_tgt_cols_expr)
        logInfo("Source File Schema Renamed to Match Target Columns")
        df_src2.printSchema()
        logInfo(f"Writing data to table {l_target_db_name}.{l_target_tbl_name}")
        df_src2.write.format("iceberg").mode("append").saveAsTable(f"{l_target_db_name}.{l_target_tbl_name}")
        logInfo(f"Successfully appended {df_src2.count()} records into {l_target_db_name}.{l_target_tbl_name}")
        #
        g_dvaudit.total_data_count = df_src2.count()
        logInfo("Analyzing the target table for collecting auditing and maintenance")
        g_dvaudit.AnalyzeTarget(spark)
        logInfo("Analysis completed, marking the job completed")
        g_dvaudit.JobCompleted()
        #
        return True
    except Exception as err:
        g_dvaudit.JobFailed()
        frame = inspect.currentframe()
        logCritical(f"Unable to process file, process failed, exception at {frame.f_code.co_name}")
        traceback.print_exc() 
        return False    
    #
#**************************************************************************************************** 
# Load a file
#****************************************************************************************************
def load_file_to_iceberg(p_mapping_script_name_with_path:str)->bool:
    #    
    logInfo(f"Mapping Script -> {p_mapping_script_name_with_path}")
    #
    if (not p_mapping_script_name_with_path):
        logWarning("Mapping script name not provided, process skipped")
        return False  
    #
    if (not dvf.is_valid_file(p_mapping_script_name_with_path)):
        logWarning(f"{p_mapping_script_name_with_path} is not a valid file, load skipped")
        return False
    #
    logInfo(f"Reading the mapping script -> {p_mapping_script_name_with_path}")
    #
    try:
        l_mapping_def_file = open(p_mapping_script_name_with_path,'r')
        l_mapping_def_json = json.load(l_mapping_def_file)
        l_mapping_def_file.close()
    except Exception as err:
        frame = inspect.currentframe()
        logCritical(f"Exception at {frame.f_code.co_name}")
        logCritical(f"Unable to open or read STM script{p_mapping_script_name_with_path}, processing skipped")
        logCritical(f"Unexpected {err=}, {type(err)=}")
        traceback.print_exc()
        return  False   
    #
    # Validate the json file ensure definition is correct
    #
    logInfo(f"Validating the mapping definition -> {p_mapping_script_name_with_path}")
    #
    l_mapping_valid = mdv.validate_csv_mapping_def(l_mapping_def_json)
    #
    if (l_mapping_valid != "valid"):
        logCritical(f"CSV Mapping {p_mapping_script_name_with_path} is incomplete, missing - {l_mapping_valid}",)        
        return False
    # 
    logInfo(f"Mapping definition -> {p_mapping_script_name_with_path}, is valid, proceeding for data load")
    #    
    l_target_db_name = l_mapping_def_json.get("TargetDatabaseName")
    l_target_table_name = l_mapping_def_json.get("TargetTableName")    
    #
    logInfo(f"Checking for existence of target table {l_target_db_name}.{l_target_table_name}")
    #
    if not spark.catalog.tableExists(f"{l_target_table_name}",f"{l_target_db_name}"):
        logCritical(f"Target table {l_target_db_name}.{l_target_table_name} does not exist, process skipped")
        return False
    #
    logInfo(f"Target table {l_target_db_name}.{l_target_table_name} found")
    #
    l_src_extract_type = l_mapping_def_json.get("SourceExtractType")
    l_target_load_type = l_mapping_def_json.get("TargetLoadType")
    l_src_file_pattern = l_mapping_def_json.get("SourceFilePattern")
    l_load_all_incr_files = l_mapping_def_json.get("LoadAllIncrementalFiles")
    l_src_sys_key = l_mapping_def_json.get("SourceSystemID")
    l_src_entity_key = l_mapping_def_json.get("SourceFileID")    
    #
    logInfo(f"Source Extract Type = {l_src_extract_type}, Source File Pattern = {l_src_file_pattern}, Load All Incremental Files = {l_load_all_incr_files}")
    logInfo(f"Target Load Type = {l_target_load_type}")
    #
    if (l_target_load_type == "overwrite"):
        logInfo(f"Truncating table {l_target_db_name}.{l_target_table_name}")
        spark.sql(f"truncate table {l_target_db_name}.{l_target_table_name}")
    #
    logInfo(f"Getting source file(s) for processing")
    #l_src_sys_rec = spark.sql(f"select * from {l_target_db_name}.elt_src_sys_load_def where src_sys_key='{l_src_sys_key}'").first()
    #l_src_file_rec = spark.sql(f"select * from {l_target_db_name}.elt_src_entity_load_def where src_sys_key='{l_src_sys_key}' and src_entity_key ='{l_src_entity_key}' ").first()
    l_src_file_rec = g_filemgr.GetSourceEntityRecord(l_src_sys_key,l_src_entity_key)    
    l_file_list = g_filemgr.GetUnprocessedEntityList(l_src_sys_key,l_src_entity_key)
    #
    #  
    if not l_file_list:
        logInfo("Search returned no files(s), processing will be skipped")
        return True 
    #
    if l_src_file_rec["src_entity_extract_type"] == "full":
        l_src_file_name = l_file_list[0]["src_entity_name"]
        logInfo(f"Processing data file {l_src_file_name}")
        l_proc_status = load_file_to_iceberg_ext(l_file_list[0],l_mapping_def_json)
        l_job_status = 'success' if l_proc_status else 'failed'
        g_filemgr.mark_file_processed(l_file_list[0],l_job_status)
        if not l_proc_status:
            return False
        if len(l_file_list) > 1:
            logInfo("Latest file was loaded, there are older files available and will be marked skipped")
            g_filemgr.skip_files_other_than_first(l_file_list)
        return True
    elif l_src_file_rec["src_entity_extract_type"] == "incremental" and l_load_all_incr_files == "yes":
        logInfo("Source extract is incremental, option provided to load all unprocessed files")
        for frec in l_file_list:
            l_src_file_name = frec["src_entity_name"]
            l_proc_status = load_file_to_iceberg_ext(frec,l_mapping_def_json)
            l_job_status = 'success' if l_proc_status else 'failed'
            g_filemgr.mark_file_processed(frec,l_job_status)
            if not l_proc_status:
                return False
        return True
    elif l_src_file_rec["src_entity_extract_type"] == "incremental" and l_load_all_incr_files == "no":        
        logInfo("Source extract is incremental, option provided to load the earlied unprocessed file")
        logInfo(f"Processing file {l_file_list[-1]['src_entity_name']}")
        l_proc_status =  load_file_to_iceberg_ext(l_file_list[-1],l_mapping_def_json)
        l_job_status = 'success' if l_proc_status else 'failed'
        g_filemgr.mark_file_processed(l_file_list[-1],l_job_status)
        return l_proc_status
    else:
        logInfo("Invalid source file processing setup provided")
        return False    
    #endif   
            
#**************************************************************************************************** 
# Load Multiple files
#****************************************************************************************************
def load_files_to_iceberg(p_mapping_list_name_with_path:list)->bool:
    logInfo(f"Processing the Mapping list {p_mapping_list_name_with_path}")
    try:
        l_mfile = open(p_mapping_list_name_with_path,'r')
        l_mjson = json.load(l_mfile)
        l_mapping_list = l_mjson.get("MappingList",[])
        for mapping_name in l_mapping_list:
            if (g_mapping_path[-1] == "/"):
                l_script_file = g_mapping_path + mapping_name.get("MappingName")
            else:
                l_script_file = g_mapping_path + "/" + mapping_name.get("MappingName")                  
            if not load_file_to_iceberg(l_script_file):
                return False
        #The batch completed successfully
        logInfo(f"Processing the Mapping list {p_mapping_list_name_with_path} completed")
        return True    
    except Exception as err:
        frame = inspect.currentframe()
        logCritical(f"Unable to process file, process failed, exception at {frame.f_code.co_name}")
        logCritical(f"Error occurred while loading list of files {p_mapping_list_name_with_path},process stopped")
        logCritical(f"Unexpected {err=}, {type(err)=}")
        traceback.print_exc() 
        return False
  
#**************************************************************************************************** 
# MAIN: Init, Validate and Load
#****************************************************************************************************
if __name__ == "__main__":
    try:
        #
        spark = dvf.create_spark_session("Loading Delimited Data File using Configurable Framework",g_logger) 
        #
        if not spark:
            dvf.stop_and_exit(None,1)
        #    
        logInfo(f"Spark Session created. Initializing environment variables")
        #
        if not init_environment():
            dvf.stop_and_exit(spark,1)
        #
        if not get_config_variables():
            dvf.stop_and_exit(spark,1)        
        #
        logInfo(f"Environment validation/setup completed")
        #             
        if (g_mapping_type == "file"):
            #load the file 
            if not load_file_to_iceberg(g_mapping_name_with_path):
                logInfo("MAIN:Processing unsuccessfull")
                dvf.stop_and_exit(spark,1)
        else:
            #load the files from the list
            if not load_files_to_iceberg(g_mapping_name_with_path):
                logInfo("MAIN:Processing unsuccessfull")
                dvf.stop_and_exit(spark,1)
        #Happy Ending!

        logInfo("MAIN:Processing completed successfully")
        dvf.stop_and_exit(spark,0)
    except Exception as err:
        #Handle any unhandled error
        frame = inspect.currentframe()
        logCritical(f"Unable to process file, process failed, reported at {frame.f_code.co_name}")
        logCritical(f"Unhandled exception occurred. See log file for trace details")
        logCritical(f"Unexpected {err=}, {type(err)=}")
        traceback.print_exc()

        dvf.stop_and_exit(spark,1)
#endif    
