import xml.etree.ElementTree as et
from xml.etree.ElementTree import XMLParser
from  dvelt_xmldatareader import XmlDataReader
import json, logging,traceback,inspect, boto3,uuid
from pyspark.sql import SparkSession, Row
import dvelt_functions as dvf
import dvelt_nabujobaudit as nja
#import ...common.scripts.dvelt_nabujobaudit as nja
import dvelt_srcfilemanager as sfmgr
#
#**************************************************************************************************
# Script/Module Name - raw_sap_load_idoc_files_v1.py
# Purpose - Load SAP IDOC file into ICEBERG table(s) based on metadata configuration 
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
#***************************************************************************************************
spark:SparkSession
g_config_name_with_path:str
g_mapping_name_with_path:str
g_mapping_type:str
g_mapping_path:str
g_src_file_path:str
g_logger:logging.Logger = dvf.get_logger()
g_dvaudit:nja.NabuJobAudit = nja.NabuJobAudit()
g_filemgr:sfmgr.SourceFileManager = sfmgr.SourceFileManager()
g_lookup_data = {}
#******************************** Log Management Helper Functions *********************************
def logInfo(p_message:str):
    g_logger.info("[+]"+p_message)
def logCritical(p_message:str):
    g_logger.critical("[+]"+p_message)
def logWarning(p_message:str):
    g_logger.warn("[+]"+p_message)
#
def logException(p_frame,p_err,p_message):
    logCritical(f"Exception occurred at {p_frame.f_code.co_name}")
    logCritical(f"Error {p_err=}, {type(p_err)=} ")
    logCritical(p_message)    
    traceback.print_exc()        
#**************************************************************************************************
# Initialize Runtime Global variables and Create spark session, Logger Objects
#**************************************************************************************************
def init_environment():
    
    global g_config_name_with_path
    global g_mapping_name_with_path
    global g_mapping_type
    global g_mapping_path
    global g_env_name
    global g_lookup_data
    #
    g_lookup_data.update({"target_tables":[]})
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
        logException(inspect.currentframe(),err,"Exception occurred while validating runtime parameters")
        return False
#    
def analyze_tables():
    global g_dvaudit
    l_tbl_list = g_lookup_data["target_tables"]
    l_db_name = g_lookup_data["target_database"]
    for tbl in l_tbl_list:
        logInfo(f"Analyzing table {l_db_name}.{tbl}")
        g_dvaudit.TargetDatabaseName = l_db_name
        g_dvaudit.TargetTableName = tbl
        g_dvaudit.AnalyzeTarget(spark)
#
         
def load_file_to_iceberg_ext(p_src_file_rec:Row,p_stm_json_obj:dict)->bool:
    global g_dvaudit
    global g_lookup_data
    #
    logInfo(f"Processing source file -> {p_src_file_rec['src_entity_name']}")
    #

    g_dvaudit.JobID = p_src_file_rec["src_entity_load_run_id"]
    g_dvaudit.JobName = p_src_file_rec["src_entity_load_run_id"]
    #All validations are complete before this step
    g_dvaudit.JobStarted()
    l_tgt_db_name = p_stm_json_obj["TargetDatabaseName"]
    l_stg_db_name = p_stm_json_obj["StagingDatabaseName"]
    l_tbl_list = {varx["RecordXPath"]:varx["TargetTableName"] for varx in p_stm_json_obj["RecordSets"]}
    #    
    g_lookup_data.update({"target_database":l_tgt_db_name})
    #    
    try:
        l_s3_path_tokens = p_src_file_rec["src_entity_path"].split("/")
        l_s3_bucket = l_s3_path_tokens[2]
        l_s3_file = "/".join(l_s3_path_tokens[3:]) + "/" + p_src_file_rec["src_entity_name"]
        #
        l_s3_client=boto3.client('s3')
        l_response=l_s3_client.get_object(Bucket=l_s3_bucket,Key=l_s3_file)
        l_file_content = l_response['Body'].read().decode()
        #
        #print(l_file_content)
        #
        l_xmlreader = XmlDataReader()
        l_xmlreader.prepareReader(p_stm_json_obj)
        l_parser = XMLParser(target=l_xmlreader)
        l_parser.feed(l_file_content)
        l_parser.close()
        #
        logInfo(f"Completed parsing the data file -> {p_src_file_rec['src_entity_name']}" )
        logInfo("Data for the following Recordsets were found in the file \n{}".format(",\n".join(l_xmlreader.getRecordsetNames())))
        l_rec_count = 0
        for tgtKey in l_xmlreader.getRecordsetNames():
            logInfo(f"Preparing to write data into {l_tbl_list[tgtKey]}")
            l_tgt_tbl_name = l_tbl_list[tgtKey]
            if l_tgt_db_name not in g_lookup_data["target_tables"]:
                g_lookup_data["target_tables"].append(l_tgt_tbl_name)
            #
            l_src_dflt_sch = l_xmlreader.getDefaultSchema(tgtKey)
            l_src_df = spark.createDataFrame(l_xmlreader.getRecordset(tgtKey),l_src_dflt_sch)
            l_src_df.printSchema()
            logInfo(f"Truncating the staging table {l_stg_db_name}.{l_tgt_tbl_name}")
            #spark.sql(f"truncate table {l_stg_db_name}.{l_tgt_tbl_name}")
            l_rec_count += l_src_df.count()
            l_src_df.write.format("iceberg").mode("append").saveAsTable(f"{l_stg_db_name}.{l_tgt_tbl_name}")
            logInfo(f"{l_src_df.count()} record(s) inserted {l_stg_db_name}.{l_tgt_tbl_name} successfully")            
               
        #
        g_dvaudit.total_data_count = l_rec_count
        g_dvaudit.incremental_data_count = l_rec_count
        #
        g_dvaudit.JobCompleted()
        return True
    except Exception as err:
        logException(inspect.currentframe(),err,"Exception occurred while loading data file to ICEBERG")
        return False
        
    #
#**************************************************************************************************** 
# Load a file
#****************************************************************************************************
def load_file_to_iceberg(p_mapping_script_name_with_path:str)->bool:
    # 
    global g_dvaudit
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
        logException(inspect.currentframe(),err,f"Unable to open or read STM script{p_mapping_script_name_with_path}, processing skipped")
        return  False   
    #
    # # 
    logInfo(f"Mapping definition -> {p_mapping_script_name_with_path}, is valid, proceeding for data load")
    #    
    l_target_db_name = l_mapping_def_json.get("TargetDatabaseName")
    l_stg_db_name = l_mapping_def_json.get("StagingDatabaseName")
    #
    #l_target_load_type = l_mapping_def_json.get("TargetLoadType")
    #
    #l_load_all_incr_files = l_mapping_def_json.get("LoadAllIncrementalFiles")
    l_src_sys_key = l_mapping_def_json.get("SourceSystemID")
    l_src_entity_key = l_mapping_def_json.get("SourceFileID")    
    #
    logInfo(f"Searching for available files for Source system {l_src_sys_key}, file id {l_src_sys_key}")
    #l_src_file_rec = g_filemgr.GetSourceEntityRecord(l_src_sys_key,l_src_entity_key)    
    l_file_list = g_filemgr.GetUnprocessedEntityList(l_src_sys_key,l_src_entity_key)
    #
    if not l_file_list:
        logInfo("Search returned no files(s), processing will be skipped")
        return True 
    #
    logInfo("Checking for availability of target tables")
    l_rs_def = l_mapping_def_json["RecordSets"]
    for rs1 in l_rs_def:
        #
        l_tbl_name = rs1["TargetTableName"]
        if not spark.catalog.tableExists(f"{l_tbl_name}",f"{l_target_db_name}"):
            logCritical(f"Target table {l_target_db_name}.{l_tbl_name} does not exist, process skipped")
            return False
        if not spark.catalog.tableExists(f"{l_tbl_name}",f"{l_stg_db_name}"):
            logCritical(f"Staging table {l_stg_db_name}.{l_tbl_name} does not exist, process skipped")
            return False
        #
        logInfo(f"Truncating staging table {l_stg_db_name}.{l_tbl_name}")
        #
        spark.sql(f"truncate table {l_stg_db_name}.{l_tbl_name}")
    #
    logInfo(f"All staging and raw tables for the file {l_src_entity_key} were found")
    #
    for frec in l_file_list:
        #
        l_proc_status = load_file_to_iceberg_ext(frec,l_mapping_def_json)
        l_job_status = 'success' if l_proc_status else 'failed'
        g_filemgr.mark_file_processed(frec,l_job_status)
        if not l_proc_status:
            return False     
    #
    return True
    #        
#**************************************************************************************************** 
# MAIN: Init, Validate and Load
#****************************************************************************************************
if __name__ == "__main__":
    try:
        #
        spark = dvf.create_spark_session("Loading SAP IDOC data using Configurable Framework",g_logger) 
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
                logInfo("MAIN:Processing failed")
                dvf.stop_and_exit(spark,1)
        else:
            logCritical("Invalid mapping type option, only 'file' is the valid option")
            logInfo("MAIN:Processing failed")
            dvf.stop_and_exit(spark,1)
        #Happy Ending!
        logInfo("Analyzing Target tables loaded")
        analyze_tables()
        #
        logInfo("MAIN:Processing completed successfully")
        dvf.stop_and_exit(spark,0)
    except Exception as err:
        #Handle any unhandled error
        logException(inspect.currentframe(),err,"MAIN:Unable to process files(s)")
        dvf.stop_and_exit(spark,1)
#endif    
