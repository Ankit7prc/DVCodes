from datetime import datetime
#import sys
import traceback
#import logging
from pyspark.sql import SparkSession, Row 
from pyspark.sql.functions import row_number, col
from pyspark.sql.types import NullType
import boto3
import fnmatch
import json
#import re
from dvelt_loadlogger import LoadLogger
import dvelt_functions as dvf
#import dvelt_mdvalidation as mdv
import psycopg2
import uuid
#*****************************************************************************************************
# Program : <dbname>_stage_source_files_v3.py
# Developed by : Lachhaman Moharana
# Purpose : Reading the s3 bucket for files of a source system and staging them for processing
# Functionality -
#   1. Scan the s3 bucket path for a source system
#   2. Read all the available files
#   3. With file timestamp.sourcesystem.filename as key look for a file if it is not staged
#   4. Take the file and make a database entry with status STAGED for next process to pick up
# Warning -
#   S3 does not support for wild card scanning, so file names for wildcard naming should not be
#   specified like *.*, this will cause all files to be picked up. A max file scan limit of
#   99999 is set to have better performance. If files pile up faster, consider archiving the
#   the old files and move them away from processing folder 
#****************************************************************************************************
def logInfo(p_message,p_target = "file"):
    if (p_target == "both"):
        g_logger.info(p_message)
        #g_dbLogger.Info(p_message)
    else:
        g_logger.info(p_message)
def logCritical(p_message,p_target="both"):
        g_logger.critical(p_message)
        #if (p_target == "both"):
        #    g_dbLogger.Critical(p_message)
def logWarning(p_message,p_target="both"):
        g_logger.warn(p_message)
        #if (p_target == "both"):
        #    g_dbLogger.Warning(p_message)
#****************************************************************************************************
# Initialize environment 
#****************************************************************************************************        
def init_environment():

    global g_config_name_with_path
    global g_config_name
    global g_config_path    
    # global g_src_sys_id
    global g_src_sys_key
    global g_environment
    global g_db_name
    #
    l_spark_vars = {sv[0]:sv[1] for sv in spark.sparkContext.getConf().getAll() if sv[0] \
                    in ["spark.finopsENV.src_sys_key",
                        "spark.finopsENV.config_name",
                        "spark.finopsENV.config_path",
                        "spark.finopsENV.environment",
                        "spark.finopsENV.db_name"]}
    l_req_vars = [
        "spark.finopsENV.src_sys_key",
        "spark.finopsENV.config_name",
        "spark.finopsENV.config_path",
        "spark.finopsENV.environment",
        "spark.finopsENV.db_name"
        ]   
    #
    l_vars_present = dvf.check_values_present(l_req_vars,l_spark_vars)
    #
    if l_vars_present != "yes":
        logInfo(f"Required spark runtime variables "+ ",".join(l_req_vars) + f" not found {l_vars_present}")
        return False
    #
    g_src_sys_key = l_spark_vars["spark.finopsENV.src_sys_key"]
    logInfo(f"Source System ID -> ({g_src_sys_key})")
    #
    g_config_name = l_spark_vars["spark.finopsENV.config_name"]
    logInfo(f"Configuration file name -> ({g_config_name})")
    #
    g_config_path =  l_spark_vars["spark.finopsENV.config_path"]   
    logInfo(f"Configuration file name -> ({g_config_path})")
    #
    g_environment = l_spark_vars["spark.finopsENV.environment"]  
    logInfo(f"Environment name -> ({g_environment})")
    #
    g_db_name = l_spark_vars["spark.finopsENV.db_name"]
    logInfo(f"Database name -> ({g_db_name})")
    #
    if g_config_path[-1] == '/':          
        g_config_name_with_path = g_config_path + g_config_name
    else: 
        g_config_name_with_path = g_config_path + "/" + g_config_name
    #
    if (not dvf.is_valid_file(g_config_name_with_path)):
        logInfo(f"{g_config_name_with_path} is not a valid file")
        return False    
    #
    return True
    #
#****************************************************************************************************
# Get environment configuration variables 
#****************************************************************************************************        
def get_config_variables():
    global g_pg_connection
    #
    try:
        l_jfile = open(g_config_name_with_path,"r")
        l_jobj = json.load(l_jfile)
        l_pg_conn_name = l_jobj[g_environment]["NabuAuditConnection"]["ConnectionName"]
        l_pg_host_name = l_jobj[g_environment]["NabuAuditConnection"]["DatabaseHostName"]
        #l_pg_db_name = l_jobj[g_environment]["NabuAuditConnection"]["DatabaseName"]
        l_pg_db_name = l_jobj[g_environment]["NabuAuditConnection"]["EltDatabaseName"]

        l_pg_port_no = l_jobj[g_environment]["NabuAuditConnection"]["DatabasePortNo"]
        #
        l_resp = dvf.get_credential_from_nabu(spark,l_pg_conn_name)
        #
        if (l_resp.status_code != 200):
            logCritical(f"Unable to retrieve password from nabu for source connection {l_pg_conn_name}, reason - {l_resp.reason}, code ->{l_resp.status_code}")
            return False
        #
        l_respjson = json.loads(l_resp.text)
        l_pg_username = l_respjson.get("data").get("username")
        l_pg_password =  l_respjson.get("data").get("password") 
        #
        g_pg_connection = psycopg2.connect(database = f"{l_pg_db_name}", 
                user = f"{l_pg_username}", 
                host= f"{l_pg_host_name}",
                password = f"{l_pg_password}",
                port = l_pg_port_no)                     
        return True    
    except Exception as err:    
        logCritical(f"Exception occurred while validating runtime variables","file")
        logCritical(f"Unexpected {err=}, {type(err)=}","file")
        return False
#**************************************************************************************************** 
# Get source entity/file details from repository database for the source system
#****************************************************************************************************
def get_source_entity_def(p_src_sys_key):
    l_sql_qry = f"""select 
    src_sys_key,
    src_entity_key,
    src_entity_id,
    src_entity_pattern 
    from finops.elt_src_entity_load_def
    where src_sys_key='{p_src_sys_key}' and src_entity_status='active' """
    #
    try:    
        l_cur = g_pg_connection.cursor()
        l_cur.execute(l_sql_qry)
        if l_cur.rowcount == 0:
            return None
        l_pg_rows = l_cur.fetchall() 
        l_ent_rows = [Row(src_sys_key=pr[0],
                          src_entity_key=pr[1],
                          src_entity_id=pr[2],
                          src_entity_pattern=pr[3]) for pr in l_pg_rows]    
        #
        l_cur.close()
        #
        return l_ent_rows
    except:
        logCritical(f"Error occurred while fetching source entity records for source system key {p_src_sys_key}")   
        logInfo(l_sql_qry)    
        traceback.print_exc()
        return None
#**************************************************************************************************** 
# Get source system details from repository database, it should have only one record for source sys
#****************************************************************************************************
def get_source_system_def(p_src_sys_key):
    # global g_dbLogger
    l_sql_qry = f"""select src_sys_key,src_sys_id,src_sys_name,src_entity_path 
                from finops.elt_src_sys_load_def 
                where src_sys_key='{p_src_sys_key}' and src_sys_status='active' """
    try:    
        l_cur = g_pg_connection.cursor()
        l_cur.execute(l_sql_qry)
        if l_cur.rowcount == 0:
            return None
        l_pg_row = l_cur.fetchone()
        l_cur.close()
        #
        l_row = Row(src_sys_key=l_pg_row[0],src_sys_id=l_pg_row[1],src_sys_name=l_pg_row[2],src_entity_path=l_pg_row[3])        
        #
        return l_row                
    except:
        logCritical(f"Error occurred while fetching source system information for {g_src_sys_key}")
        logCritical(l_sql_qry)
        traceback.print_exc()
        return None
#*************************** Build s3 path and prefix variables ***********************************
#**************************************************************************************************** 
# Stage the new files
#****************************************************************************************************
def stage_new_source_files():
    if not g_src_sys_key:
        logCritical("Source system key is null")
        return False
    #
    l_src_sys_rec = get_source_system_def(g_src_sys_key)
    if not l_src_sys_rec:
        logCritical(f"Source sytem definition record not found for {g_src_sys_key}")
        return False
    #
    l_src_entity_recs = get_source_entity_def(g_src_sys_key)
    if not l_src_entity_recs:
        logCritical(f"Source entity records not found for source system {g_src_sys_key}")
        return False
    #
    l_tmp_table1 = f"{g_db_name}.tmp1_{g_src_sys_key}_src_files"
    # 
    #spark.sql(f"drop table if exists {l_tmp_table1}")
    # 
    logInfo("Opening cursor to metadata repository connection")         
    #
    l_cur = g_pg_connection.cursor()
    #
    for src_entity_rec in l_src_entity_recs:
         logInfo(f"Processing source entity {src_entity_rec['src_entity_key']}")
         spark.sql(f"drop table if exists {l_tmp_table1}")
         collect_all_files(l_src_sys_rec, src_entity_rec)
         try:
            df_files = spark.sql(f"select * from {l_tmp_table1}")
         except Exception as err:
            continue   
         #         
         logInfo(f"{df_files.count()} files found for entity {src_entity_rec['src_entity_key']}")
         #
         
         l_cur.execute(f"""select max(src_entity_inst_no) 
                       from finops.elt_src_entity_load_hist 
                       where src_sys_key='{src_entity_rec["src_sys_key"]}' and 
                       src_entity_key='{src_entity_rec["src_entity_key"]}'""")
         l_src_inst_no = l_cur.fetchone()[0]
         if l_src_inst_no is None:
             l_src_inst_no = 0
         #
         l_ins_query = """insert into finops.elt_src_entity_load_hist(
            src_entity_inst_key,
            src_sys_key,
            src_entity_key,
            src_entity_inst_no,
            src_entity_path,
            src_entity_name,
            src_entity_timestamp,
            src_entity_size,
            src_entity_staged_time,
            src_entity_load_status,
            src_entity_load_mesg,
            src_entity_load_run_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
         for sr in df_files.collect():
            logInfo(f"entity instance key is {sr['src_entity_inst_key']}")
            #           
            #l_cur = g_pg_connection.cursor()
            l_cur.execute(f"""select 1 from finops.elt_src_entity_load_hist where src_entity_inst_key='{sr["src_entity_inst_key"]}'""")
            if (l_cur.rowcount == 0):
                l_src_inst_no = l_src_inst_no + 1
                l_cur.execute(l_ins_query,(sr["src_entity_inst_key"],
                                           sr["src_sys_key"],
                                           sr["src_entity_key"],
                                           l_src_inst_no,
                                           sr["src_entity_path"],
                                           sr["src_entity_name"],
                                           sr["src_entity_timestamp"],
                                           sr["src_entity_size"],
                                           sr["src_entity_staged_time"],
                                           "staged",
                                           "staged",
                                           str(uuid.uuid4())
                                           ))
    #    
    #
    l_cur.close()
    g_pg_connection.commit()
    g_pg_connection.close()
    #
    return True
#**************************************************************************************************** 
# Collect all the files available on the s3 bucket/path to find out new files
#****************************************************************************************************
def collect_all_files(p_src_sys_rec, p_src_entity_rec):
    #
    l_botoclient = boto3.client('s3')
    #    
    l_path_tokens = p_src_sys_rec["src_entity_path"].split("/")
    l_bucket_name = l_path_tokens[2]
    l_path_prefix = "/".join(l_path_tokens[3:])
    #
    l_entity_search_pattern = p_src_entity_rec["src_entity_pattern"]
    #
    if (l_entity_search_pattern.startswith("*")):
            l_src_entity_prefix = l_path_prefix + "/"
    elif ("*" in l_entity_search_pattern ):
            l_src_entity_prefix = l_path_prefix + "/" + l_entity_search_pattern[0:l_entity_search_pattern.index("*")]
    else:
            l_src_entity_prefix = l_path_prefix + "/" + l_entity_search_pattern
    #    
    l_file_filter  = l_path_prefix + "/" + l_entity_search_pattern
    logInfo(f"Searching in s3 bucket={l_bucket_name}, prefix={l_src_entity_prefix} for files {p_src_entity_rec['src_entity_pattern']}","both")
    # Create a paginator
    l_paginator = l_botoclient.get_paginator('list_objects_v2')
    # Paginate through the objects
    l_page_iterator = l_paginator.paginate(Bucket=l_bucket_name,Prefix=l_src_entity_prefix)
    # Filter and collect the file names
    l_tmp_table = f"{g_db_name}.tmp1_{g_src_sys_key}_src_files"
    for page in l_page_iterator:        
        if 'Contents' in page:
            l_entity_recs = [
                Row(
                    src_entity_inst_key = "{ftime}.{fsys}.{fname}".format(  
                                                    ftime = entity_s3_rec['LastModified'].strftime("%Y%m%d.%H%M%S.%f"),
                                                    fsys = p_src_sys_rec["src_sys_key"], 
                                                    fname = entity_s3_rec['Key'].split("/")[-1]) ,                                   
                    src_sys_key = p_src_entity_rec["src_sys_key"],
                    src_entity_key = p_src_entity_rec["src_entity_key"],
                    src_entity_path = p_src_sys_rec["src_entity_path"],
                    src_entity_name = entity_s3_rec['Key'].split("/")[-1],
                    src_entity_timestamp = entity_s3_rec['LastModified'],
                    src_entity_size = entity_s3_rec['Size'],
                    src_entity_load_status = "staged",
                    src_entity_load_mesg = "Staged",
                    src_entity_load_run_id = "{fsysid}.{fentid}.{fentinst}".format(
                                                fsysid = "{:05d}".format(p_src_sys_rec["src_sys_id"]),
                                                fentid = "{:05d}".format(p_src_entity_rec["src_entity_id"]),
                                                fentinst = "{:010d}".format(p_src_entity_rec["src_entity_id"])
                                                ),
                    src_sys_id = p_src_sys_rec["src_sys_id"],
                    src_entity_id = p_src_entity_rec["src_entity_id"],                                                                    
                    src_entity_staged_time = datetime.now())                     
                for entity_s3_rec in page['Contents'] if fnmatch.fnmatch(entity_s3_rec['Key'],l_file_filter)]
            #
            logInfo(f"Search returned {len(l_entity_recs)} file(s)")
            #
            if l_entity_recs:
                tmp_df = spark.createDataFrame(l_entity_recs)
                tmp_df.write.format("iceberg").mode("append").saveAsTable(l_tmp_table)
    #        
#**************************************************************************************************** 
# MAIN: Init, Validate and Load
#****************************************************************************************************
if __name__ == "__main__":
    global spark
    global g_logger
    global g_dbLogger
    try:
        g_logger = dvf.get_logger()        
        #
        spark = dvf.create_spark_session("Staging source files",g_logger)
        if not spark:
            dvf.stop_and_exit(None,1)
        # 
        if not init_environment():
            logCritical("Initializing environments failed.")
            dvf.stop_and_exit(spark,1)
        #
        if not get_config_variables():
            dvf.stop_and_exit(spark,1)
        #   
        logInfo(f"Initializing environment completed","both")                
        # 
        if not stage_new_source_files():
            dvf.stop_and_exit(spark,1)
        #
        dvf.stop_and_exit(spark,0)
    except Exception as err:
        #Handle any unhandled error
        logCritical(f"Unhandled exception occurred (main).")
        logCritical(f"Unexpected {err=}, {type(err)=}")
        traceback.print_exc()
        dvf.stop_and_exit(spark,1)
#endif    