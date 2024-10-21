from datetime import datetime
import sys
import traceback
import logging
from pyspark.sql import SparkSession, Row 
from pyspark.sql.functions import row_number, col
from pyspark.sql.types import NullType
import boto3
import fnmatch
import json
import re
from dvelt_loadlogger import LoadLogger
import dvelt_functions as dvf
import dvelt_mdvalidation as mdv
#*****************************************************************************************************
# Program : <dbname>_stage_source_files_v1.py
# Developed by : Lachhaman Moharana
# Purpose : Reading the s3 bucket for files of a source system and staging them for processing
# Functionality -
#   1. Scan the s3 bucket path for a source system
#   2. Read all the available files
#   3. With file timestamp.sourcesystem.filename as key look for a file if it is not staged
#   4. Take the file and make a database entry with status STAGED for next process to pick up
# Warning -
#   S3 does not allow for wild card scanning, so file names for wildcard naming should not be
#   specified like *.*, this will cause all files to be picked up. A max file scan limit of
#   99999 is set to have better performance. If files pile up faster, consider archiving the
#   the old files and move them away from processing folder 
#****************************************************************************************************
def logInfo(p_message,p_target = "file"):
    if (p_target == "both"):
        g_logger.info(p_message)
        g_dbLogger.Info(p_message)
    else:
        g_logger.info(p_message)
def logCritical(p_message,p_target="both"):
        g_logger.critical(p_message)
        if (p_target == "both"):
            g_dbLogger.Critical(p_message)
def logWarning(p_message,p_target="both"):
        g_logger.warn(p_message)
        if (p_target == "both"):
            g_dbLogger.Warning(p_message)
#****************************************************************************************************
# Initialize environment 
#****************************************************************************************************        
def init_environment():

    global g_config_name_with_path
    global g_config_name
    global g_config_path    
    # global g_src_sys_id
    global g_src_sys_key
    #
    l_spark_vars = {sv[0]:sv[1] for sv in spark.sparkContext.getConf().getAll() if sv[0] \
                    in ["spark.finopsENV.src_sys_key","spark.finopsENV.config_path","spark.finopsENV.config_name", \
                        "spark.finopsENV.pipeline_name"]}
    l_req_vars = [
        "spark.finopsENV.src_sys_key",
        "spark.finopsENV.config_name",
        "spark.finopsENV.config_path"
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
    global g_rep_db_name
    global g_elt_src_sys_load_def
    global g_elt_src_entity_load_def
    global g_elt_src_entity_load_hist
    global g_elt_src_entity_load_hist_arch
    global g_elt_run_messages
    global g_src_entity_path 

    try:
        l_jfile = open(g_config_name_with_path,"r")
        l_jobj = json.load(l_jfile)
        l_env = {l : l_jobj[l] for l in l_jobj}
        #
        l_req_env = mdv.get_src_load_metadata_tbl_list()
        #
        l_avl_env = dvf.check_values_present(l_req_env,l_env)
        #
        if (l_avl_env != "yes"):
            logInfo("Required environment variables " + ",".join(l_req_env) + f" not found {l_avl_env}" )
            return False
        #
        g_rep_db_name = mdv.get_rep_db_name(l_env)
        logInfo(f"Repository Database name => {g_rep_db_name}")
        #
        g_elt_src_sys_load_def = mdv.get_src_sys_load_def_tbl_name(l_env)
        logInfo(f"Source system load definition table => {g_elt_src_sys_load_def}")
        #
        g_elt_src_entity_load_def = mdv.get_src_entity_load_def_tbl_name(l_env)
        logInfo(f"Source file load definition table name => {g_elt_src_entity_load_def}")
        #
        g_elt_src_entity_load_hist = mdv.get_src_entity_load_hist_tbl_name(l_env)
        logInfo(f"Source file load history table name => {g_elt_src_entity_load_hist}")
        # 
        g_elt_run_messages = mdv.get_src_entity_load_log_tbl_name(l_env)
        logInfo(f"Source load log table name => {g_elt_run_messages}")
        #  
        g_elt_src_entity_load_hist_arch = mdv.get_src_orcl_load_hist_arch_tbl_name(l_env)                      
        return True    
    except Exception as err:    
        logCritical(f"Exception occurred while validating runtime parameters","file")
        logCritical(f"Unexpected {err=}, {type(err)=}","file")
        return False
#****************************************************************************************************
# Validate all the repository tables exist 
#****************************************************************************************************        
def validate_md_repository():
    global g_dbLogger
    g_dbLogger.rep_db_name = g_rep_db_name
    g_dbLogger.elt_src_sys_load_def = g_elt_src_sys_load_def
    g_dbLogger.elt_src_entity_load_def = g_elt_src_entity_load_def
    g_dbLogger.elt_src_entity_load_hist = g_elt_src_entity_load_hist
    g_dbLogger.elt_run_messages = g_elt_run_messages
    g_dbLogger.elt_src_entity_load_hist_arch = g_elt_src_entity_load_hist_arch
    #
    if not g_dbLogger.Validate_Repository():
        logCritical("One or more metadata repository tables for ELT do not exist")
        return False
    logInfo("Repository tables exist")
    return True
#**************************************************************************************************** 
# Get source entity/file details from repository database for the source system
#****************************************************************************************************
def get_source_entity_def(p_src_sys_key):
    l_sql_qry = f"""select * from {g_rep_db_name}.{g_elt_src_entity_load_def} 
                where src_sys_key='{p_src_sys_key}' and src_entity_status='active' """
    try:    
        l_src_entity_df = spark.sql(l_sql_qry)       
        logInfo(f"Found {l_src_entity_df.count()} active source definition records for {p_src_sys_key}","both")
        return l_src_entity_df.collect()                
    except:
        logCritical(f"Error occurred while fetching source entity information for {p_src_sys_key}")       
        traceback.print_exc()
        return None
#**************************************************************************************************** 
# Get source system details from repository database, it should have only one record for source sys
#****************************************************************************************************
def get_source_system_def(p_src_sys_key):
   # global g_dbLogger
    l_sql_qry = f"""select * from {g_rep_db_name}.{g_elt_src_sys_load_def} 
                where src_sys_key='{p_src_sys_key}'"""
    try:    
        l_src_sys_df = spark.sql(l_sql_qry)       
        if l_src_sys_df.count() == 0 or l_src_sys_df.count() > 1:
            logCritical(f"""Source system definition for source system
                         {g_src_sys_key} in table {g_rep_db_name}.{g_elt_src_sys_load_def} has 0 or more than 1 record.
                         Expected 1 found {l_src_sys_df.count}
                         """)
            return None
        #
        return l_src_sys_df.first()                
    except:
        g_logger.critical(f"Error occurred while fetching source system information for {g_src_sys_key}")
        g_logger.critical(l_sql_qry)
        traceback.print_exc()
        return None
#*************************** Build s3 path and prefix variables ***********************************
#**************************************************************************************************** 
# Stage the new files
#****************************************************************************************************
def stage_new_source_files():
    if not g_src_sys_key:
        return False
    #
    l_src_sys_rec = get_source_system_def(g_src_sys_key)
    if not l_src_sys_rec:
        return False
    #
    l_src_entity_recs = get_source_entity_def(g_src_sys_key)
    if not l_src_entity_recs:
        return False
    #
    l_tmp_table1 = f"{g_rep_db_name}.tmp1_{g_src_sys_key}_src_files"
    l_tmp_table2 = f"{g_rep_db_name}.tmp2_{g_src_sys_key}_src_files"
    spark.sql(f"drop table if exists {l_tmp_table1}")
    spark.sql(f"drop table if exists {l_tmp_table2}")    
    #
    for src_entity_rec in l_src_entity_recs:
         collect_all_files(l_src_sys_rec, src_entity_rec)  
    #    
    l_new_files_qry = f""" select 
                                src_entity_inst_key,                                   
                                src_sys_key,
                                nvl(src_entity_last_inst_no,0) + 
                                row_number() over(partition by src_entity_key order by src_entity_inst_key) as src_entity_inst_no,
                                src_entity_key,
                                src_entity_path,
                                src_entity_name,
                                src_entity_timestamp,
                                src_entity_size,
                                src_entity_load_status,
                                src_entity_load_mesg,
                                src_entity_load_run_id,
                                src_entity_staged_time,
                                src_sys_id,
                                src_entity_id  
                            from {l_tmp_table1}
                            where 
                                src_entity_inst_key not in (select src_entity_inst_key
                                from {g_rep_db_name}.{g_elt_src_entity_load_hist}
                                where src_sys_key = '{l_src_sys_rec["src_sys_key"]}')
                        """
    l_tmp1_df = spark.sql(l_new_files_qry)
    l_tmp2_rows = [Row(
                        src_entity_inst_key = tr['src_entity_inst_key'],                                   
                        src_sys_key = tr['src_sys_key'],
                        src_entity_inst_no = tr['src_entity_inst_no'],
                        src_entity_key = tr['src_entity_key'],
                        src_entity_path = tr['src_entity_path'],
                        src_entity_name = tr['src_entity_name'],
                        src_entity_timestamp = tr['src_entity_timestamp'],
                        src_entity_size = tr['src_entity_size'],
                        src_entity_load_status = tr['src_entity_load_status'],
                        src_entity_load_mesg = tr['src_entity_load_mesg'],
                        src_entity_load_run_id = "{fsysid}.{fentid}.{fentinst}".format(
                                                fsysid = "{:05d}".format(tr["src_sys_id"]),
                                                fentid = "{:05d}".format(tr["src_entity_id"]),
                                                fentinst = "{:010d}".format(tr["src_entity_inst_no"])
                                                ),
                        src_entity_staged_time = tr['src_entity_staged_time']
                      )
                   for tr in l_tmp1_df.collect()]
    #     
    if (len(l_tmp2_rows) == 0):
        logInfo(f"No new files were found for source system {l_src_sys_rec['src_sys_key']}","both")
        return True
    #     
    l_tmp2_df = spark.createDataFrame(l_tmp2_rows)
    l_tmp2_df.write.format("iceberg").mode("overwrite").saveAsTable(l_tmp_table2) 
    #
    logInfo(f"{l_tmp2_df.count()} new files were staged for source system {l_src_sys_rec['src_sys_key']}","both")
    #
    l_ins_sql = f"""
                insert into {g_rep_db_name}.{g_elt_src_entity_load_hist}
                (src_entity_inst_key, src_sys_key, src_entity_key, src_entity_name, src_entity_timestamp,
                src_entity_size, src_entity_staged_time, src_entity_load_status, src_entity_load_mesg,
                src_entity_inst_no,src_entity_path,src_entity_load_run_id)
                select 
                    src_entity_inst_key,src_sys_key,src_entity_key,src_entity_name,src_entity_timestamp,src_entity_size,
                    src_entity_staged_time,src_entity_load_status,src_entity_load_mesg,
                    src_entity_inst_no,src_entity_path,src_entity_load_run_id
                from {l_tmp_table2}    
                """              
    spark.sql(l_ins_sql)
    df_max = l_tmp2_df.groupby('src_sys_key','src_entity_key').max('src_entity_inst_no').withColumnRenamed('max(src_entity_inst_no)','max_src_entity_inst_no')
    #
    for xr in df_max.collect():
        spark.sql(f"""update {g_rep_db_name}.{g_elt_src_entity_load_def} 
                      set src_entity_last_inst_no = {xr['max_src_entity_inst_no']}
                      where src_sys_key = '{xr["src_sys_key"]}' and src_entity_key = '{xr["src_entity_key"]}'
                  """)
    #
    logInfo(f"Staging files for source system {l_src_sys_rec['src_sys_key']} completed successfully","both")
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
    l_tmp_table = f"{g_rep_db_name}.tmp1_{g_src_sys_key}_src_files"
    for page in l_page_iterator:        
        if 'Contents' in page:
            l_entity_recs = [
                Row(
                    src_entity_inst_key = "{ftime}.{fsys}.{fname}".format(  
                                                    ftime = entity_s3_rec['LastModified'].strftime("%Y%m%d.%H%M%S.%f"),
                                                    fsys = p_src_sys_rec["src_sys_key"], 
                                                    fname = p_src_entity_rec["src_entity_key"]) ,                                   
                    src_sys_key = p_src_entity_rec["src_sys_key"],
                    src_entity_last_inst_no = p_src_entity_rec["src_entity_last_inst_no"] ,
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
        g_dbLogger = LoadLogger()

        spark = dvf.create_spark_session("Staging source files",g_logger)
        if not spark:
            dvf.stop_and_exit(None,1)
        # 
        g_dbLogger.sparksession = spark
        #               
        if not init_environment():
            g_logger.Critical("Initializing environments failed.")
            dvf.stop_and_exit(spark,1)
        #
        if not get_config_variables():
            dvf.stop_and_exit(spark,1)
        #
        if not validate_md_repository():
            dvf.stop_and_exit(spark,1)  
        #      
        g_dbLogger.process_run_id = f"{g_src_sys_key}.s3_files_staging"
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