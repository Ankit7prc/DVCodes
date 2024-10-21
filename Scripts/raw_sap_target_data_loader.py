import sys,os,uuid,json,traceback,inspect
#current_dir = os.path.dirname(os.path.abspath(__file__))
#common_script_dir = current_dir.replace("tl","common")
#sys.path.insert(0, common_script_dir)
#
import dvelt_functions as dvf
import dvelt_nabujobaudit as nja
#import ...common.scripts.dvelt_functions as dvf
#import ...common.scripts.dvelt_nabujobaudit as nja
#from common.scripts.dvelt_metadatamgr import MetadataManager
from dvelt_metadatamgr import MetadataManager
from pyspark.sql.functions import sha2, concat_ws, when, col 
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField
#**************************************************************************************************
# Script/Module Name - raw_sap_target_data_loader.py
# Purpose - Builds source data and updates it on the basis of configuration setup
# Change History:
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Lachhaman Moharana  -           Initial version
#
#**************************************************************************************************
# Runtime Parameters, passed through spark submit
#  1) spark.finopsENV.load_id - Load id defined in the configuration table
#  2) spark.finopsENV.environment - Envornment - Development/Validation/Production
#  3) spark.finopsENV.config_name  - Environment configuration json file
#  4) spark.finopsENV.config_path  - Environment configuration json file path
#==================================================================================================
#************************************************************************* Global Variables
spark = None
g_logger = dvf.get_logger()
g_dvaudit = nja.NabuJobAudit()
g_load_id:str = None
g_env_name:str = None
g_metadata_db:str = None
g_config_name_with_path:str = None
#g_load_cfg_df = None
g_load_cfg_row:Row = None
g_src_priority_ord_df = None
g_src_col_priority_ord_df = None

#****************************************************************************************************
# Shorthands for logging
#****************************************************************************************************
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
 # 
#****************************************************************************************************
# Initialize Environment
#****************************************************************************************************
def init_environment()->bool:
    global g_env_name
    global g_load_id
    global g_metadata_db
    global g_config_name_with_path
    #
    logInfo("Checking for required runtime parameter values")
    #
    l_req_vars = [
        "spark.finopsENV.load_id",
        "spark.finopsENV.metadata_db",
        "spark.finopsENV.environment",
        "spark.finopsENV.config_path",
        "spark.finopsENV.config_name"
        ]
    #
    l_spark_vars = {sv[0]:sv[1] for sv in spark.sparkContext.getConf().getAll()}
    #
    l_vars_present = dvf.check_values_present(l_req_vars,l_spark_vars)    
    #
    if l_vars_present != "yes":
        logInfo(f"Required spark runtime variables "+ ",".join(l_req_vars) +f" not found {l_vars_present}")
        return False
    #
    logInfo("Required parameters found, assigning them to respective variables")
    #
    g_load_id = l_spark_vars["spark.finopsENV.load_id"]
    g_env_name = l_spark_vars["spark.finopsENV.environment"]
    g_metadata_db = l_spark_vars["spark.finopsENV.metadata_db"]
    #
    if l_spark_vars["spark.finopsENV.config_path"][-1] == "/":
        g_config_name_with_path = l_spark_vars["spark.finopsENV.config_path"] + l_spark_vars["spark.finopsENV.config_name"] 
    else:
            g_config_name_with_path = l_spark_vars["spark.finopsENV.config_path"] + "/" + l_spark_vars["spark.finopsENV.config_name"]     
    #
    if not dvf.is_valid_file(g_config_name_with_path):
        logCritical(f"{g_config_name_with_path} does not exist or not valid file")
        return False
    #
    logInfo(f"Load id -> {g_load_id}")
    logInfo(f"Environment -> {g_env_name}")
    logInfo(f"Metadata Database Name -> {g_metadata_db}")
    logInfo(f"Environment Configuration File -> {g_config_name_with_path}")
    #
    return True
#**************************************************************************************************
# Get configuration variables
#**************************************************************************************************
def get_config_variables()->bool:
    #
    #global g_load_cfg_df
    global g_load_cfg_row
    #
    global g_dvaudit
    global g_src_connection_name
    global g_driver_name
    global g_connection_url
    #
    try:
        logInfo("Reading/assigning source configuration variables")
        l_jfile = open(g_config_name_with_path,"r")
        l_json_obj = json.load(l_jfile)
        #
        l_env_json = l_json_obj.get(g_env_name)
        #
        g_src_connection_name = l_env_json.get("SourceConnectionName")
        g_driver_name = l_env_json.get("SourceJdbcDriverName")
        g_connection_url = l_env_json.get("SourceJdbcUrl")
        #
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
        logInfo("Fetching Nabu audit credential through Nabu API service")
        l_resp = dvf.get_credential_from_nabu(spark,l_aud_connection_name)
        if (l_resp.status_code != 200):
            logCritical(f"Unable to retrieve password from nabu for audit connection {l_aud_connection_name}, error -{l_resp.reason}")
            return False
        #
        l_respjson = json.loads(l_resp.text)
        g_dvaudit.AuditDatabaseUser = l_respjson.get("data").get("username")
        g_dvaudit.AuditDatabasePassword =  l_respjson.get("data").get("password")      
        #
        l_jfile.close()
        #
        l_mdmgr = MetadataManager()
        l_mdmgr.MetdataDbName = l_env_json.get("NabuAuditConnection").get("EltDatabaseName")
        l_mdmgr.MetdataDbHost = l_env_json.get("NabuAuditConnection").get("DatabaseHostName")
        l_mdmgr.MetdataDbPort = l_env_json.get("NabuAuditConnection").get("DatabasePortNo") 
        l_mdmgr.MetdataDbUsername = l_respjson.get("data").get("username")
        l_mdmgr.MetdataDbPassword =  l_respjson.get("data").get("password")   
        # Get Load configuration record    
        logInfo("Fetching load configuration data from metadata tables")          
        #l_load_cfg_df = spark.sql(f"select * from {g_metadata_db}.elt_data_load_cfg where load_id ='{g_load_id}'")
        #g_load_cfg_row = l_load_cfg_df.first()
        g_load_cfg_row = l_mdmgr.GetLoadConfigRecord(g_load_id)
        if g_load_cfg_row is None:
            logCritical(f"Data load configuration entry not found for load id -> {g_load_id}, process failed")
            return False
        #
        # if l_load_cfg_df.count() > 1:
        #     logCritical(f"Duplicate load configuration entry found for load id -> {g_load_id}, process failed")
        #     return False
        #
        logInfo("Finished fetching/assignment configuration variables")
        return True    
    except Exception as err:  
        logException(inspect.currentframe(),err,"Exception occurred while fetching/assigning configuration variables")  
        return False
#**************************************************************************************************
# Validation Metadta configuration
#**************************************************************************************************
def validate_metadata_config()->bool:         
    #
    if g_load_cfg_row['src_type'] not in ["query","table"]:
        logCritical(f"Source type should be query or table {g_load_cfg_row['src_type']}")
        return False
    #
    if g_load_cfg_row['src_qry_text'] is None and g_load_cfg_row['src_type'] == "query":
        logCritical(f"Source query text is not specified, process failed")
        return False
    #
    if (g_load_cfg_row['src_tbl_name'] is None or g_load_cfg_row['src_db_name'] is None) and g_load_cfg_row['src_type'] == "table":
        logCritical(f"Source database/table name not specified, process failed")
        return False
    #
    if g_load_cfg_row['src_type'] == "table" and not spark.catalog.tableExists(g_load_cfg_row["src_tbl_name"],g_load_cfg_row["src_db_name"]):
        logCritical(f"Source table {g_load_cfg_row['src_db_name']}.{g_load_cfg_row['src_tbl_name']} does not exist, process failed")
        return False
    if not spark.catalog.tableExists(g_load_cfg_row["tgt_tbl_name"],g_load_cfg_row["tgt_db_name"]):
        logCritical(f"""Target Table {g_load_cfg_row["tgt_db_name"]}.{g_load_cfg_row["tgt_tbl_name"]} does not exist, process failed""")
        return False
    #         
    if g_load_cfg_row["aud_cols_json"] is None:  
        logCritical(f"Audit columns are not specified, process failed")
        return False  
    #
    if g_load_cfg_row["key_cols_json"] is None:  
        logCritical(f"Key columns not specified, process failed") 
        return False        
    #
    if g_load_cfg_row["tgt_upd_strategy"] is None:  
        logCritical(f"Target update strategy not specified, process failed") 
        return False        
    #
    if g_load_cfg_row["identify_src_delete"] is None:  
        logCritical(f"Identify source delete value not specified, process failed") 
        return False        
    if g_load_cfg_row["identify_src_delete"] == "yes" and g_load_cfg_row['tgt_del_flag_col_name'] is None:
        logCritical(f"Target deletion flag column name not, process failed")
    #
    return True
#**************************************************************************************************
# Validate data table structure, keys etc.
#**************************************************************************************************
def validate_data_tables_structure()->bool:
    try:
        logInfo("Validating data table structure for the load")
        #g_load_cfg_row = g_load_cfg_df.first()
        l_tgt_tbl_df = spark.sql(f"describe table {g_load_cfg_row['tgt_db_name']}.{g_load_cfg_row['tgt_tbl_name']}")
        l_tgt_col_list = [f["col_name"] for f in l_tgt_tbl_df.collect()]
        #
        l_req_aud_columns = get_column_list(g_load_cfg_row['aud_cols_json'])
        #
        if not all(colname in l_tgt_col_list for colname in l_req_aud_columns):
            logInfo(f"Target table name ->{g_load_cfg_row['tgt_db_name']}.{g_load_cfg_row['tgt_tbl_name']}")
            logInfo(f"Targed column list {l_tgt_col_list}")
            logInfo(f"Required audit column list {l_req_aud_columns}")
            logCritical("All the required audit columns are not present in the target table")
            return False
        #
        l_key_cols = get_column_list(g_load_cfg_row['key_cols_json'])
        if not all(colname in l_tgt_col_list for colname in l_key_cols):
            logInfo(f"Target table name ->{g_load_cfg_row['tgt_db_name']}.{g_load_cfg_row['tgt_tbl_name']}")
            logInfo(f"Targed column list {l_tgt_col_list}")
            logCritical(f"Target must have all key columns present key columns -> {l_key_cols}")
            return False
        #
        if g_load_cfg_row['identify_src_delete'] == "yes" and g_load_cfg_row['tgt_del_flag_col_name'] not in l_tgt_col_list:
            logCritical(f"Column name {g_load_cfg_row['tgt_del_flag_col_name']} identifying deleted record flag is not present on target table, process failed")
            return False
        logInfo("Validating data table structure, key and audit column requirements completed")
        #
        return True
    except Exception as err:
        logException(inspect.currentframe(),err,"Exception occurred while validating table structures")  
        return False    
#**************************************************************************************************
# Helper functions
#**************************************************************************************************
def get_column_list(p_json_text:str)->list:
    l_jo = json.loads(p_json_text)
    return [l_jo[key] for key in l_jo]
#**************************************************************************************************
# Merge Changed/New Records to Target table
#**************************************************************************************************
def merge_source_to_target()->bool:
    #
    l_tgt_tbl_name = f"{g_load_cfg_row['tgt_db_name']}.{g_load_cfg_row['tgt_tbl_name']}"
    #l_tmp_tbl_name = f"{g_load_cfg_row['tgt_db_name']}.tmp1_{g_load_cfg_row['tgt_tbl_name']}"
    l_tmp_tbl_name = f"global_temp.tmp1_{g_load_cfg_row['tgt_tbl_name']}"
    #
    l_key_cols = get_column_list(g_load_cfg_row['key_cols_json'])
    l_aud_cols = get_column_list(g_load_cfg_row['aud_cols_json'])
    l_tgt_df = spark.sql(f"describe table {l_tgt_tbl_name}")
    l_all_tgt_col_list = [dc["col_name"] for dc in l_tgt_df.collect() if (dc["col_name"] not in l_aud_cols)]
    l_data_col_list = [dc["col_name"] for dc in l_tgt_df.collect() if (dc["col_name"] not in l_key_cols and dc["col_name"] not in l_aud_cols)]
    #
    if g_load_cfg_row['identify_src_delete'] == "yes":        
        l_all_tgt_col_list.remove(g_load_cfg_row["tgt_del_flag_col_name"])
        l_data_col_list.remove(g_load_cfg_row["tgt_del_flag_col_name"])
    #
    l_jc = " and ".join([f"src.{colm} = tgt.{colm}" for colm in l_key_cols])
    #
    l_aud_col_json = json.loads(g_load_cfg_row['aud_cols_json'])
    #
    l_set_clause = ",".join([f"tgt.{col_name} = src.{col_name}" for col_name in l_data_col_list])
    #
    l_ins_tgt_columns = ",".join(l_all_tgt_col_list)
    l_ins_src_columns = "src." + ",src.".join(l_all_tgt_col_list)
    #
    l_ins_tgt_columns += ",{}".format(l_aud_col_json["RecordInsertDate"])
    l_ins_src_columns += ",current_timestamp()"
    #
    l_ins_tgt_columns += ",{}".format(l_aud_col_json["RecordInsertBy"])
    l_ins_src_columns += f",'{g_load_id}'"
    #
    l_ins_tgt_columns += ",{}".format(l_aud_col_json["RecordUpdateDate"])
    l_ins_src_columns += ",null"
    #
    l_ins_tgt_columns += ",{}".format(l_aud_col_json["RecordUpdateBy"])
    l_ins_src_columns += ",null"
    #
    if g_load_cfg_row['identify_src_delete'] == "yes":
        l_ins_tgt_columns += ",{}".format(g_load_cfg_row['tgt_del_flag_col_name'])
        l_ins_src_columns += ",'N'"
    #
    l_set_clause += ",{col1} = current_timestamp()".format(col1=l_aud_col_json["RecordUpdateDate"])
    l_set_clause += ",{col1} = '{val1}'".format(col1=l_aud_col_json["RecordUpdateBy"],val1=g_load_id)
    #
    if (g_load_cfg_row["pre_tgt_load_sql"]) is not None:
        logInfo(f"Executing pre target load sql")
        l_pre_sql = g_load_cfg_row["pre_tgt_load_sql"]
        l_pre_sql = l_pre_sql.replace("<<source>>",l_tmp_tbl_name)
        l_pre_sql = l_pre_sql.replace("<<target>>",l_tgt_tbl_name)
        logInfo("Executing SQL ->\n{}".format(l_pre_sql))
        spark.sql(l_pre_sql)
    #      
    l_merge_sql = f"""
        MERGE INTO {l_tgt_tbl_name} AS tgt USING {l_tmp_tbl_name} AS src
        ON ({l_jc})
        WHEN MATCHED AND record_status = 'changed' THEN
            UPDATE SET
                {l_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({l_ins_tgt_columns})
            VALUES ({l_ins_src_columns})             
        """
    #
    logInfo("Executing merge query")
    print(l_merge_sql)
    #
    spark.sql(l_merge_sql)
    #             
    logInfo("Merge completed successfully")
    if g_load_cfg_row['identify_src_delete'] == "yes":
        logInfo("Identifying source deleted records and updating target by comparing source and target on the basis of key")
        l_prev_del_count = spark.sql(f"""select count(*) from {l_tgt_tbl_name} where {g_load_cfg_row['tgt_del_flag_col_name']} ='Y'""").first()[0]
        l_upd_sql = f"""update {l_tgt_tbl_name} tgt 
        set {g_load_cfg_row["tgt_del_flag_col_name"]}='Y', 
        {l_aud_col_json["RecordUpdateDate"]} = current_timestamp(),
        {l_aud_col_json["RecordUpdateBy"]} = '{g_load_id}' 
        where 
        not exists (select 1 from {l_tmp_tbl_name} src where {l_jc})
        """
        l_tmp_count = spark.sql(f"select count(*) from {l_tmp_tbl_name}").first()[0]
        logInfo(f"Temp table has {l_tmp_tbl_name} records before executing update {l_upd_sql}")
        logInfo("Executing query->\n{}".format(l_upd_sql))
        spark.sql(l_upd_sql)
        l_curr_del_count = spark.sql(f"""select count(*) from {l_tgt_tbl_name} where {g_load_cfg_row['tgt_del_flag_col_name']} ='Y'""").first()[0]
        logInfo(f"Number of records soft deleted {l_curr_del_count - l_prev_del_count}")
    #   
    if (g_load_cfg_row["post_tgt_load_sql"]) is not None:
        logInfo(f"Executing post target load sql")
        l_post_sql = g_load_cfg_row["post_tgt_load_sql"]
        l_post_sql = l_post_sql.replace("<<source>>",l_tmp_tbl_name)
        l_post_sql = l_post_sql.replace("<<target>>",l_tgt_tbl_name)
        logInfo("Executing SQL ->\n{}".format(l_post_sql))
        spark.sql(l_post_sql)
    #logInfo(f"Dropping temporary table {l_tmp_tbl_name}")
    #spark.sql(f"drop table if exists {l_tmp_tbl_name}")
    return True
#**************************************************************************************************
# Insert/append Records
#**************************************************************************************************
def insert_source_to_target()->bool:
    global g_dvaudit
    try:
        #
        logInfo("Preparing to insert data from source to target")        
        l_aud_cols = get_column_list(g_load_cfg_row['aud_cols_json'])
        l_tgt_tbl_name = f"{g_load_cfg_row['tgt_db_name']}.{g_load_cfg_row['tgt_tbl_name']}"
        l_tgt_df = spark.sql(f"select * from {l_tgt_tbl_name} where 1=2")
        l_tgt_schema = {tc.name : StructField(tc.name,tc.dataType,tc.nullable) for tc in l_tgt_df.schema.fields if tc.name not in l_aud_cols}
        logInfo(f"Target table schema->\n")
        print(l_tgt_schema)
        if g_load_cfg_row['src_type'] == "query":
            logInfo("Source type is query preparing dataset for insert operation")
            l_src_sql = g_load_cfg_row['src_qry_text']
            l_src_df = spark.sql(l_src_sql)
            l_src_cols = [f.name.lower() for f in l_src_df.schema.fields]
            #
            l_src_df.printSchema()
            #
            l_src_schema = StructType([l_tgt_schema[fname] for fname in l_src_cols])
            print(l_src_schema)
            #
            logInfo("Applying target schema to source dataset")
            l_src_df = spark.createDataFrame(l_src_df.rdd,l_src_schema)
            logInfo("Applied target schema to source dataset")
            l_src_df.printSchema()
            logInfo("Building temporary dataset from the query/data frame")
            l_src_df.createOrReplaceGlobalTempView(g_load_cfg_row['tgt_tbl_name'])
            l_src_tbl_name = f"global_temp.{g_load_cfg_row['tgt_tbl_name']}"
        else:
            l_src_md_df = spark.sql(f"describe table {g_load_cfg_row['src_db_name']}.{g_load_cfg_row['src_tbl_name']}")
            l_src_cols = [f['col_name'].lower() for f in l_src_md_df.collect()]   
            l_src_tbl_name = f"{g_load_cfg_row['src_db_name']}.{g_load_cfg_row['src_tbl_name']}"
        #             
        l_tgt_cols = [f.name for f in l_tgt_df.schema.fields]
        l_tgt_col_count = len(l_tgt_cols)        
        #
        for ac in l_aud_cols:
            if ac in l_src_cols:
                l_src_cols.remove(ac)
            if ac in l_tgt_cols:
                l_tgt_cols.remove(ac)
        #
        logInfo("Source columns ->{}".format(",".join(l_src_cols)))
        logInfo("Target columns ->{}".format(",".join(l_tgt_cols)))
        #
        if len(l_src_cols) != len(l_tgt_cols):
            logCritical("Source and target columns mismatch, process failed")
            return False
        #
        if not all(lkey in l_tgt_cols for lkey in l_src_cols):
            logCritical("Source and target columns mismatch, process failed")
            return False
        #
        if len(l_src_cols) + 4 != l_tgt_col_count:
            logCritical("Target columns should be source plus 4 audit columns, mismatch found, process failed")
            return False
        #        
        if g_load_cfg_row['pre_tgt_load_sql'] is not None:
            l_pre_sql = g_load_cfg_row['pre_tgt_load_sql']
            l_pre_sql = l_pre_sql.replace('<<source>>',l_src_tbl_name)
            l_pre_sql = l_pre_sql.replace('<<target>>',l_tgt_tbl_name)
            logInfo(f"Executing the pre load sql ->\n {l_pre_sql} \n")
            spark.sql(l_pre_sql)
        #
        l_audjon = json.loads(g_load_cfg_row['aud_cols_json'])
        logInfo("Preparing insert statement")
        l_ins_cols = ",".join(l_src_cols)
        l_ins_cols += ","+l_audjon['RecordInsertDate']
        l_ins_cols += ","+l_audjon['RecordInsertBy']
        l_ins_cols += ","+l_audjon['RecordUpdateDate']
        l_ins_cols += ","+l_audjon['RecordUpdateBy']
        #
        l_ins_sel = ",".join(l_src_cols)
        l_ins_sel += f",current_timestamp(),'{g_load_id}',null,null"
        # 
        l_pre_count = spark.sql(f"select count(*) from {l_tgt_tbl_name}").first()[0]
        l_ins_sql = f"insert into {l_tgt_tbl_name}({l_ins_cols}) select {l_ins_sel} from {l_src_tbl_name}"
        logInfo("Executing the following insert statement \n{}\n".format(l_ins_sql))
        spark.sql(l_ins_sql)
        l_post_count = spark.sql(f"select count(*) from {l_tgt_tbl_name}").first()[0]
        logInfo(f"Number of records affected {l_post_count - l_pre_count}")
        g_dvaudit.incremental_data_count = l_post_count - l_pre_count
        g_dvaudit.total_data_count = l_post_count - l_pre_count
        #
        if g_load_cfg_row['post_tgt_load_sql'] is not None:
            l_post_sql = g_load_cfg_row['post_tgt_load_sql']
            l_post_sql = l_post_sql.replace('<<source>>',l_src_tbl_name)
            l_post_sql = l_post_sql.replace('<<target>>',l_tgt_tbl_name)
            logInfo(f"Executing the post load sql -> {l_post_sql}")
            spark.sql(l_post_sql)
        #
        return True
        #     
    except Exception as err:
        logException(inspect.currentframe(),err,"Exception occurred while inserting source to target")  
        return False
    #
    return True
#**************************************************************************************************
# Delete Records
#**************************************************************************************************
def delete_target()->bool:
    logInfo(f"Deletion is not implemented, but it can be achieved through pre/post target load configuration")
    return True
#**************************************************************************************************
# Build target dataset
#**************************************************************************************************
def build_target_dataset()->bool:
    g_dvaudit.TargetDatabaseName = g_load_cfg_row['tgt_db_name']
    g_dvaudit.TargetTableName = g_load_cfg_row['tgt_tbl_name']
    g_dvaudit.JobID = uuid.uuid4()
    g_dvaudit.JobName = g_load_id
    #
    g_dvaudit.JobStarted()
    #
    if g_load_cfg_row["tgt_upd_strategy"] == "merge":
        if not build_source_dataset():
            g_dvaudit.LastMessage = "Unable to build source dataset"
            g_dvaudit.JobFailed()
            return False
        if not merge_source_to_target():
            g_dvaudit.LastMessage = "Unable to merge source dataset with target"
            g_dvaudit.JobFailed()
            return False        
    elif g_load_cfg_row["tgt_upd_strategy"] == "insert":
        if not insert_source_to_target():
            g_dvaudit.LastMessage = "Unable to insert source data into target"
            g_dvaudit.JobFailed()
            return False
    elif g_load_cfg_row["tgt_upd_strategy"] == "delete":
        if not delete_target():
            g_dvaudit.LastMessage = "Unable to delete data from target"
            g_dvaudit.JobFailed()
            return False
    else:
        logInfo(f"Target update strategy {g_load_cfg_row['tgt_upd_strategy']} is not implemented yet, process skipped") 
    #endif
    g_dvaudit.JobCompleted()
    logInfo("Job Completed, analyzing target table")
    g_dvaudit.AnalyzeTarget(spark)
    return True
    #        
#**************************************************************************************************
# Build master records and merge to main table if records have changed/new records found
#**************************************************************************************************
def build_source_dataset()->bool:
    global g_dvaudit
    try:    
    #   
        #
        logInfo("Started process of building source dataset")
        l_tgt_aud_cols = get_column_list(g_load_cfg_row["aud_cols_json"])
        logInfo(f"Target audit columns -> {l_tgt_aud_cols}")
        l_tgt_key_cols = get_column_list(g_load_cfg_row["key_cols_json"])
        logInfo(f"Target key columns -> {l_tgt_key_cols}")
        #
        logInfo("Building source DataFrame using the query->\n {} \n".format(g_load_cfg_row['src_qry_text']))
        #
        l_tgt_tbl_name = g_load_cfg_row['tgt_db_name'] + '.' + g_load_cfg_row['tgt_tbl_name']
        #
        l_src_df = spark.sql(g_load_cfg_row['src_qry_text'])
        logInfo("Completed building source DataFrame")
        #
        l_src_col_list = [f.name.lower() for f in l_src_df.schema.fields]
        #
        l_tgt_df = spark.sql(f"select * from {l_tgt_tbl_name}")
        #
        logInfo("Applying target data type for each column to the source DataFrame")
        l_tgt_col_dict = {tc.name.lower():StructField(tc.name,tc.dataType,tc.nullable) for tc in l_tgt_df.schema.fields}
        #
        logInfo("\nTarget Schema")
        l_tgt_df.printSchema()
        #
        logInfo("\nDefault Source Schema")
        l_src_df.printSchema()
        #
        logInfo("\nSource dataframe fields ->\n{}".format("\n".join(l_src_col_list)))
        logInfo("\nTarget dataframe fields ->\n{}".format("\n".join(l_tgt_col_dict.keys())))
        #
        l_src_schema = StructType([l_tgt_col_dict[skey] for skey in l_src_col_list])
        l_src_df = spark.createDataFrame(l_src_df.rdd,l_src_schema)
        #
        logInfo("\nSource schema after applying target data types")
        l_src_df.printSchema()
        #
        l_tgt_col_list = [tkey for tkey in l_tgt_col_dict.keys() if tkey not in l_tgt_aud_cols]
        #
        if g_load_cfg_row['identify_src_delete'] == "yes" and g_load_cfg_row['tgt_del_flag_col_name'] in l_tgt_col_list:
            l_tgt_col_list.remove(g_load_cfg_row['tgt_del_flag_col_name'])

        l_tgt_data_col_list = [colm for colm in l_tgt_col_list if colm not in l_tgt_key_cols]
        l_tgt_data_col_concat_expr = [col(f"{colm}").cast("string") for colm in l_tgt_data_col_list]
        #
        #if not all(cname in l_src_col_list for cname in l_tgt_col_list):
        if (sorted(l_src_col_list) != sorted(l_tgt_col_list)):
            logInfo("Source columns list ->{}\n".format(",".join(sorted(l_src_col_list))))
            logInfo("Target columns list ->{}\n".format(",".join(sorted(l_tgt_col_list))))
            logCritical("All target columns must be present in the source data, process failed")
            return False
        #
        l_tgt_df = spark.sql(f"select * from {l_tgt_tbl_name}")
        #
        logInfo("Data Columns List ->{}".format(l_tgt_data_col_list))
        #
        logInfo("Concatenated columns used for SHA2 hash key calculation")
        #
        logInfo("Adding source SHA2 key to the source DataFrame")  
        l_src_df = l_src_df.withColumn("src_sha2_key", sha2(concat_ws("-", *l_tgt_data_col_concat_expr), 512))
        #      
        logInfo("Adding target SHA2 key to the target DataFrame")  
        l_tgt_df = l_tgt_df.withColumn("tgt_sha2_key", sha2(concat_ws("-", *l_tgt_data_col_concat_expr), 512))
        #
        logInfo("Building final source DataFrame")  
        l_final_src_df = l_src_df.join(l_tgt_df,on=l_tgt_key_cols,how="leftouter").select(l_src_df["*"],l_tgt_df["tgt_sha2_key"])
        logInfo("Adding record status column to be used for determining new/changed records")
        l_final_src_df = l_final_src_df.withColumn(
            "record_status",
            when(col("tgt_sha2_key").isNull(), "new")
            .when(col("tgt_sha2_key") != col("src_sha2_key"), "changed")            
            .otherwise("unchanged")
        )
        #
        l_final_count = l_final_src_df.count()
        #
        logInfo("Completed building new DataFrame with target data types")
        l_final_src_df.printSchema()
        #
        l_final_count1 = l_final_src_df.count()
        # 
        if l_final_count != l_final_count1:
            logWarning(f"Data contains null values for keyfields and are filtered out rec count-> {l_final_count - l_final_count1}")
        #     
        l_tmp_tbl_name = f"tmp1_{g_load_cfg_row['tgt_tbl_name']}"
        logInfo(f"Creating temporary table {l_tmp_tbl_name} with source data")
        #
        l_final_src_df.createOrReplaceGlobalTempView(l_tmp_tbl_name)
        #
        l_df_sum = l_final_src_df.groupBy("record_status").count()
        g_dvaudit.incremental_data_count = 0
        g_dvaudit.total_data_count = 0
        for r in l_df_sum.collect():
            g_dvaudit.incremental_data_count += r["count"] if r["record_status"] in ["new","changed"] else 0
            g_dvaudit.total_data_count += r["count"] if r["record_status"] in ["new","changed"] else 0
            print("Record Status %20s Record Count %10d" %(r["record_status"],r["count"]))
        #
        return True
    #
    except Exception as err:
        logException(inspect.currentframe(),err,"Exception occurred while building source dataset")  
        traceback.print_exc()
        return False  
#**************************************************************************************************** 
# MAIN: Init, Validate and Load
#****************************************************************************************************
if __name__ == "__main__":
    try:        
        #
        spark = dvf.create_spark_session("Building Master Record using configurable framework",g_logger) 
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
        logInfo(f"Completed fetching/assigning environment and configuration variables")
        # 
        if not validate_metadata_config():
            dvf.stop_and_exit(spark,1) 
        #             
        logInfo(f"Completed validating metadata setup")
        #
        if not validate_data_tables_structure():
            dvf.stop_and_exit(spark,1)
        logInfo("Completed validating data table structures successfully")
        #
        # if not build_source_dataset():
        #     dvf.stop_and_exit(spark,1) 
        # logInfo(f"Completed building source dataset for load id {g_load_id}")     
        #             
        if not build_target_dataset():
            logCritical("MAIN:Processing failed")
            dvf.stop_and_exit(spark,1)               
        #Happy Ending!
        logInfo("MAIN:Processing completed successfully")
        dvf.stop_and_exit(spark,0)
    except Exception as err:
        #Handle any unhandled error
        logException(inspect.currentframe(),err,"MAIN:Unhanlded exception")  
        dvf.stop_and_exit(spark,1)
#endif        
