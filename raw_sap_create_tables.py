import os
import logging
import json
import sys
from pyspark.sql import SparkSession
import dvelt_functions as dvf
import dvelt_mdvalidation as mdv
import dvelt_nabujobaudit as nja
import traceback
import inspect
#
global spark
global g_ddl_script_name_with_path
global g_ddl_script_type
global g_ddl_script_path
global g_env_name
#
g_logger:logging.Logger = dvf.get_logger()
#g_dvaudit:nja.NabuJobAudit = nja.NabuJobAudit()

#**************************************************************************************************
# Script/Module Name - raw_sap_create_tables.py
# Purpose - Read the Metadata definition from a JSON file and create the the table by building ddl
# Change History:
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Mridul Pathak/Vipul  -           Initial version
#
#**************************************************************************************************
# Runtime Parameters, passed through spark submit
#  1) spark.finopsENV.config_path - Github relative path where the ddl JSON/list located
#  2) spark.finopsENV.config_name - Name of the script to process
#  3) spark.finopsENV.ddl_script_path - Name of the script path
#  4) spark.finopsENV.ddl_script_name - Name of the script file 
#  5) spark.finopsENV.ddl_script_type - file or list
# NOTE - Drop table is not processed even if it is set to yes
#**************************************************************************************************
# Validate the DDL definition file and find if all the requirements are met
#**************************************************************************************************
def validate_ddl_def(p_table_json_def):
    l_required_keys = {"TableName", "DatabaseName", "Columns"}
    if not all(key in p_table_json_def for key in l_required_keys):
        logCritical("Table definition JSON does not have required values defined -database, table, columns")
        return False
    if not isinstance(p_table_json_def["Columns"], list) or not p_table_json_def["Columns"]:
        logCritical("Columns must be a non-empty list.")
        return False
    for col in p_table_json_def["Columns"]:
        if not {"ColumnID", "ColumnName", "FormattedDataType", "IsNull"}.issubset(col):
            logCritical("Table definition JSON does not have required values defined - columns")
            return False
    #
    logInfo("Table DDL definition validation succeeded")
    return True
    #
#============================================================================================================
#
# {
#   "TableList": [
#        {"TableName":"NameOftheTable","ScriptFileName":"DDLScriptName","DDLType":"Create","DropTable":"No"},
#        ...
#     ]
# }
#=============================================================================================================     
def create_tables(p_table_list_name)->bool:
    try:
        l_jfile = open(p_table_list_name)
        l_ddl_jobj = json.load(l_jfile)
        for ddl_table in l_ddl_jobj["TableList"]:
            if g_ddl_script_path[-1] == "/":                
                l_ddl_script_with_path = g_ddl_script_path + ddl_table.get("ScriptFileName")
            else:
                l_ddl_script_with_path = g_ddl_script_path + "/"+ ddl_table.get("ScriptFileName")   
            #
            if not create_table(l_ddl_script_with_path):
                logCritical(f"Unable to create table from list, processing stopped")
                return False
            #      
    except Exception as err:
        print("error")    
    #
    return True
    #
  
#*************************************************************************************************************
# Take the list of table properties defined as default table property
# Override the property value if that property is defined specifically for this table
# Return the merged property value
#*************************************************************************************************************
def merge_table_properties(p_default_properties, p_table_properties):
    l_merged_properties = p_default_properties.copy()
    for key, value in p_table_properties.items():
        if key in p_default_properties:
            if p_table_properties[key] != p_default_properties[key]:
                p_table_properties[key] = p_default_properties[key]
        else:
            l_merged_properties[key] = value
    for key, value in p_default_properties.items():
        if key not in l_merged_properties:
            l_merged_properties[key] = value
    return l_merged_properties
#************************************************************************************************************      
def create_table(p_table_script_name_with_path)->bool:
    global g_default_table_properties
    logInfo(f"Creating table from script {p_table_script_name_with_path}")
    if not (dvf.is_valid_file(p_table_script_name_with_path)):
        logCritical(f"{p_table_script_name_with_path} does not exist or not valid")
        return False 
    #
    try:
        l_jfile = open(p_table_script_name_with_path)
        l_ddl_jobj = json.load(l_jfile)
        if not validate_ddl_def(l_ddl_jobj):
            logCritical(f"JSON definition for DDL scriptis not valid for file {p_table_script_name_with_path}")
            return False
        #
        print('*' * 85)
        print("Database Name : %-30s Table Name: %-30s" % (l_ddl_jobj.get("DatabaseName"),l_ddl_jobj.get("TableName")))            
        print('*' * 85)
        print("%-5s %-30s %-30s %15s" % ("ID","Column Name","Data Type","Is Null?"))
        print('*' * 85)
        #
        l_col_def_list = []
        for col in l_ddl_jobj['Columns']:
            print("%5d %-30s %-30s %15s" % (col['ColumnID'],col['ColumnName'],col['FormattedDataType'],col['IsNull']))
            l_column_def = f"{col['ColumnName']} {col['FormattedDataType']}"
            #
            if not col['IsNull']:
                l_column_def += " NOT NULL"
            #  
            l_description = col.get('Description')
            #
            if l_description:
                l_column_def += f" COMMENT '{l_description}'"
            #
            l_col_def_list.append(l_column_def)
            #
            print('*' * 85)   
        #
        #    
        l_column_defs_str = ", ".join(l_col_def_list)
        #
        # Merge the Table properties, if no property was found issue a warning
        #
        if l_ddl_jobj.get("IsExternal") == "yes":
            l_external_tbl_val = "EXTERNAL"
        else:
            l_external_tbl_val=""

        l_table_properties = merge_table_properties(g_default_table_properties, l_ddl_jobj.get("TableProperties", {}))
        l_table_props_str = ", ".join([f"'{key}'='{value}'" for key, value in l_table_properties.items()])
        l_ddl_script = f"""CREATE {l_external_tbl_val} TABLE IF NOT EXISTS {l_ddl_jobj.get("DatabaseName")}.{l_ddl_jobj.get("TableName")} ({l_column_defs_str}) USING iceberg {f"TBLPROPERTIES ({l_table_props_str})" if l_table_properties else ""}"""
        #
        print("Executing DDL=>",l_ddl_script)
        spark.sql(l_ddl_script)
        #create table
        return True             
    except Exception as err:
        logCritical(f"Error creating table from script {p_table_script_name_with_path}")
        return False    
   
#*********************************************************************
#------------------------------  -----------------------------
def logInfo(p_message,p_target = "both"):
    g_logger.info(p_message)
def logCritical(p_message):
        g_logger.critical(p_message)
def logWarning(p_message):
        g_logger.warn(p_message)
#**************************************************************************************************
def init_environment()->bool:
    global g_config_path
    global g_ddl_script_name_with_path
    global g_ddl_script_type
    global g_ddl_script_path    
    global g_env_name    
    global g_default_table_properties
    #
    logInfo("Checking for required runtime parameter values")
    #
    l_req_vars = [
        "spark.finopsENV.config_path",
        "spark.finopsENV.ddl_script_name",
        "spark.finopsENV.ddl_script_path",
        "spark.finopsENV.ddl_script_type",
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
    logInfo("Required parameters found, assigning them to respective local variables")
    #
    l_config_path  = l_spark_vars["spark.finopsENV.config_path"]
    l_ddl_script_name = l_spark_vars["spark.finopsENV.ddl_script_name"]
    g_ddl_script_path = l_spark_vars["spark.finopsENV.ddl_script_path"]
    g_ddl_script_type = l_spark_vars["spark.finopsENV.ddl_script_type"]
    g_env_name = l_spark_vars["spark.finopsENV.environment"]
    #
    logInfo(f"Config path - {l_config_path}")
    logInfo(f"DDL Script path - {g_ddl_script_path}")
    logInfo(f"DDL Script name - {l_ddl_script_name}")
    logInfo(f"DDL Script type - {g_ddl_script_type}")
    logInfo(f"Environment name - {g_env_name}")
    #
    g_config_path = l_config_path
    #

    #
    if g_ddl_script_path[-1] == "/":
        g_ddl_script_name_with_path = g_ddl_script_path + l_ddl_script_name
    else:
        g_ddl_script_name_with_path = g_ddl_script_path + "/" + l_ddl_script_name
    #        
    if not (dvf.is_valid_file(g_ddl_script_name_with_path)):
        logCritical(f"{g_ddl_script_name_with_path} is not a valid file or does not exist")
        return False
    logInfo(f"DDL Script File Name => {g_ddl_script_name_with_path}")    
    #
    #
    if g_ddl_script_type not in ["file","list"]:
        logCritical(f"Invalid script type specified {g_ddl_script_type}, must be 'file' or 'list'")
        return False
    #
    return True

#*******************************************************************************************************
def load_default_properties()->bool:
    global g_default_table_properties
    global g_env_name
    if g_config_path[-1] == "/":
        l_dflt_table_property_file = g_config_path +  "default_iceberg_table_prop.json"
    else:
        l_dflt_table_property_file = g_config_path + "/" + "default_iceberg_table_prop.json"    
    #
    logInfo(f"Checking for default ICEBERG Table property file {l_dflt_table_property_file}")
    #
    if not (dvf.is_valid_file(l_dflt_table_property_file)):
        logCritical(f"{l_dflt_table_property_file} does not exist or not valid")
        return False
    if g_env_name not in ["Development","Validation","Production"]:
        logCritical(f"Invalid environment name specified {g_env_name}, valid values are Development/Validation/Production")
        return False
    #
    try:
        l_jfile = open(l_dflt_table_property_file)
        l_ddl_jobj = json.load(l_jfile)
        g_default_table_properties = l_ddl_jobj.get(g_env_name)
        return True
    except Exception as err:
        logCritical(f"Unable to process default table properties file {l_dflt_table_property_file}")
        logCritical(f"Error {err=} ,{type(err)=}")
        return False

#**************************************************************************************************** 
# MAIN: Init, Validate and Load
#****************************************************************************************************
if __name__ == "__main__":      
    try:             
        global spark
        spark = dvf.create_spark_session("Creating Iceberg Tables",g_logger) 
        #
        if not spark:
            dvf.stop_and_exit(None,1)
        #    
        logInfo(f"Spark Session created. Initializing environment variables")
        #
        if not init_environment():
            dvf.stop_and_exit(spark,1)
        #       
        #
        #             
        if not load_default_properties():
            dvf.stop_and_exit(spark,1)
        #
        logInfo("Default table properties fetched") 
        if (g_ddl_script_type == "file"):
            if not create_table(g_ddl_script_name_with_path):
                dvf.stop_and_exit(spark,1)
        elif (g_ddl_script_type == "list"):
            if not create_tables(g_ddl_script_name_with_path): 
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
