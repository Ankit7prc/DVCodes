import sys
import os
from pyspark.sql import SparkSession
import logging
import json
import requests as http_request
#**************************************************************************************************
# Script/Module Name - dvfinopscommonfunctions.py
# Purpose - Provides helper functions to support ELT routines 
# Change History:
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Lachhaman Moharana  -           Initial version
#
#**************************************************************************************************
# Check if a file exists and its a file
#**************************************************************************************************
def is_valid_file(p_file_name_with_path):
    return(os.path.exists(p_file_name_with_path) and os.path.isfile(p_file_name_with_path))    

#**************************************************************************************************
# Stop the spark session and exit with given exit code
#**************************************************************************************************
def stop_and_exit(p_spark_session,p_exit_code):    
    if p_spark_session:
        p_spark_session.stop()
    sys.exit(p_exit_code)    

#****************************************************************************************************
# Create spark session : Init, Validate and Load
#****************************************************************************************************
def create_spark_session(p_session_name,p_logger):    
    try:
        spark = SparkSession.builder \
            .appName(p_session_name) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        return spark
    except Exception as err:
        if p_logger is None:
            print("Unable to create spark session")
            print(f"Unexpected {err=}, {type(err)=}")
        else:    
            p_logger.critical("Unable to create spark session")
            p_logger.critical(f"Unexpected {err=}, {type(err)=}")
        return False
#**************************************************************************************************** 
# Initialize Logger
#****************************************************************************************************
def get_logger():
    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    l_logger = logging.getLogger(__name__)
    l_logger.setLevel(logging.INFO)    
    return l_logger
#**************************************************************************************************** 
# Get a variable name passed to spark submit/spark shell command tied to 
#****************************************************************************************************  
def get_spark_env_variable(p_sparksession, p_variable_name):
    conf = p_sparksession.sparkContext.getConf()
    l_var_value = conf.get("{p_variable_name}")
    return l_var_value
#**************************************************************************************************** 
# Get a variable name passed to spark submit/spark shell command tied to 
#****************************************************************************************************  
def get_credential_from_nabu(p_sparksession,p_credential_name):
    conf = p_sparksession.sparkContext.getConf()
    l_app_name = conf.get("spark.app.name")
    l_endpoint_url = conf.get("spark.nabu.fireshots_creds_end_point")
    l_token = conf.get("spark.nabu.token")
    #
    print(f"spark.app.name = {l_app_name} Empty spark.nabu.fireshots_creds_end_point = {l_endpoint_url}, spark.nabu.token = {l_token}")
    if not l_endpoint_url or not l_token:
        return
    l_payload = json.dumps ({"credential_name" : f"{p_credential_name}"})
    l_headers = {'Authorization': f'{l_token}','Content-Type':'application/json','Accept':'application/json'}
    #
    l_response = http_request.post(l_endpoint_url,headers=l_headers,data=l_payload,verify=False)
    return l_response
def check_values_present(p_keys_list, p_dictionary):
    
    l_navl_list  = ""

    for l_key in p_keys_list:
        if l_key not in p_dictionary:            
            l_navl_list = l_navl_list + " " + l_key
    #
    if l_navl_list == "":
        return "yes"
    #
    return l_navl_list
