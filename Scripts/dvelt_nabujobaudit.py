import traceback
import inspect
from datetime import date, datetime
from pyspark.sql import SparkSession 
import psycopg2

#**************************************************************************************************
# Script/Module Name - dvelt_nabujobaudit.py
# Purpose - Provides helper functions to support Dataverse Auditing Requirements 
# Change History:
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Lachhaman Moharana  -           Initial version
#                     08/21/2024  Added conditions for performing maintenance
#**************************************************************************************************
class NabuJobAudit:
    def __init__(self):        
        self.__target_db_name:str = None
        self.__target_tbl_name:str = None
        self.__last_message:str = None
        self.__tm_frequency:int = 7
        #
        #Member variables for table EXTRACTION.EXTRACTION_JOBS_AUDIT
        #
        self.__nabu_audit_table_name:str = "EXTRACTION.EXTRACTION_JOBS_AUDIT"
        self.__nabu_maint_table_name:str = "EXTRACTION.EXTRACTION_MAINTENANCE_JOBS_AUDIT"
        self.__nabu_database_name:str = None
        self.__nabu_username:str  = None
        self.__nabu_password:str = None
        self.__nabu_dbport:int = None
        self.__nabu_db_host:str = None
        #
        self.__domain_name:str = None
        self.__job_name:str = None
        self.__layer_name:str = None
        self.__job_id:str = None
        self.__app_id:str = None
        #
        self.incremental_data_count:int = None
        self.hard_deleted_data_count:int = None
        self.total_data_count:int = None
        #
        self.rewritten_data_files_count: int = None #spark_catalog.system.rewrite_data_files
        self.added_data_files_count: int = None #spark_catalog.system.rewrite_data_files
        self.rewritten_bytes_count: int = None #spark_catalog.system.rewrite_data_files
        self.rewritten_manifests_count: int = None #spark_catalog.system.rewrite_manifests
        self.added_mainfests_count: int = None #spark_catalog.system.rewrite_manifests
        self.rewritten_delete_files_count: int = None #spark_catalog.system.rewrite_position_delete_files
        self.added_delete_files_count: int = None  #spark_catalog.system.rewrite_position_delete_files      
        self.added_bytes_count: int = None #spark_catalog.system.rewrite_position_delete_files
        self.deleted_data_files_count: int = None #spark_catalog.system.rewrite_manifests
        self.deleted_position_delete_files_count: int = None #spark_catalog.system.rewrite_manifests
        self.deleted_equality_delete_files_count: int = None #spark_catalog.system.rewrite_manifests
        self.deleted_manifest_files_count: int = None #spark_catalog.system.rewrite_manifests
        self.deleted_manifest_lists_count: int = None #spark_catalog.system.rewrite_manifests
    #
    def ResetVariables(self):
        self.__job_id = None
        #
        self.__target_db_name = None
        self.__target_tbl_name = None
        self.__last_message = "What a wonderful world!"
        #
        self.incremental_data_count:int = None
        self.hard_deleted_data_count:int = None
        self.total_data_count:int = None
        #
        self.rewritten_data_files_count: int = None 
        self.added_data_files_count: int = None 
        self.rewritten_bytes_count: int = None 
        self.rewritten_manifests_count: int = None 
        self.added_mainfests_count: int = None 
        self.rewritten_delete_files_count: int = None
        self.added_delete_files_count: int = None        
        self.added_bytes_count: int = None
        self.deleted_data_files_count: int = None
        self.deleted_position_delete_files_count: int = None
        self.deleted_equality_delete_files_count: int = None
        self.deleted_manifest_files_count: int = None
        self.deleted_manifest_lists_count: int = None
    #
    def __GetNullStr(self,p_value):
        if p_value is None:
            return 'null'
        else:
            return p_value
    # 
    def __ExecuteSql(self,p_aud_sql:str):
        try:            
            #
            print("jobid->",self.__job_id)
            #
            l_conn = psycopg2.connect(database = f"{self.AuditDatabaseName}", 
                        user = f"{self.AuditDatabaseUser}", 
                        host= f'{self.AuditDatabaseHost}',
                        password = f"{self.AuditDatabasePassword}",
                        port = self.AuditDatabasePort)
            l_cur = l_conn.cursor()
            #
            print("*********************EXECUTE SQL**********************")
            print(p_aud_sql)
            #print("*********************MAINT SQL**********************")
            #print(p_maint_sql)  
            #
            l_cur.execute(p_aud_sql)
            #l_cur.execute(p_maint_sql)
            #
            l_conn.commit()
            # Close cursor and communication with the database
            l_cur.close()
            l_conn.close()
            #
        except Exception as err:
            print("Failed to execute sql for audit data logging")
            frame = inspect.currentframe()
            print(f"Exeption {frame.f_code.co_name},{err=}, {type(err)=}")
    #
    def JobStarted (self):
        self.__ExecuteSql(self.__AuditStartSql)
       
    def JobFailed(self):
        self.__ExecuteSql(self.__AuditErrorSql)
    #
    def JobCompleted(self):                
        self.__ExecuteSql(self.__AuditSuccessSql)
    #
    def IsMaintenanceDue(self)->bool:
        l_conn = psycopg2.connect(database = f"{self.AuditDatabaseName}", 
                        user = f"{self.AuditDatabaseUser}", 
                        host= f'{self.AuditDatabaseHost}',
                        password = f"{self.AuditDatabasePassword}",
                        port = self.AuditDatabasePort)
        l_cur = l_conn.cursor()
        #
        l_sql = """select count(*) 
        from EXTRACTION.EXTRACTION_MAINTENANCE_JOBS_AUDIT 
        where domain=%s and job_name=%s and (current_timestamp::date-start_time::date)>%s """
        l_cur.execute(l_sql,(self.__domain_name,self.__job_name,self.__tm_frequency))
        l_count = l_cur.fetchone()[0]
        if l_count > 0:
            return True
        #
        return False
    #
    def AnalyzeTarget(self,p_spark_session:SparkSession):
        if (p_spark_session is None or self.TargetDatabaseName is None or self.TargetTableName is None):
            print("One of these (SparkSession, Target Database or Target Table) is not set - analysis skipped")
            return
        # Add the Condition for Maitenance
        #        
        try:
            dfx = p_spark_session.sql(f"select sum(summary.`total-position-deletes`) as pos_del  from {self.__target_db_name}.{self.__target_tbl_name}.snapshots ")
            l_position_deletes = dfx.first()["pos_del"]
            dfx1=p_spark_session.sql(f"select count(*) as data_files  from {self.__target_db_name}.{self.__target_tbl_name}.data_files")
            l_data_files = dfx1.first()["data_files"]
            #
            if not (l_position_deletes > 100000 or l_data_files > 200):
                print(f"Maintenance on table {self.__target_db_name}.{self.__target_tbl_name} skipped, as conditions not met")
                return
            #
            if not self.IsMaintenanceDue():
                print(f"Maintenance on table {self.__target_db_name}.{self.__target_tbl_name} is not due yet")
                return
            #
            self.__ExecuteSql(self.__MaintainanceStartSql)
            #
            analysis_df = p_spark_session.sql(f"CALL spark_catalog.system.rewrite_data_files('{self.__target_db_name}.{self.__target_tbl_name}')")
            #
            print("spark_catalog.system.rewrite_data_files")
            analysis_df.show()
            #
            self.rewritten_data_files_count = analysis_df.first()['rewritten_data_files_count']
            self.added_data_files_count = analysis_df.first()['added_data_files_count']
            #
            analysis_df = p_spark_session.sql(f"CALL spark_catalog.system.expire_snapshots('{self.__target_db_name}.{self.__target_tbl_name}')")
            #
            print("spark_catalog.system.expire_snapshots")
            analysis_df.show()
            #
            self.deleted_data_files_count = analysis_df.first()['deleted_data_files_count']
            self.deleted_position_delete_files_count = analysis_df.first()['deleted_position_delete_files_count']
            self.deleted_equality_delete_files_count = analysis_df.first()['deleted_equality_delete_files_count']
            self.deleted_manifest_files_count = analysis_df.first()['deleted_manifest_files_count']
            self.deleted_manifest_lists_count = analysis_df.first()['deleted_manifest_lists_count']

            #
            analysis_df = p_spark_session.sql(f"CALL spark_catalog.system.rewrite_manifests('{self.__target_db_name}.{self.__target_tbl_name}')")
            #
            print("spark_catalog.system.rewrite_manifests")
            analysis_df.show()
            #
            self.rewritten_manifests_count = analysis_df.first()['rewritten_manifests_count']
            self.added_mainfests_count = analysis_df.first()['added_manifests_count']

            #
            analysis_df = p_spark_session.sql(f"CALL spark_catalog.system.rewrite_position_delete_files('{self.__target_db_name}.{self.__target_tbl_name}')")
            #
            print("spark_catalog.system.rewrite_position_delete_files")
            analysis_df.show()
            #
            self.rewritten_delete_files_count = analysis_df.first()['rewritten_delete_files_count']
            self.added_delete_files_count = analysis_df.first()['added_delete_files_count']
            self.rewritten_bytes_count = analysis_df.first()['rewritten_bytes_count']
            self.added_bytes_count = analysis_df.first()['added_bytes_count']
            #
            self.__ExecuteSql(self.__MaintainanceSuccesstSql)
            #
        except Exception as err:       
            self.__ExecuteSql(self.__MaintainanceErrorSql)     
            frame = inspect.currentframe()
            print(f"ERROR in analyzing table, exception at {frame.f_code.co_name}")        
            print(f"Exception=> {err=}, {type(err)=}")
            traceback.print_exc() 
    #***********************************************************************************************
    # Properties to Assign/Get different values
    #***********************************************************************************************    
    @property
    def LastMessage(self):
        return self.__last_message
    @LastMessage.setter
    def LastMessage(self,new_value):
        self.__last_message = new_value
    #********************************************* Database Name
    @property
    def AuditDatabaseName(self):
        return self.__nabu_database_name 
    @AuditDatabaseName.setter
    def AuditDatabaseName(self,new_value):
        self.__nabu_database_name = new_value   
    #********************************************* Database User
    @property
    def AuditDatabaseUser(self):
        return self.__nabu_username 
    @AuditDatabaseUser.setter
    def AuditDatabaseUser(self,new_value):
        self.__nabu_username = new_value            
    #******************************************** Database Password
    @property
    def AuditDatabasePassword(self):
        return self.__nabu_password 
    @AuditDatabasePassword.setter
    def AuditDatabasePassword(self,new_value):
        self.__nabu_password = new_value       
    #******************************************** Database Host Name
    @property
    def AuditDatabaseHost(self):
        return self.__nabu_db_host 
    @AuditDatabaseHost.setter
    def AuditDatabaseHost(self,new_value):
        self.__nabu_db_host = new_value       
    #******************************************** Database Port Number
    @property
    def AuditDatabasePort(self):
        return self.__nabu_dbport 
    @AuditDatabasePort.setter
    def AuditDatabasePort(self,new_value:int):
        self.__nabu_dbport = new_value       
    #********************************************Job Audit Table Name
    @property
    def AuditTableName(self):
        return self.__nabu_audit_table_name 
    @AuditTableName.setter
    def AuditTableName(self,new_value):
        self.__nabu_audit_table_name = new_value
    #****************************************** Job Maintenance Table Name
    @property
    def MaintenanceTableName(self):
        return self.__nabu_maint_table_name 
    @MaintenanceTableName.setter
    def MaintenanceTableName(self,new_value):
        self.__nabu_maint_table_name = new_value        
    #****************************************** Target Database Name
    @property
    def TargetDatabaseName(self):
        return self.__target_db_name
    @TargetDatabaseName.setter
    def TargetDatabaseName(self,new_value):
        self.__target_db_name = new_value                 
    #***************************************** Target Table Name
    @property
    def TargetTableName(self):
        return self.__target_tbl_name
    @TargetTableName.setter
    def TargetTableName(self,new_value):
        self.__target_tbl_name = new_value   
    #***************************************** Job ID
    @property
    def JobID(self):
        return self.__job_id
    @JobID.setter
    def JobID(self,new_value):
        self.__job_id = new_value        
    #***************************************** Job Name
    @property
    def JobName(self):
        return self.__job_name
    @JobName.setter
    def JobName(self,new_value):
        self.__job_name = new_value           
    #******************************************* Domain Name
    @property
    def DomainName(self):
        return self.__domain_name 
    @DomainName.setter
    def DomainName(self,new_value):
        self.__domain_name = new_value    
    @property           
    #***************************************** Layer Name
    @property
    def LayerName(self):
        return self.__layer_name
    @LayerName.setter
    def LayerName(self,new_value):
        self.__layer_name = new_value    
    #JobID    
    #**************************************** Application ID
    @property
    def ApplicationID(self):
        return self.__app_id
    @ApplicationID.setter
    def ApplicationID(self,new_value):
        self.__app_id = new_value      
    #**************************************** Target Maintenance Frequency
    @property
    def TargetMaintenanceFrequency(self):
        return self.__tm_frequency
    @TargetMaintenanceFrequency.setter
    def TargetMaintenanceFrequency(self,new_value):
        self.__tm_frequency = new_value                                 
#*******************************************************************
# SQLs, put them into properties so main code is better readability
#*******************************************************************
    @property
    def __AuditStartSql(self):
        return f"""
INSERT INTO {self.AuditTableName}
    (id, domain, layer, job_name, spark_application_id, status,start_time)
VALUES
    ('{self.__job_id}','{self.__domain_name}','{self.__layer_name}','{self.__job_name}',
    '{self.__app_id}','RUNNING',current_timestamp)            
"""
    @property
    def __MaintainanceStartSql(self):
        return f"""
INSERT INTO {self.MaintenanceTableName}
    (id, domain, layer, job_name,status,start_time) 
VALUES 
    ('{self.__job_id}','{self.__domain_name}', '{self.__layer_name}', '{self.__job_name}','RUNNING',current_timestamp);
"""   
    @property
    def __AuditSuccessSql(self):
        return f"""
UPDATE {self.AuditTableName} 
    SET 
        status='COMPLETED', 
        end_time=current_timestamp, 
        incremental_data_count= {self.__GetNullStr(self.incremental_data_count)}, 
        hard_deleted_data_count= {self.__GetNullStr(self.hard_deleted_data_count)}, 
        total_data_count = {self.__GetNullStr(self.total_data_count)} 
    WHERE 
        id = '{self.__job_id}'
"""
    @property
    def __MaintainanceSuccesstSql(self):
        return f"""
 UPDATE 
    {self.MaintenanceTableName}
    SET 
        status='COMPLETED', 
        rewritten_data_files_count={self.__GetNullStr(self.rewritten_data_files_count)}, 
        added_data_files_count = {self.__GetNullStr(self.added_data_files_count)}, 
        rewritten_bytes_count = {self.__GetNullStr(self.rewritten_bytes_count)}, 
        rewritten_manifests_count = {self.__GetNullStr(self.rewritten_manifests_count)}, 
        added_mainfests_count = {self.__GetNullStr(self.added_mainfests_count)}, 
        deleted_data_files_count = {self.__GetNullStr(self.deleted_data_files_count)}, 
        deleted_position_delete_files_count = {self.__GetNullStr(self.deleted_position_delete_files_count)}, 
        deleted_equality_delete_files_count = {self.__GetNullStr(self.deleted_equality_delete_files_count)}, 
        deleted_manifest_files_count = {self.__GetNullStr(self.deleted_manifest_files_count)}, 
        deleted_manifest_lists_count = {self.__GetNullStr(self.deleted_manifest_lists_count)}, 
        rewritten_delete_files_count = {self.__GetNullStr(self.rewritten_delete_files_count)}, 
        added_delete_files_count = {self.__GetNullStr(self.added_delete_files_count)},        
        added_bytes_count = {self.__GetNullStr(self.added_bytes_count)}, 
        end_time = current_timestamp 
    WHERE 
        id='{self.__job_id}'; 
"""
    @property
    def __AuditErrorSql(self):
        return f"""
UPDATE {self.AuditTableName} 
    SET 
        status='ERROR', 
        end_time = current_timestamp, 
        incremental_data_count = null, 
        hard_deleted_data_count = null, 
        total_data_count = null 
    WHERE 
        id = '{self.__job_id}'
"""
    @property
    def __MaintainanceErrorSql(self):
        #self.__last_message = self.__last_message.replace("'","")
        return f"""
UPDATE {self.MaintenanceTableName} 
SET 
    status='ERROR', 
    rewritten_data_files_count=null, 
    added_data_files_count = null, 
    rewritten_manifests_count = null, 
    added_mainfests_count = null,
    deleted_data_files_count = null, 
    deleted_position_delete_files_count = null, 
    deleted_equality_delete_files_count= null,
    deleted_manifest_files_count = null, 
    deleted_manifest_lists_count = null, 
    rewritten_delete_files_count = null, 
    added_delete_files_count = null, 
    rewritten_bytes_count = null, 
    added_bytes_count = null, 
    end_time = current_timestamp,
    error_message = '{self.__last_message}' 
WHERE 
    id ='{self.__job_id}'
"""          
