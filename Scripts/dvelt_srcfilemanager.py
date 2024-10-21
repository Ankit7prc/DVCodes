from pyspark.sql import Row
import boto3
from datetime import datetime
from pyspark.sql import SparkSession, Row
import fnmatch
import uuid
import psycopg2
#***********************************************************************************************
#
#***********************************************************************************************

class SourceFileManager:
    def __init__(self): 
        self.__md_db_name:str = None
        self.__md_db_username:str  = None
        self.__md_db_password:str = None
        self.__md_db_port:int = None
        self.__md_db_host:str = None
        self.__db_connection = None
    #****************************************** Open Connection
    def ConnectDatabase(self):
         try:
              self.__db_connection = psycopg2.connect(
                    database = f"{self.MetdataDbName}", 
                    user = f"{self.MetdataDbUsername}", 
                    host= f'{self.MetdataDbHost}',
                    password = f"{self.MetdataDbPassword}",
                    port = self.MetdataDbPort  
              )     
              return True
         except (Exception,psycopg2.DatabaseError):
              return False
    #****************************************** Get Source System Record
    def GetSourceEntityRecord(self,p_src_sys_key:str,p_src_entity_key:str)->Row:
        self.ConnectDatabase()
        l_cur = self.__db_connection.cursor()
        l_cur.execute(f"""select src_sys_key,src_entity_key,src_entity_extract_type
                       from finops.elt_src_entity_load_def
                       where src_sys_key='{p_src_sys_key}' and src_entity_key='{p_src_entity_key}'""")
        if l_cur.rowcount == 0:
             l_cur.close()
             return None
        #
        l_rec1 = l_cur.fetchone()
        l_entity_rec1 = Row(src_sys_key=l_rec1[0],src_entity_key=l_rec1[1],src_entity_extract_type=l_rec1[2])
        l_cur.close()
        self.DisconnectDatabase()
        #
        return l_entity_rec1
    #****************************************** Get list of unprocessed files
    def GetUnprocessedEntityList(self,p_src_sys_key:str,p_src_entity_key:str)->list:
        self.ConnectDatabase()
        l_cur = self.__db_connection.cursor()
        l_cur.execute(f"""select 
                    src_entity_inst_key ,
                    src_entity_path ,
                    src_entity_name ,
                    src_entity_load_run_id,
                    src_entity_load_last_bookmark                    
                    from finops.elt_src_entity_load_hist
                    where 
                      src_sys_key='{p_src_sys_key}' and 
                      src_entity_key='{p_src_entity_key}' and
                      src_entity_load_status in('staged','failed')
                    order by src_entity_timestamp desc,src_entity_name desc  """) 
        if l_cur.rowcount == 0:
             l_cur.close()
             return None
        #
        l_rec1 = [Row(src_entity_inst_key=rec[0],
                      src_entity_path=rec[1],
                      src_entity_name=rec[2],
                      src_entity_load_run_id=rec[3],
                      src_entity_load_last_bookmark=rec[4]
                      ) for rec in l_cur.fetchall()]
        #
        l_cur.close()
        self.DisconnectDatabase()
        #
        return l_rec1
    #****************************************** Close Connection
    def DisconnectDatabase(self):
         self.__db_connection.close()
    #****************************************** Skip all files other than first
    def skip_files_other_than_first(self,p_files_list:list):
        self.ConnectDatabase()
        l_cur = self.__db_connection.cursor()
        #
        for frec_indx in range(1,len(p_files_list)):
            lsql = f"""update 
                    finops.elt_src_entity_load_hist 
                    set 
                        src_entity_load_status='skipped',
                        src_entity_load_mesg = 'File skipped as newer file is found'
                    where 
                        src_entity_inst_key='{p_files_list[frec_indx]["src_entity_inst_key"]}'
                    """
            l_cur.execute(lsql)
        #
        print(f"""{p_files_list[frec_indx]["src_entity_name"]} will be marked skipped""")
        #
        l_cur.close()
        self.__db_connection.commit()
        self.DisconnectDatabase()        
        #
    #****************************************** Mark file processed
    def mark_file_processed(self,p_src_file_rec:Row,p_process_status:str):
        self.ConnectDatabase()
        l_cur = self.__db_connection.cursor()
        lsql = f"""update 
                    finops.elt_src_entity_load_hist 
                    set 
                        src_entity_load_status='{p_process_status}'
                    where 
                        src_entity_inst_key='{p_src_file_rec["src_entity_inst_key"]}'
                    """
        l_cur.execute(lsql)
        print(f"{p_src_file_rec['src_entity_name']} processed status = {p_process_status}")  
        #
        l_cur.close()
        self.__db_connection.commit()
        self.DisconnectDatabase()        
    #****************************************** Metadata Database Name
    @property
    def MetdataDbName(self):
        return self.__md_db_name
    @MetdataDbName.setter
    def MetdataDbName(self,new_value):
        self.__md_db_name = new_value    
    #****************************************** Metadata Database Host Name
    @property
    def MetdataDbHost(self):
        return self.__md_db_host
    @MetdataDbHost.setter
    def MetdataDbHost(self,new_value):
        self.__md_db_host = new_value            
    #****************************************** Metadata Port Number
    @property
    def MetdataDbPort(self):
        return self.__md_db_port
    @MetdataDbPort.setter
    def MetdataDbPort(self,new_value):
        self.__md_db_port = new_value     
    #****************************************** Metadata Database User Name
    @property
    def MetdataDbUsername(self):
        return self.__md_db_username
    @MetdataDbUsername.setter
    def MetdataDbUsername(self,new_value):
        self.__md_db_username = new_value     
    #****************************************** Metadata Database Password
    @property
    def MetdataDbPassword(self):
        return self.__md_db_password
    @MetdataDbPassword.setter
    def MetdataDbPassword(self,new_value):
        self.__md_db_password = new_value                             