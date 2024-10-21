from pyspark.sql import Row
import uuid
import psycopg2
#***********************************************************************************************
# ELT Metadata Manager
#***********************************************************************************************
class MetadataManager:
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
    #****************************************** Close Connection
    def DisconnectDatabase(self):
         self.__db_connection.close()
    #****************************************** Get Source System Record
    def GetLoadConfigRecord(self,p_load_id:str)->Row:
        self.ConnectDatabase()
        l_cur = self.__db_connection.cursor()
        l_cur.execute(f"""select 
                        load_id ,
                        load_name ,
                        src_type ,
                        src_db_name ,
                        src_tbl_name ,
                        src_qry_text ,
                        aud_cols_json ,
                        key_cols_json ,
                        tgt_db_name ,
                        tgt_tbl_name ,
                        tgt_upd_strategy ,
                        tgt_del_flag_col_name ,
                        identify_src_delete ,
                        pre_tgt_load_sql ,
                        post_tgt_load_sql
                        from finops.elt_data_load_cfg
                        where load_id='{p_load_id}'""")
        #
        if l_cur.rowcount == 0:
             l_cur.close()
             return None
        #
        l_rec1 = l_cur.fetchone()
        l_entity_rec1 = Row(
            load_id = l_rec1[0],
            load_name = l_rec1[1],
            src_type = l_rec1[2],
            src_db_name = l_rec1[3],
            src_tbl_name = l_rec1[4],
            src_qry_text = l_rec1[5],
            aud_cols_json = l_rec1[6],
            key_cols_json = l_rec1[7],
            tgt_db_name = l_rec1[8],
            tgt_tbl_name = l_rec1[9],
            tgt_upd_strategy = l_rec1[10],
            tgt_del_flag_col_name = l_rec1[11],
            identify_src_delete = l_rec1[12],
            pre_tgt_load_sql = l_rec1[13],
            post_tgt_load_sql= l_rec1[14])
        l_cur.close()
        self.DisconnectDatabase()
        #
        return l_entity_rec1   
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
