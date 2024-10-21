import traceback
import inspect
from datetime import date, datetime
from pyspark.sql import SparkSession

#**************************************************************************************************
# Script/Module Name - dvelt_loadlogger.py
# Purpose - Provides helper functions to support source to staging/raw layer data load 
# Change History:
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Lachhaman Moharana  -           Initial version
#
#**************************************************************************************************
class LoadLogger:
    def __init__(self):        
        self.sparksession:SparkSession = None
        self.source_tracking_enabled = False
        self.pipeline_name:str = "None"
        self.run_id:str = "None"
        self.rep_db_name:str = "None"         
        self.elt_src_entity_load_hist:str = "None" #"elt_src_entity_load_hist"
        self.elt_src_sys_load_def:str = "None" #"elt_src_sys_load_def"
        self.elt_src_entity_load_def:str = "None" #"elt_src_entity_load_def"
        self.elt_src_entity_load_hist_arch:str = "None" #e.g. "elt_src_entity_load_hist_arch"
        self.elt_run_messages:str = "None" #"elt_run_messages"
        self.process_start_time:datetime = datetime.now()
        self.process_finish_time:datetime = datetime.now()
        self.src_rec_count:int= 0
        self.tgt_rec_count:int= 0
        self.load_status:str= "started"
        self.last_message:str = "init"
        self.message_seq:int = 0
        self.message_type:str = "info"
        self.message_text:str = "init"
        self.src_sys_key:str = None
        self.src_entity_key:str = None
        self.src_sys_id:int = 1
        self.src_entity_id:int = 1
        self.process_run_id:str = 'init'
        self.db_initialized = False
    #
    def SourceLoadStarted (self):
        if self.sparksession is None or self.source_tracking_enabled is False:
            return
        try:
            self.process_start_time = datetime.now()
            updateStmt = f"""update {self.rep_db_name}.{self.elt_src_entity_load_hist} 
                        set src_entity_load_status='started',
                        src_entity_load_mesg='Started' ,
                        src_entity_load_start_time = current_timestamp()  
                        where 
                        src_entity_load_run_id = '{self.process_run_id}'"""
            self.sparksession.sql(updateStmt)
            self.Info("Load process tarted")
        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")
    #
    def SourceLoadFinished(self):
        if self.sparksession is None or self.source_tracking_enabled is False:
            return
        try:
            self.process_finish_time = datetime.now()
            updateStmt = f"""update {self.rep_db_name}.{self.elt_src_entity_load_hist} 
                        set src_entity_load_status='completed',
                        src_entity_load_mesg= '{self.message_text}', 
                        src_entity_load_finish_time = current_timestamp(),
                        src_entity_rec_count = {self.src_rec_count} 
                        where 
                        src_entity_load_run_id = '{self.process_run_id}'"""
            self.sparksession.sql(updateStmt)
            self.Info("Load process completed")
        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")      
    #
    def SourceLoadSkipped(self):
        if self.sparksession is None or self.source_tracking_enabled is False:
            return
        try:
            self.process_finish_time = datetime.now()
            updateStmt = f"""update {self.rep_db_name}.{self.elt_src_entity_load_hist} 
                        set src_entity_load_status='skipped',
                        src_entity_load_mesg='Data load skipped', 
                        src_entity_load_finish_time = current_timestamp() 
                        where 
                        src_entity_load_run_id = '{self.process_run_id}'"""
            self.sparksession.sql(updateStmt)    
            self.Info("Load skipped because there is recent file than this found and this is a full refresh source data file")
        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")     
    #
    def SourceLoadFailed(self):
        if self.sparksession is None or self.source_tracking_enabled is False:
            return
        try:
            self.process_finish_time = datetime.now()
            updateStmt = f"""update {self.rep_db_name}.{self.elt_src_entity_load_hist} 
                    set src_entity_load_status='failed',
                    src_entity_load_mesg='Data load failed', 
                    src_entity_load_finish_time = current_timestamp()
                    where 
                    src_entity_load_run_id ='{self.process_run_id}'"""
            self.sparksession.sql(updateStmt)            
            self.Error("Load process failed")
        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")     
    #
    def SourceLoadError(self):
        if self.sparksession is None or self.source_tracking_enabled is False:
            return
        try:
           self.process_finish_time = datetime.now()
           updateStmt = f"""update {self.rep_db_name}.{self.elt_src_entity_load_hist}
                    set src_entity_load_status='error',
                    src_entity_load_mesg='Error Occurred', 
                    src_entity_load_finish_time = current_timestamp() 
                    where 
                    src_entity_load_run_id ='{self.process_run_id}'"""
           self.sparksession.sql(updateStmt)
          
           self.LogMessage("ERROR","Error occurred")
        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")   
    #
    def GetLastSourceBookmark(self)->str:
        if self.sparksession is None:
            return
        
        try:
            l_df = self.sparksession.sql(f"""select src_load_last_bookmark 
                                from {self.rep_db_name}.{self.elt_src_entity_load_def} 
                                where
                                src_sys_key = '{self.src_sys_key}' and 
                                src_entity_key='{self.src_entity_key}'""")
            if (l_df.count() == 0):
                return "error"
            else:
                return l_df.first()["src_load_last_bookmark"]
        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")   
    # 
    def Info(self,message_text):
        self.LogMessage("info",message_text)
    #
    #
    def Error(self,message_text):
        self.LogMessage("error",message_text)
    #    
    def Warning(self,message_text):
        self.LogMessage("warning",message_text)
    #
    def Critical(self,message_text):
        self.LogMessage("critical",message_text)
    #
    def LogMessage(self,p_message_type,p_message_text):
        if self.sparksession is None:
            return
        try:
            self.message_seq = self.message_seq+1
            self.message_time = datetime.now() 
            self.message_type = p_message_type
            self.message_text = p_message_text
            l_df =self.sparksession.createDataFrame([(self.process_run_id,
                                                    self.message_seq,
                                                    self.message_type,
                                                    self.message_time,
                                                    self.message_text)],
                                                    'run_id string, \
                                                    message_seq long, \
                                                    message_type string, \
                                                    message_time timestamp,\
                                                    message_text string')
            
            l_df.write.format("iceberg").mode("append").saveAsTable(f"{self.rep_db_name}.{self.elt_run_messages}")                  

        except Exception as err:
            frame = inspect.currentframe()
            print(f"Unhandled exception in {frame.f_code.co_name},{err=}, {type(err)=}")   
    #
    def Validate_Repository(self) -> bool:

        if not self.sparksession.catalog.tableExists(f"{self.rep_db_name}.{self.elt_run_messages}"):
            return False
        print(f"{self.rep_db_name}.{self.elt_run_messages} exists")       
        #
        if not self.sparksession.catalog.tableExists(f"{self.rep_db_name}.{self.elt_src_entity_load_def}"):
            return False
        print(f"{self.rep_db_name}.{self.elt_src_entity_load_def} exists")
        #           
        if not self.sparksession.catalog.tableExists(f"{self.rep_db_name}.{self.elt_src_sys_load_def}"):
            return False
        print(f"{self.rep_db_name}.{self.elt_src_sys_load_def} exists")
        #
        if not self.sparksession.catalog.tableExists(f"{self.rep_db_name}.{self.elt_src_entity_load_hist}"):
            return False
        print(f"{self.rep_db_name}.{self.elt_src_entity_load_hist} exists")
        #
        if not self.sparksession.catalog.tableExists(f"{self.rep_db_name}.{self.elt_src_entity_load_hist_arch}"):
            return False
        print(f"{self.rep_db_name}.{self.elt_src_entity_load_hist_arch} exists")
        #
        return True
    #***********************************************************************************************************
    # 
    def Get_Staged_Files(self,p_src_sys_key:str,p_src_entity_key:str,p_src_extract_type:str)->list:    
    #***********************************************************************************************
    # Get the list of staged files
    #   1) If the source extract type is incremental, get the list of staged (unprocessed) files
    #      in ascending order, so the first file arrived is the first in list
    #   2) If the source extract type is full, get the list of files in the descending order, so
    #      the last file arrived is first in the list
    #***********************************************************************************************
        self.Info(f"Fetching staged files list for source extract type => {p_src_extract_type}")
        if (p_src_extract_type == "incremental"):
            
            l_src_query = f"""
                        select * 
                        from {self.rep_db_name}.{self.elt_src_entity_load_hist}
                        where
                            src_sys_key = '{p_src_sys_key}' and src_entity_key = '{p_src_entity_key}' and  
                            src_entity_load_status='staged' order by src_entity_inst_key asc 
                        """
        else:        
            l_src_query = f"""
                        select *
                        from {self.rep_db_name}.{self.elt_src_entity_load_hist}
                        where 
                            src_sys_key = '{p_src_sys_key}' and src_entity_key = '{p_src_entity_key}' and 
                            src_entity_load_status='staged' order by src_entity_inst_key desc 
                        """         
        try:
            self.Info(f"Metadata query to fetch staged files {l_src_query}")
            l_df = self.sparksession.sql(l_src_query)
            self.Info(f"Query returned {l_df.count()} staged files")
            return l_df.collect()
        except Exception as err:
            frame = inspect.currentframe()
            self.Critical(f"Exception at {frame.f_code.co_name}")        
            self.Critical(f"Unexpected {err=}, {type(err)=}")
            self("Checking for source files staged for processing failed")
            return []    
