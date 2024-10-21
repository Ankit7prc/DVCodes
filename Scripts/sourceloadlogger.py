from datetime import date, datetime
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
#**************************************************************************************************
# Script/Module Name - sourceloadlogger.py
# Purpose - Provides helper functions to support source to staging/raw layer data load 
# Developed by : Lachhaman Moharana
#**************************************************************************************************
class LoadLogger:
    def __init__(self):        
        self.sparksession = None
        self.pipeline_name = ""
        self.src_file_load_run_id = 0
        self.rep_db_name = ""
        self.load_hist_table_name = "src_file_load_hist"
        self.load_mesg_table_name = "src_file_load_log"
        self.src_file_load_start_time = datetime.now()
        self.src_file_load_finish_time = datetime.now()
        self.src_rec_count= 0
        self.tgt_rec_count= 0
        self.load_status= "STARTED"
        self.last_message = "Process started"
        self.message_seq = 0
        self.message_type = "INFO"
        self.message_text = ""
        self.message_time = datetime.now()
        self.src_sys_id = "na"
    #
    def LoadStarted (self):
        if self.sparksession is None:
            return
        self.src_file_load_start_time = datetime.now()
        updateStmt = f"""update {self.rep_db_name}.{self.load_hist_table_name} 
                    set src_file_load_status='STARTED',
                    src_file_load_mesg='Started' ,
                    src_file_load_start_time = current_timestamp()  
                    where 
                    src_file_load_run_id={self.src_file_load_run_id}"""
        self.sparksession.sql(updateStmt)
        self.Info("Load process tarted")
    #
    def LoadFinished(self):
        if self.sparksession is None:
            return
        self.src_file_load_finish_time = datetime.now()
        updateStmt = f"""update {self.rep_db_name}.{self.load_hist_table_name} 
                    set src_file_load_status='COMPLETED',
                    src_file_load_mesg='Load completed' 
                    src_file_load_finish_time = current_timestamp(),
                    src_file_rec_count = {self.src_rec_count} 
                    where 
                    src_file_load_run_id={self.src_file_load_run_id}"""
        self.sparksession.sql(updateStmt)
        self.Info("Load process completed")        
    #
    def LoadSkipped(self):
        if self.sparksession is None:
            return
        self.src_file_load_finish_time = datetime.now()
        updateStmt = f"""update {self.rep_db_name}.{self.load_hist_table_name} 
                    set src_file_load_status='SKIPPED',
                    src_file_load_mesg='Data load skipped', 
                    src_file_load_finish_time = current_timestamp() 
                    where 
                    src_file_load_run_id={self.src_file_load_run_id}"""
        self.sparksession.sql(updateStmt)    
        self.Info("Load skipped because there is recent file than this found and this is a full refresh source data file")
    #
    def LoadFailed(self):
        if self.sparksession is None:
            return
        self.src_file_load_finish_time = datetime.now()
        updateStmt = f"""update {self.rep_db_name}.{self.load_hist_table_name} 
                    set src_file_load_status='FAILED',
                    src_file_load_mesg='Data load failed', 
                    src_file_load_finish_time = current_timestamp()
                    where 
                    src_file_load_run_id={self.src_file_load_run_id}"""
        self.sparksession.sql(updateStmt)
           
        self.Error("Load process failed")
    #
    def LoadError(self):
        if self.sparksession is None:
            return
        self.src_file_load_finish_time = datetime.now()
        updateStmt = f"""update {self.rep_db_name}.{self.load_hist_table_name}
                    set src_file_load_status='ERROR',
                    src_file_load_mesg='Error Occurred', 
                    src_file_load_finish_time = current_timestamp() 
                    where 
                    src_file_load_run_id={self.src_file_load_run_id}"""
        self.sparksession.sql(updateStmt)
          
        self.LogMessage("ERROR","Error occurred")
    #
    def Info(self,message_text):
        self.LogMessage("INFO",message_text)
    #
    #
    def Error(self,message_text):
        self.LogMessage("ERROR",message_text)
    #    
    def Warning(self,message_text):
        self.LogMessage("WARNING",message_text)
    #
    def Critical(self,message_text):
        self.LogMessage("CRITICAL",message_text)
    
    def LogMessage(self,message_type,message_text):
        if self.sparksession is None:
            return
        self.message_seq = self.message_seq+1
        self.message_time = datetime.now() 
        self.message_type = message_type
        self.message_text = message_text
                          
        msgStmt = f"""insert into {self.rep_db_name}.{self.load_mesg_table_name}
            (src_file_load_run_id,message_seq,message_type,message_time,message_text)
            values({self.src_file_load_run_id},{self.message_seq},'{self.message_type}',     
            current_timestamp(),
            '{message_text}')"""    
        #print(msgStmt)
        self.sparksession.sql(msgStmt)