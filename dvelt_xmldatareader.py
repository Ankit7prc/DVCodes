from xml.etree.ElementTree import XMLParser
import json
#
class XmlDataReader:                     
    #
    __tempDataRecord = {}
    __dataRecordSets = {}
    __xpathPrefix = None
    __xpathTokens = []
    __recordXPaths = {}
    #
    #**************************************************************
    def prepareReader(self,p_xmlConfig:list):
        for varx in p_xmlConfig["RecordSets"]:
            self.__recordXPaths.update({varx["RecordXPath"]:varx})
            self.__dataRecordSets.update({varx["RecordXPath"]:[]}) 
        self.__xpathPrefix = p_xmlConfig["NSPrefix"]         
    #**************************************************************
    def __processRecord(self):
        l_xpath = self.__getXPath()
        #
        if l_xpath not in self.__recordXPaths.keys():
            return
        #
        l_dataRecord = {}
        #
        l_outColumns = self.__recordXPaths[l_xpath]["Columns"]
        #        
        for outColumn in l_outColumns:
            l_dataRecord.update({outColumn["ColumnName"]:None})
            if outColumn["ColumnXPath"] in self.__tempDataRecord:
                l_dataRecord.update({outColumn["ColumnName"]:self.__tempDataRecord[outColumn["ColumnXPath"]]})
                if outColumn["ResetOnRecordOut"] == "yes":                    
                    self.__tempDataRecord[outColumn["ColumnXPath"]] = None
        if any(l_dataRecord.values()):
            self.__dataRecordSets[l_xpath].append(l_dataRecord)                
    #************************************************************
    def __getXPath(self):
        l_xpath = "/".join(self.__xpathTokens)   
        l_xpath = l_xpath.replace(self.__xpathPrefix,"") 
        return l_xpath    
    #************************************************************ Update the data to temporary record
    def __updateData(self,p_data):
        l_xpath = self.__getXPath()   
        self.__tempDataRecord.update({l_xpath:p_data})           
    #************************************************************ Called for each opening tag by the parser
    #************************************************************ Never change the signature
    def start(self, tag, attrib):   
        self.__xpathTokens.append(tag)
    #************************************************************ Called for each closing tag by the parser
    #************************************************************ Never change the signature
    def end(self, tag):              
        self.__processRecord()
        self.__xpathTokens.pop()
    #*********************************************************** Called for each data element by the parser
    #*********************************************************** Never change the signature
    def data(self, data):
        self.__updateData(p_data=data)        
    #********************************************************** Called when all data has been parsed
    #********************************************************** Never change the signature
    def close(self):     
        #pass
        for rsk in self.__dataRecordSets:
            print(rsk,"Record Count",len(self.__dataRecordSets[rsk]))
        #    
    #********************************************************* Get the recordset keys if available after processing all data
    def getRecordsetNames(self)->list:
        return [rk  for rk in self.__dataRecordSets if len(self.__dataRecordSets[rk])>0]
    #********************************************************* Get the default schema of a recordset
    def getDefaultSchema(self,p_recordsetKey)->str:
        l_cols = [xcol["ColumnName"] + " string" for xcol in self.__recordXPaths[p_recordsetKey]["Columns"]]
        return ",".join(l_cols)
    #********************************************************* Get the recordset for the given key
    def getKeyColumns(self,p_recordsetKey)->dict:
        return self.__recordXPaths[p_recordsetKey]["KeyColumns"]
    #*********************************************************** Get all the columns
    def getAllColumns(self,p_recordsetKey)->list:
        l_cols = [xcol["ColumnName"] for xcol in self.__recordXPaths[p_recordsetKey]["Columns"]]
        return l_cols
    #*********************************************************** Get the Json object representing keyColumns for the recordset
    def getRecordset(self,p_recordsetKey)->dict:
        if p_recordsetKey in self.__dataRecordSets:
            return self.__dataRecordSets[p_recordsetKey]
        else:
            return None
