#**************************************************************************************************
# Validate mapping definition to see if all the required fields are defined in STM
# for CSV data ingestion
#**************************************************************************************************
# Author              Date        Change Summary
#--------------------------------------------------------------------------------------------------
# Lachhaman Moharana  -           Initial version
#
#**************************************************************************************************
#**************************************************************************************************
# Validate mapping definition to see if all the required fields are defined in STM for Oracle 
# data ingestion
#**************************************************************************************************    
def validate_csv_mapping_def(p_mapping_json_def):
    l_req_def = [
                    "MappingName",
                    "SourceFileName",
                    "SourceSystemID",
                    "SourceFileID",
                    "SourceFilePattern",                    
                    "SourceFileType",
                    "SourceExtractType",
                    "HasHeaderRecord",
                    "RecordDelimiter",
                    "TargetTableName",
                    "TargetDatabaseName",
                    "TargetLoadType",
                    "LoadAllIncrementalFiles",
                    "LoadLatestFile",
                    "TextQualifier",
                    "Multiline",
                    "EscapeChar",
                    "DateFormat",
                    "TimestampFormat",
                    "SourceToTargetMapping"
                ]
    l_navl_def =""
    for lrd in l_req_def:
        if lrd not in p_mapping_json_def:
            l_navl_def = l_navl_def + " " + lrd
    #
    if l_navl_def != "":
        return l_navl_def
    #     
    if not isinstance(p_mapping_json_def["SourceToTargetMapping"], list) :
        return f"Source to Target Mapping must be a non-empty list."
    for stm_col in p_mapping_json_def["SourceToTargetMapping"]:
        l_req_stm = {"SourceColumnName", "SourceColumnID", "SourceDataType", "SourceDataFormat", "TargetColumnName", "TargetDataType"}
        if not l_req_stm.issubset(stm_col):            
            return f"Source to Target Mapping for column is incomplete, required mapping {l_req_stm}"    
    #
    return "valid"

#***************************************************************************************************
# Given the source to target mapping json object, build the reader options based on different params
#***************************************************************************************************
def get_csv_reader_options(p_stm_json : dict, p_infer_schema : bool) -> dict:
    l_options = dict()
    #
    if p_infer_schema:
        l_options.update({"inferSchema":True})
    #
    if p_stm_json.get("HasHeaderRecord").lower() == "yes":
        l_options.update({"header":True})
    #
    if p_stm_json.get("RecordDelimiter") is None or p_stm_json.get("RecordDelimiter") == "":
        l_options.update({"sep":","})
    else:
        l_options.update({"sep":p_stm_json.get("RecordDelimiter")})
    #
    if p_stm_json.get("TextQualifier") and p_stm_json.get("TextQualifier") != "":
        l_options.update({"quote":p_stm_json.get("TextQualifier")})
    #    
    if  p_stm_json.get("Multiline") and p_stm_json.get("Multiline") == "yes":
        l_options.update({"multiLine":True})
    #    
    if p_stm_json.get("EscapeChar") and p_stm_json.get("EscapeChar") !="":
        l_options.update({"escape":p_stm_json.get("EscapeChar")})    
    #
    if p_stm_json.get("DateFormat") and p_stm_json.get("DateFormat") != "":
        l_options.update({"dateFormat": p_stm_json.get("DateFormat") })
    #    
    if p_stm_json.get("TimestampFormat") and p_stm_json.get("TimestampFormat") != "":
        l_options.update({"timestampFormat": p_stm_json.get("TimestampFormat") })
    #
    if p_stm_json.get("IgnoreLeadingWhiteSpace") and p_stm_json.get("IgnoreLeadingWhiteSpace") == "yes":
        l_options.update({"ignoreLeadingWhiteSpace": True })
        #
    if p_stm_json.get("IgnoreTrailingWhiteSpace") and p_stm_json.get("IgnoreTrailingWhiteSpace") == "yes":
        l_options.update({"ignoreTrailingWhiteSpace": True})

    return l_options     