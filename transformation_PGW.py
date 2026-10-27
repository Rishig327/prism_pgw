import apache_beam as beam
import re 
from LookupsManager import LookupManager
from datetime import datetime

COMMON_LOOKUPS = [
    "lk_VFUK_C_UC_EVENT_TYPE",
    "lk_VFUK_C_UC_SUBSCRIBER_TYPE",
    "lk_VFUK_C_UC_CALL_TYPE",
    "lk_VFUK_C_UC_SERVICE_TYPE",
    "lk_VFUK_C_UC_NETWORK_ID",
    "lk_VFUK_C_UC_MP_INFO",
    "lk_VFUK_C_UC_CALL_NO_GRP",
    "lk_VFUK_C_UC_FILTER_CODE",
    "lk_VFUK_C_UC_DURATION_STEP",
    "lk_VFUK_C_UC_PORTED",
    "lk_VFUK_C_UC_APN",
    "lk_VFUK_C_UC_EVENT_STATUS",
    "lk_VFUK_C_UC_PARTNER",
    "lk_VFUK_C_UC_SUBSCRIBER_STATUS",
]


def str_match_re(value: str, pattern: str) -> bool:
   return re.match(pattern, value or "") is not None

def is_set(value) -> bool:
   return value is not None and value != ""

def safe_number(value):
            try:
                if value is None or str(value).strip() == "":
                    return 0
                return float(str(value).strip())
            except Exception:
                return 0
def apply_common_lookups(out_row, **kwargs):

    lookup_mapping = {
        "DIM_EVENT_TYPE_ID": ("lk_VFUK_C_UC_EVENT_TYPE", kwargs.get("event_type")),
        "DIM_SUBSCRIBER_TYPE_ID": ("lk_VFUK_C_UC_SUBSCRIBER_TYPE", kwargs.get("subscriber_type")),
        "DIM_CALL_TYPE_ID": ("lk_VFUK_C_UC_CALL_TYPE", kwargs.get("call_type")),
        "DIM_SERVICE_TYPE_ID": ("lk_VFUK_C_UC_SERVICE_TYPE", kwargs.get("service_type")),
        "DIM_NETWORK_IN_ID": ("lk_VFUK_C_UC_NETWORK_ID", kwargs.get("network_in_id")),
        "DIM_NETWORK_OUT_ID": ("lk_VFUK_C_UC_NETWORK_ID", kwargs.get("network_out_id")),
        "DIM_ENTITY": ("lk_VFUK_C_UC_MP_INFO", kwargs.get("entity")),
        "DIM_FILTER_CODE_ID": ("lk_VFUK_C_UC_FILTER_CODE", kwargs.get("filter_code")),
        "DIM_PORTED_ID": ("lk_VFUK_C_UC_PORTED", kwargs.get("ported")),
        "DIM_APN": ("lk_VFUK_C_UC_APN", kwargs.get("apn"))
    }
   
    for target_field, (lookup_name, key) in lookup_mapping.items():
        if key not in (None, ""):
            result_lk_values = LookupManager.get_lookup(
                lookup_name,
                key,
                default={}
            )
            out_row[target_field] = result_lk_values.get("VALUE","")
 
class transform_record(beam.DoFn):

    def setup(self):

        #LookupManager.load_all()
        LookupManager.load_selected(COMMON_LOOKUPS + ["lk_VFUK_R_UC_APN_LIST", "lk_VFUK_R_UC_XGSN_IP_ADDRESS","lk_VFUK_R_UC_MVNO_IMSI"])
        #LookupManager.load_lookup("subscriber_type")



    def process(self, row):
        
        out_row={}

        ch_timeOfFirstUsages = str(row.get("ch_timeOfFirstUsage", "")).split("|") 
        ch_timeOfLastUsages = str(row.get("ch_timeOfLastUsage", "")).split("|")
        ch_timeUsages = str(row.get("ch_timeUsage", "")).split("|")
        ch_datavolumeFBCUplinks = str(row.get("ch_datavolumeFBCUplink", "")).split("|")
        ch_datavolumeFBCDownlinks = str(row.get("ch_datavolumeFBCDownlink", "")).split("|")
        ch_ratingGroups = str(row.get("ch_ratingGroup", "")).split("|")
        ch_servingNodeAddresss = str(row.get("ch_servingNodeAddress", "")).split("|")
        



        max_records = max(
            len(ch_timeOfLastUsages),
            len(ch_timeOfFirstUsages),
            len(ch_timeUsages),
            len(ch_datavolumeFBCUplinks),
            len(ch_datavolumeFBCDownlinks),
            len(ch_ratingGroups),
            len(ch_servingNodeAddresss)
        )

        ########### SERVED_MSISDN ################ 

        out_row["SERVED_MSISDN"] = row.get("servedMSISDN", "")

        SERVED_MSISDN = str(row.get("servedMSISDN", ""))

        '''SERVED_MSISDN = "".join(
            SERVED_MSISDN[i + 1] + SERVED_MSISDN[i]
            for i in range(0, len(SERVED_MSISDN) - 1, 2)
        ).rstrip("fF")'''

        out_row["SERVED_IMSI"] = SERVED_MSISDN


        ########### accessPointNameNI ################ 

        accessPointNameNI = row.get("accessPointNameNI", "")

        if accessPointNameNI == "blackberry.net":
            out_row["APN"] = "BLACKBERRY"

        elif accessPointNameNI == "pp.vodafone.co.uk":
            out_row["APN"] = "MOBINTERNET"

        elif accessPointNameNI in ("ppbundle.internet", "pp.internet"):
            out_row["APN"] = "MOBBROADBAND"

        else:
            out_row["APN"] = "_N"

        ####### For DIM_APN ##########

        if out_row["APN"] =="_N":
            temp_APN = "_UN"
        else:
            temp_APN = out_row["APN"]


        ########### CALL_TYPE ################ 

        out_row["CALL_TYPE"] = "GPRS"

        ########### RATTYPE ################ 

        out_row["RATTYPE"] = row.get("rATType", "")
                

        ########### EVENT_TYPE ################ 

        try:
            tmp_rATType = int(row.get("rATType", ""))
        except (ValueError, TypeError):
            tmp_rATType = 0

                
        if tmp_rATType == 1:
            out_row["EVENT_TYPE"] = "UTRAN"
        elif tmp_rATType == 2:
            out_row["EVENT_TYPE"] = "GERAN"
        elif tmp_rATType == 3:
            out_row["EVENT_TYPE"] = "WLAN"
        elif tmp_rATType == 4:
            out_row["EVENT_TYPE"] = "GAN"
        elif tmp_rATType == 5:
            out_row["EVENT_TYPE"] = "HSPA-E"
        elif tmp_rATType == 6:
            out_row["EVENT_TYPE"] = "EUTRAN"
        else:
            out_row["EVENT_TYPE"] = "_N"

        ########### CHARGING_CHARACTERISTICS ################ 

        out_row["CHARGING_CHARACTERISTICS"] = row.get("chargingCharacteristics", "")

        if out_row["CHARGING_CHARACTERISTICS"]  == "0800":
            temp_CHARGING_CHARACTERISTICS = "NOT_PREPAID"
        elif out_row["CHARGING_CHARACTERISTICS"]  == "0400":
            temp_CHARGING_CHARACTERISTICS = "PREPAID"
        else:
            temp_CHARGING_CHARACTERISTICS = out_row["CHARGING_CHARACTERISTICS"]
        



        ########### SGSN_ADDRESS ################ 
        
        out_row["SGSN_ADDRESS"] = row.get("servingNodeAddress", "")

        result_lk_VFUK_R_UC_XGSN_IP_ADDRESS = LookupManager.get_lookup(
            "lk_VFUK_R_UC_XGSN_IP_ADDRESS",
            out_row["SGSN_ADDRESS"],
            default={}
            )
       
        sgsn_address_found = result_lk_VFUK_R_UC_XGSN_IP_ADDRESS.get("VALUE","FALSE")
        
        ########### GGSN_ADDRESS ################ 
        
        #out_row["GGSN_ADDRESS"] = row.get("p-GWAddress", "")
        

        ########### SERVED_IMSI ################ 
        
        
        served_imsi = str(row.get("servedIMSI", ""))

        '''served_imsi = "".join(
            served_imsi[i + 1] + served_imsi[i]
            for i in range(0, len(served_imsi) - 1, 2)
        ).rstrip("fF")'''

        out_row["SERVED_IMSI"] = served_imsi

        

        ########### SESSION_ID ################ 
        
        out_row["SESSION_ID"] = row.get("chargingID", "")
        
        ########### DIM_FILENAME and NE_ID and Network_Out ################ 
        filename = str(row.get("filename", ""))
        
        out_row["FILENAME"] = filename
        out_row["filename"] = filename
        
        out_row["NE_ID"] = filename[:6]

        out_row["NETWORK_OUT"] = filename[:6]
        out_row["NETWORK_IN"] = "_UN"
                
        

        out_row["MES_SEQ_NUM"] = filename.split("file")[-1].split(".")[0]
        



        ########### ACCESS_POINT_NAME_NI ################ 
        
        APN = row.get("accessPointNameNI", "")

        out_row["ACCESS_POINT_NAME_NI"] = APN

        APN_TYPE = "PREPAY"
        NETWORK = "VFUK"
        
        result_lk_VFUK_R_UC_APN_LIST = LookupManager.get_lookup(
            "lk_VFUK_R_UC_APN_LIST",
            APN,
            APN_TYPE,
            NETWORK,
            default={"VALUE": "_UN"}
            )

        APN_found = result_lk_VFUK_R_UC_APN_LIST.get("VALUE","FALSE")

        ########### SUBSCRIBER_TYPE ################ 

        out_row["SUBSCRIBER_TYPE"] = "_UN"

        result_lk_VFUK_R_UC_MVNO_IMSI = LookupManager.get_lookup(
            "lk_VFUK_R_UC_MVNO_IMSI",
            served_imsi,
            default={}
            )
       
        MVNO_VALUE = result_lk_VFUK_R_UC_MVNO_IMSI.get("MVNO","")
      
        #Case 1

        if APN_found =="TRUE":
            if sgsn_address_found =="TRUE":
                out_row["SUBSCRIBER_TYPE"] = "VFUK-PRE"
            else:
                out_row["SUBSCRIBER_TYPE"] = "ROAMERS-PRE"


        ########### MES_CDR_COUNT and MES_EVENT_COUNT  ################ 
        
        out_row["MES_CDR_COUNT"] = "1"
        
        out_row["MES_EVENT_COUNT"] = "1"      

        ########### DIM_PORTED_ID  ################
        
        out_row["PORTED"] = "_UN"     

        ####### Based on Rating Group writing multiple cdr ########


        for i in range(max_records):


            
            startTime = ch_timeOfFirstUsages[i] if i < len(ch_timeOfFirstUsages) and ch_timeOfFirstUsages[i] else row.get("recordOpeningTime", "")
            endTime = ch_timeOfLastUsages[i] if i < len(ch_timeOfLastUsages) and ch_timeOfLastUsages[i] else row.get("recordOpeningTime", "")

            ch_timeUsage = ch_timeUsages[i] if i < len(ch_timeUsages) and ch_timeUsages[i] else "0"
            ch_datavolumeFBCUplink = ch_datavolumeFBCUplinks[i] if i < len(ch_datavolumeFBCUplinks) and ch_datavolumeFBCUplinks[i] else "0"
            ch_datavolumeFBCDownlink = ch_datavolumeFBCDownlinks[i] if i < len(ch_datavolumeFBCDownlinks) and ch_datavolumeFBCDownlinks[i] else "0"
            ch_ratingGroup = ch_ratingGroups[i] if i < len(ch_ratingGroups) and ch_ratingGroups[i] else "0"
            ch_servingNodeAddress = ch_servingNodeAddresss[i] if i < len(ch_servingNodeAddresss) and ch_servingNodeAddresss[i] else row.get("p-GWAddress", "")

            ########### GGSN_ADDRESS ################ 
            
            out_row["GGSN_ADDRESS"] = ch_servingNodeAddress
            
            ########### startTime ################        
            

            if len(startTime) >= 12:
                out_row["STARTTIME"] = ( "20" + startTime[0:2] + "-" + startTime[2:4] + "-" + startTime[4:6] + "-" + startTime[6:12]            )
            else:
                out_row["STARTTIME"] = "1999-01-01-000000"

            
            
            

            out_row["EVT_DATE"] = out_row.get("STARTTIME", "")[:10]

                      


            ########### endTime and PROC_DATE and  PROC_HOUR ################        

            if len(endTime) >= 12:
                out_row["ENDTIME"] = ( "20" + endTime[0:2] + "-" + endTime[2:4] + "-" + endTime[4:6] + "-" + endTime[6:12] )
            else:
                out_row["ENDTIME"] = "1999-01-01-000000"

            out_row["DIM_END_DATE"] = out_row.get("ENDTIME", "")[:10].replace("-", "")  
            out_row["PROC_DATE"] = out_row.get("ENDTIME", "")[:10]

            out_row["PROC_HOUR"] = out_row.get("ENDTIME")[11:13]


            #########################################
            
            out_row["MES_EVENT_DURATION"] = ch_timeUsage 
            out_row["RATING_GROUP"] = ch_ratingGroup

            if str(ch_ratingGroup).isdigit() and 4 <= int(ch_ratingGroup) <= 12:
                out_row["TRAFF_CAT_900"] = f"900{ch_ratingGroup}"
            else:
                out_row["TRAFF_CAT_900"] = "0"
            


            out_row["MES_UPLINK_VOLUME"] = ch_datavolumeFBCUplink 
            out_row["MES_DOWNLINK_VOLUME"] = ch_datavolumeFBCDownlink
            out_row["MES_CHARGED_AMOUNT"] =round((int(ch_datavolumeFBCUplink or 0) + int(ch_datavolumeFBCDownlink or 0 )) / 1024 * 0.1 , 5)

            out_row["MES_DATA_VOLUME"] = int(ch_datavolumeFBCUplink or 0) +  int(ch_datavolumeFBCDownlink or 0 )

            ################## FILTER_CODE #######################

            if safe_number(ch_ratingGroup) >=6000:
                out_row["FILTER_CODE"] = "BILLABLE"
            else:
                if sgsn_address_found == "TRUE":
                    out_row["FILTER_CODE"] = "FILTERED-UK"
                else:
                    out_row["FILTER_CODE"] = "FILTERED-ROAM"

            ################## SERVICE_TYPE #######################
            
            out_row["SERVICE_TYPE"]=""

            if accessPointNameNI =="blackberry.net" and sgsn_address_found == "TRUE":
                if temp_CHARGING_CHARACTERISTICS == "PREPAID":
                    out_row["SERVICE_TYPE"] == out_row.get("SERVICE_TYPE","") + "INFO-BB-ROAMER-PRE|"
                else:
                    out_row["SERVICE_TYPE"] == out_row.get("SERVICE_TYPE","") + "INFO-BB-ROAMER-POS|"
            else:
                if accessPointNameNI =="blackberry.net" and sgsn_address_found == "FALSE":
                    if temp_CHARGING_CHARACTERISTICS == "PREPAID":
                        out_row["SERVICE_TYPE"] == out_row.get("SERVICE_TYPE","") + "INFO-BB-UK-PRE|"
                    else:
                        out_row["SERVICE_TYPE"] == out_row.get("SERVICE_TYPE","") + "INFO-BB-UK-POS|"


            aux_rating_group = safe_number(row.get("ch_ratingGroup", 0))
            aux_data_up = safe_number(row.get("ch_datavolumeFBCUplink", 0))
            aux_data_down = safe_number(row.get("ch_datavolumeFBCDownlink", 0))

            
            allowed_apn = ("internet", "wap.vodafone.co.uk")

            if (aux_rating_group <= 12999  and (aux_data_up > 0 or aux_data_down > 0) and APN in allowed_apn):
                out_row["SERVICE_TYPE"] = (str(out_row.get("SERVICE_TYPE", "")) + "VFUK_MOBILE_BROWSING_FREE|")
            else:
                if (aux_rating_group <= 5999 and (aux_data_up > 0 or aux_data_down > 0) and APN in allowed_apn):
                    out_row["SERVICE_TYPE"] = (str(out_row.get("SERVICE_TYPE", ""))+ "ROAMERS_MOBILE_BROWSING_FREE|")


            if (aux_data_up == 0 and aux_data_down == 0 and APN in allowed_apn):
                out_row["SERVICE_TYPE"] = (str(out_row.get("SERVICE_TYPE", "")) + "MOBILE_BROWSING_VOID|")


            if (APN not in allowed_apn and APN_found == "FALSE"):
                out_row["SERVICE_TYPE"] = (str(out_row.get("SERVICE_TYPE", "")) + "NON_MOBILE_BROWSING|")

            if(len(out_row["SERVICE_TYPE"]) < 2 or out_row["SERVICE_TYPE"] ==""):
                out_row["SERVICE_TYPE"] ="_UN"

        ########### SUBSCRIBER_TYPE ################ 
            #Case 2

            if (aux_data_up > 0 or aux_data_down > 0) and APN in allowed_apn:
                if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                    out_row["SUBSCRIBER_TYPE"] = "VFUK-POS"
                elif sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                    out_row["SUBSCRIBER_TYPE"] = "ROAMERS-POS"

            #case3

            if MVNO_VALUE =="DIGITALK":
                if accessPointNameNI == "Gprsconnect.co.uk":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-DIGITALK-PRE-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-DIGITALK-PRE-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-DIGITALK-PRE" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-DIGITALK"

            elif MVNO_VALUE =="LEBARA":
                if accessPointNameNI == "uk.lebara.mobi":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-LEBARA-PRE-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-LEBARA-PRE-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-LEBARA-PRE" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-LEBARA"
                            
            elif MVNO_VALUE =="TKMOB":
                if accessPointNameNI == "talkmobile.co.uk":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-TKMOB-POS-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-TKMOB-POS-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-TKMOB-POS" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-TKMOB"

            elif MVNO_VALUE =="TOMTOM":
                if accessPointNameNI == "tomtom.com":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-TOMTOM-POS-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-TOMTOM-POS-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-TOMTOM-POS"
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-TOMTOM"     

            elif MVNO_VALUE =="TALKTALK":
                if accessPointNameNI == "mobile.talktalk.co.uk":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-TALKTALK-POS-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-TALKTALK-POS-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-TALKTALK-POS" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-TALKTALK"

            elif MVNO_VALUE =="ASDA":
                if accessPointNameNI == "my.internet":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-ASDA-PRE-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-ASDA-PRE-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-ASDA-PRE" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-ASDA"

            elif MVNO_VALUE =="VENUS":
                if accessPointNameNI == "my.internet":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-VENUS-PRE-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-VENUS-PRE-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-VENUS-PRE" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-VENUS"

            elif MVNO_VALUE =="IVY":
                if accessPointNameNI == "uk.data.esim":
                    if sgsn_address_found =="TRUE" and aux_rating_group > 12999:
                        out_row["SUBSCRIBER_TYPE"] = "MVNO-IVY-PRE-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-IVY-PRE-ROAM"
                        else:
                            out_row["SUBSCRIBER_TYPE"] = "MVNO-IVY-PRE" 
                else:
                    out_row["SUBSCRIBER_TYPE"] = "MVNO-IVY"

            elif MVNO_VALUE =="":
                if accessPointNameNI in ("mec.vodafone.south","mec.vodafone.north","mec.vodafone.co.uk"):
                    if sgsn_address_found =="TRUE" and aux_rating_group > 5998:
                        out_row["SUBSCRIBER_TYPE"] = "MEC-UK"
                    else:   
                        if sgsn_address_found =="FALSE" and aux_rating_group > 5999:
                            out_row["SUBSCRIBER_TYPE"] = "MEC-ROAM"

            else:
                if (sgsn_address_found =="TRUE" and aux_rating_group > 12999) or (sgsn_address_found =="FALSE" and aux_rating_group > 5999):
                    
                    APN_2 = row.get("accessPointNameNI", "")
                    APN_TYPE_2 = "PREPAY"
                    NETWORK_2 = "MVNO"
                    
                    result_lk_VFUK_R_UC_APN_LIST_2 = LookupManager.get_lookup(
                        "lk_VFUK_R_UC_APN_LIST",
                        APN_2,
                        APN_TYPE_2,
                        NETWORK_2,
                        default={}
                        )

                    if result_lk_VFUK_R_UC_APN_LIST_2.get("VALUE","") == "TRUE":
                        APN_found_2 = "MNVO"
                    else:
                        APN_found_2 = ""
                    
                    APN_3 = row.get("accessPointNameNI", "")
                    APN_TYPE_3 = "POS"
                    NETWORK_3 = "MVNO"
                    
                    result_lk_VFUK_R_UC_APN_LIST_3 = LookupManager.get_lookup(
                        "lk_VFUK_R_UC_APN_LIST",
                        APN_3,
                        APN_TYPE_3,
                        NETWORK_3,
                        default={}
                        )

                    if result_lk_VFUK_R_UC_APN_LIST_3.get("VALUE","") == "TRUE":
                        APN_found_3 = "MNVO"
                    else:
                        APN_found_3 = ""

                    if APN_found_2 =="MVNO" or APN_found_3 =="MNVO": 
                        out_row["SUBSCRIBER_TYPE"] = "MVNO"
                    else:
                        out_row["SUBSCRIBER_TYPE"] = "_UN"

            apply_common_lookups(
                out_row,
                event_type=out_row["EVENT_TYPE"],
                subscriber_type=out_row["SUBSCRIBER_TYPE"],
                service_type=out_row["SERVICE_TYPE"],
                call_type=out_row["CALL_TYPE"],
                network_in_id=out_row["NETWORK_IN"],
                network_out_id=out_row["NETWORK_OUT"],
                entity='M100_SWITCHES_PGW',
                filter_code=out_row["FILTER_CODE"],
                ported=out_row.get("PORTED", "_UN"),
                apn=temp_APN
                )

            out_row["DIM_OWNER"] = 0
            out_row["DIM_SUBSLOC"] = 0
            out_row["FIELD1"] = 0
            out_row["FIELD2"] = 0
            out_row["FIELD3"] = 0
            out_row["FIELD4"] = 0
            out_row["FIELD5"] = 0
            out_row["FIELD6"] = 0
            out_row["FIELD7"] = 0
            out_row["FIELD8"] = 0
            

            
            yield dict(out_row)   # PRISM: copy - out_row is reused/mutated for every rating-group row
