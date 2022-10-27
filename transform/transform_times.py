import traceback
from util.db_connection import Db_Connection
import pandas as pd 
from transform.transforms import date_str_month

def extTimes(load_id):
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        times_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "end_of_cal_month":[],
            "calendar_quarter_desc":[],
            "calendar_year":[],
            "LOAD_ID":[]
        }

        times_tra = pd.read_sql('SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,\
        DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC \
        END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times', ses_db_stg)

        if not times_tra.empty:
            for id,nam,nwe,nmo,wnu,mnum,ecm,cqd,cye \
                in zip(times_tra["TIME_ID"],times_tra["DAY_NAME"],
                times_tra["DAY_NUMBER_IN_WEEK"],times_tra["DAY_NUMBER_IN_MONTH"],
                times_tra["CALENDAR_WEEK_NUMBER"],times_tra["CALENDAR_MONTH_NUMBER"]
                ,times_tra["END_OF_CAL_MONTH"],
                times_tra["CALENDAR_QUARTER_DESC"],times_tra["CALENDAR_YEAR"]):
                times_dict["time_id"].append(date_str_month(id)),
                times_dict["day_name"].append(nam),
                times_dict["day_number_in_week"].append(nwe),
                times_dict["day_number_in_month"].append(nmo),
                times_dict["calendar_week_number"].append(wnu),
                times_dict["calendar_month_number"].append(mnum),
                times_dict["end_of_cal_month"].append(ecm),
                times_dict["calendar_quarter_desc"].append(cqd),
                times_dict["calendar_year"].append(cye),
                times_dict["LOAD_ID"].append(load_id)

        if times_dict["time_id"]:
            df_times_tra = pd.DataFrame(times_dict)
            df_times_tra.to_sql('times',ses_db_stg,if_exists='append',index=False)
        print(df_times_tra)
    except:
        traceback.print_exc()
    finally:
        pass