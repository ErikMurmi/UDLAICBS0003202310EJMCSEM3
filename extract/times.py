import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def extTimes():
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        times_csv = pd.read_csv("data/times.csv")
        
        times_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }

        if not times_csv.empty:
            for id,nam,nwe,nmo,wnu,mnum,mdes,ecm,cqd,cye \
                in zip(times_csv["TIME_ID"],times_csv["DAY_NAME"],
                times_csv["DAY_NUMBER_IN_WEEK"],times_csv["DAY_NUMBER_IN_MONTH"],
                times_csv["CALENDAR_WEEK_NUMBER"],times_csv["CALENDAR_MONTH_NUMBER"],
                times_csv["CALENDAR_MONTH_DESC"],times_csv["END_OF_CAL_MONTH"],
                times_csv["CALENDAR_QUARTER_DESC"],times_csv["CALENDAR_YEAR"]):
                times_dict["time_id"].append(id),
                times_dict["day_name"].append(nam),
                times_dict["day_number_in_week"].append(nwe),
                times_dict["day_number_in_month"].append(nmo),
                times_dict["calendar_week_number"].append(wnu),
                times_dict["calendar_month_number"].append(mnum),
                times_dict["calendar_month_desc"].append(mdes),
                times_dict["end_of_cal_month"].append(ecm),
                times_dict["calendar_quarter_desc"].append(cqd),
                times_dict["calendar_year"].append(cye)

        if times_dict["time_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE TIMES")
            df_times_ext = pd.DataFrame(times_dict)
            df_times_ext.to_sql('times',ses_db_stg,if_exists='append',index=False)
        print(times_csv)
    except:
        traceback.print_exc()
    finally:
        pass