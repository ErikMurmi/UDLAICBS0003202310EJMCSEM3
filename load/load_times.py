import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def loadTimes(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        times_dict = {
            "TIME_ID":[],
            "DAY_NAME":[],
            "DAY_NUMBER_IN_WEEK":[],
            "DAY_NUMBER_IN_MONTH":[],
            "CALENDAR_WEEK_NUMBER":[],
            "CALENDAR_MONTH_NUMBER":[],
            "CALENDAR_MONTH_DESC":[],
            "END_OF_CAL_MONTH":[],
            "CALENDAR_MONTH_NAME":[],
            "CALENDAR_QUARTER_DESC":[],
            "CALENDAR_YEAR":[]
        }

        times_tra = pd.read_sql(f'SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM tra_times where LOAD_ID = {load_id}', ses_db_stg)

        times_loaded = pd.read_sql(f'SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,\
        DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC, \
        END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM dim_times', ses_db_sor)

        if not times_tra.empty:
            for id,nam,nwe,nmo,wnu,mnum,mdes,ecm,cqd,cye \
                in zip(times_tra["TIME_ID"],times_tra["DAY_NAME"],
                times_tra["DAY_NUMBER_IN_WEEK"],times_tra["DAY_NUMBER_IN_MONTH"],
                times_tra["CALENDAR_WEEK_NUMBER"],times_tra["CALENDAR_MONTH_NUMBER"],
                times_tra['CALENDAR_MONTH_DESC'],times_tra["END_OF_CAL_MONTH"],
                times_tra["CALENDAR_QUARTER_DESC"],times_tra["CALENDAR_YEAR"]):
                times_dict["TIME_ID"].append(id),
                times_dict["DAY_NAME"].append(nam),
                times_dict["DAY_NUMBER_IN_WEEK"].append(nwe),
                times_dict["DAY_NUMBER_IN_MONTH"].append(nmo),
                times_dict["CALENDAR_WEEK_NUMBER"].append(wnu),
                times_dict["CALENDAR_MONTH_NUMBER"].append(mnum),
                times_dict["CALENDAR_MONTH_DESC"].append(mdes),
                times_dict["END_OF_CAL_MONTH"].append(ecm),
                times_dict["CALENDAR_QUARTER_DESC"].append(cqd),
                times_dict["CALENDAR_MONTH_NAME"].append(mnum),
                times_dict["CALENDAR_YEAR"].append(cye)

        if times_dict["TIME_ID"]:
            df_times_load = pd.DataFrame(times_dict)
            df_times_merge = df_times_load.merge(times_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_times_merge.to_sql('dim_times',ses_db_sor,if_exists='append',index=False)
        print('Datos agregados: ',df_times_merge)
    except:
        traceback.print_exc()
    finally:
        pass