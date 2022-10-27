import traceback
from util.db_connection import Db_Connection
from transform.transforms import *
import pandas as pd 



def transformPromotions(load_id):
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "LOAD_ID":[]
        }
        promotions_tra = pd.read_sql('SELECT PROMO_ID,PROMO_NAME,PROMO_COST,\
        PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions', ses_db_stg)
        if not promotions_tra.empty:
            for id,nam,cos,beg_dat,end_dat\
                in zip(promotions_tra["PROMO_ID"],promotions_tra["PROMO_NAME"],
                promotions_tra["PROMO_COST"],promotions_tra["PROMO_BEGIN_DATE"],
                promotions_tra["PROMO_END_DATE"]):
                promotions_dict["promo_id"].append(id),
                promotions_dict["promo_name"].append(nam),
                promotions_dict["promo_cost"].append(str_float_format(cos)),
                promotions_dict["promo_begin_date"].append(date_str_month(beg_dat)),
                promotions_dict["promo_end_date"].append(date_str_month(end_dat)),
                promotions_dict["LOAD_ID"].append(load_id)

        if promotions_dict["promo_id"]:
            df_promotions_tra = pd.DataFrame(promotions_dict)
            df_promotions_tra.to_sql('tra_promotions',ses_db_stg,if_exists='append',index=False)
        print(df_promotions_tra)
    except:
        traceback.print_exc()
    finally:
        pass