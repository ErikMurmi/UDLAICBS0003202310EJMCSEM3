import traceback
from util.db_connection import Db_Connection
import pandas as pd 


def extPromotions():
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        promotions_csv = pd.read_csv("data/promotions.csv")
        print(promotions_csv)
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }

        if not promotions_csv.empty:
            for id,nam,cos,beg_dat,end_dat\
                in zip(promotions_csv["PROMO_ID"],promotions_csv["PROMO_NAME"],
                promotions_csv["PROMO_COST"],promotions_csv["PROMO_BEGIN_DATE"],
                promotions_csv["PROMO_END_DATE"]):
                promotions_dict["promo_id"].append(id),
                promotions_dict["promo_name"].append(nam),
                promotions_dict["promo_cost"].append(cos),
                promotions_dict["promo_begin_date"].append(beg_dat),
                promotions_dict["promo_end_date"].append(end_dat)

        if promotions_dict["promo_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE PROMOTIONS")
            df_promotions_ext = pd.DataFrame(promotions_dict)
            df_promotions_ext.to_sql('promotions',ses_db_stg,if_exists='append',index=False)
        print(promotions_csv)
    except:
        traceback.print_exc()
    finally:
        pass