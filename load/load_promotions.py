import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def loadPromotions(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        promotions_dict = {
            "PROMO_ID":[],
            "PROMO_NAME":[],
            "PROMO_COST":[],
            "PROMO_BEGIN_DATE":[],
            "PROMO_END_DATE":[]
        }
        promotions_tra = pd.read_sql(f'SELECT PROMO_ID,PROMO_NAME,PROMO_COST,\
        PROMO_BEGIN_DATE,PROMO_END_DATE FROM tra_promotions where LOAD_ID={load_id}', ses_db_stg)
        promotions_loaded = pd.read_sql(f'SELECT PROMO_ID,PROMO_NAME,PROMO_COST,\
        PROMO_BEGIN_DATE,PROMO_END_DATE FROM dim_promotions', ses_db_sor)
        if not promotions_tra.empty:
            for id,nam,cos,beg_dat,end_dat\
                in zip(promotions_tra["PROMO_ID"],promotions_tra["PROMO_NAME"],
                promotions_tra["PROMO_COST"],promotions_tra["PROMO_BEGIN_DATE"],
                promotions_tra["PROMO_END_DATE"]):
                promotions_dict["PROMO_ID"].append(id),
                promotions_dict["PROMO_NAME"].append(nam),
                promotions_dict["PROMO_COST"].append(cos),
                promotions_dict["PROMO_BEGIN_DATE"].append(beg_dat),
                promotions_dict["PROMO_END_DATE"].append(end_dat)
        if promotions_dict["PROMO_ID"]:
            df_promotions_load = pd.DataFrame(promotions_dict)
            df_promotions_merge = df_promotions_load.merge(promotions_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_promotions_merge.to_sql('dim_promotions',ses_db_sor,if_exists='append',index=False)
        print('Datos agregados: ',df_promotions_merge)
    except:
        traceback.print_exc()
    finally:
        pass