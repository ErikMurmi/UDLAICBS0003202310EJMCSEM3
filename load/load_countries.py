import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def load_countries(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        countries_dict = {
            "COUNTRY_ID":[],
            "COUNTRY_NAME":[],
            "COUNTRY_REGION":[],
            "COUNTRY_REGION_ID":[]
        }

        countries_tra = pd.read_sql(f'SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,\
        COUNTRY_REGION_ID FROM tra_countries where LOAD_ID={load_id}', ses_db_stg)

        countries_loaded = pd.read_sql('SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,\
        COUNTRY_REGION_ID FROM dim_countries',ses_db_sor)

        if not countries_tra.empty:
            for id,nam,reg,reg_id \
                in zip(countries_tra["COUNTRY_ID"],countries_tra["COUNTRY_NAME"],
                countries_tra["COUNTRY_REGION"],countries_tra["COUNTRY_REGION_ID"]):
                countries_dict["COUNTRY_ID"].append(id),
                countries_dict["COUNTRY_NAME"].append(nam),
                countries_dict["COUNTRY_REGION"].append(reg),
                countries_dict["COUNTRY_REGION_ID"].append(reg_id)

        if countries_dict["COUNTRY_ID"]:
            df_countries_load = pd.DataFrame(countries_dict)
            df_countries_merge = df_countries_load.merge(countries_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_countries_merge.to_sql('dim_countries',ses_db_sor,if_exists='append',index=False)
        print('Datos agregados: ',df_countries_merge)
    except:
        traceback.print_exc()
    finally:
        pass