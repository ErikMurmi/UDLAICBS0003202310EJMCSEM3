import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def extCountries():
    try:

        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        countries_csv = pd.read_csv("data/countries.csv")
        
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }

        if not countries_csv.empty:
            for id,nam,reg,reg_id \
                in zip(countries_csv["COUNTRY_ID"],countries_csv["COUNTRY_NAME"],
                countries_csv["COUNTRY_REGION"],countries_csv["COUNTRY_REGION_ID"]):
                countries_dict["country_id"].append(id),
                countries_dict["country_name"].append(nam),
                countries_dict["country_region"].append(reg),
                countries_dict["country_region_id"].append(reg_id)

        if countries_dict["country_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE COUNTRIES")
            df_countries_ext = pd.DataFrame(countries_dict)
            df_countries_ext.to_sql('countries',ses_db_stg,if_exists='append',index=False)
        print(countries_csv)
    except:
        traceback.print_exc()
    finally:
        pass