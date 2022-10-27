import traceback
from sqlalchemy import column
from util.db_connection import Db_Connection
import pandas as pd 

def transformCustomers(load_id):
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")

        customers_dict = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[],
            "LOAD_ID":[]
        }
        
        customers_tra = pd.read_sql('SELECT CUST_ID,CUST_FIRST_NAME,CUST_LAST_NAME,\
        CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE, \
        CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL, \
        CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers', ses_db_stg)
        if not customers_tra.empty:
            for id,fir_nam,las_nam,gen,bir,sta,add,cod,cit,prov,ctr_id,pho,inc,lim,ema \
               in zip(customers_tra["CUST_ID"],customers_tra["CUST_FIRST_NAME"]
                ,customers_tra["CUST_LAST_NAME"],customers_tra["CUST_GENDER"]
                ,customers_tra["CUST_YEAR_OF_BIRTH"],customers_tra["CUST_MARITAL_STATUS"]
                ,customers_tra["CUST_STREET_ADDRESS"],customers_tra["CUST_POSTAL_CODE"]
                ,customers_tra["CUST_CITY"],customers_tra["CUST_STATE_PROVINCE"]
                ,customers_tra["COUNTRY_ID"],customers_tra["CUST_MAIN_PHONE_NUMBER"]
                ,customers_tra["CUST_INCOME_LEVEL"],customers_tra["CUST_CREDIT_LIMIT"],
                customers_tra["CUST_EMAIL"]):
                customers_dict["cust_id"].append(id),
                customers_dict["cust_first_name"].append(fir_nam),
                customers_dict["cust_last_name"].append(las_nam),
                customers_dict["cust_gender"].append(gen),
                customers_dict["cust_year_of_birth"].append(bir),
                customers_dict["cust_marital_status"].append(sta),
                customers_dict["cust_street_address"].append(add),
                customers_dict["cust_postal_code"].append(cod),
                customers_dict["cust_city"].append(cit),
                customers_dict["cust_state_province"].append(prov),
                customers_dict["country_id"].append(ctr_id),
                customers_dict["cust_main_phone_number"].append(pho),
                customers_dict["cust_income_level"].append(inc),
                customers_dict["cust_credit_limit"].append(lim),
                customers_dict["cust_email"].append(ema),
                customers_dict["LOAD_ID"].append(load_id)

        if customers_dict["cust_id"]:
            df_customers_tra = pd.DataFrame(customers_dict)
            df_customers_tra.to_sql('tra_customers',ses_db_stg,if_exists='append',index=False)
        print(df_customers_tra)
    except:
        traceback.print_exc()
    finally:
        pass