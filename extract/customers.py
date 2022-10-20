import traceback

from sqlalchemy import column
from util.db_connection import Db_Connection
import pandas as pd 

def extCustomers():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'admin'
        db = 'ejmcdbstg'

        con_db_stg = Db_Connection(type,host,port,user,pwd,db)
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        customers_csv = pd.read_csv("data/customers.csv")
        columns_names = list(customers_csv.columns)
        #print(columns_names)
        #customers_dict = dict.fromkeys(columns_names,[])
        #print(customers_dict)
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
            "cust_email":[]
        }
        
        if not customers_csv.empty:
            for id,fir_nam,las_nam,gen,bir,sta,add,cod,cit,prov,ctr_id,pho,inc,lim,ema \
               in zip(customers_csv["CUST_ID"],customers_csv["CUST_FIRST_NAME"]
                ,customers_csv["CUST_LAST_NAME"],customers_csv["CUST_GENDER"]
                ,customers_csv["CUST_YEAR_OF_BIRTH"],customers_csv["CUST_MARITAL_STATUS"]
                ,customers_csv["CUST_STREET_ADDRESS"],customers_csv["CUST_POSTAL_CODE"]
                ,customers_csv["CUST_CITY"],customers_csv["CUST_STATE_PROVINCE"]
                ,customers_csv["COUNTRY_ID"],customers_csv["CUST_MAIN_PHONE_NUMBER"]
                ,customers_csv["CUST_INCOME_LEVEL"],customers_csv["CUST_CREDIT_LIMIT"],
                customers_csv["CUST_EMAIL"]):
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


        if customers_dict["cust_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE CUSTOMERS")
            df_customers_ext = pd.DataFrame(customers_dict)
            df_customers_ext.to_sql('customers',ses_db_stg,if_exists='append',index=False)
        print(customers_csv)
    except:
        traceback.print_exc()
    finally:
        pass