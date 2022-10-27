from util.db_connection import Db_Connection
import pandas as pd
import traceback

def load_customers(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging database")

        customers_dict = {
            "CUST_ID":[],
            "CUST_FIRST_NAME":[],
            "CUST_LAST_NAME":[],
            "CUST_GENDER":[],
            "CUST_YEAR_OF_BIRTH":[],
            "CUST_MARITAL_STATUS":[],
            "CUST_STREET_ADDRESS":[],
            "CUST_POSTAL_CODE":[],
            "CUST_CITY":[],
            "CUST_STATE_PROVINCE":[],
            "COUNTRY_ID":[],
            "CUST_MAIN_PHONE_NUMBER":[],
            "CUST_INCOME_LEVEL":[],
            "CUST_CREDIT_LIMIT":[],
            "CUST_EMAIL":[],
        }

        customers_tra=pd.read_sql(f"SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME, CUST_GENDER, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL, CUST_CREDIT_LIMIT, CUST_EMAIL from tra_customers where LOAD_ID={load_id}", ses_db_stg)
        customers_loaded=pd.read_sql(f"SELECT COUNTRY_SOR_ID, COUNTRY_ID FROM dim_countries", ses_db_sor)

        cust_dict=dict()
        if not customers_loaded.empty:
            for id, cou_id \
                in zip(customers_loaded['COUNTRY_SOR_ID'], customers_loaded['COUNTRY_ID']):
                cust_dict[cou_id] = id

        if not customers_tra.empty:
            for id,nom,last,gen,yea,mar,str,pos,cit,sta,cou,mai,inc,cre,ema  \
                in zip(customers_tra['CUST_ID'],customers_tra['CUST_FIRST_NAME'],
                customers_tra['CUST_LAST_NAME'],
                customers_tra['CUST_GENDER'],
                customers_tra['CUST_YEAR_OF_BIRTH'], customers_tra['CUST_MARITAL_STATUS'],
                customers_tra['CUST_STREET_ADDRESS'], customers_tra['CUST_POSTAL_CODE'],
                customers_tra['CUST_CITY'], customers_tra['CUST_STATE_PROVINCE'],
                customers_tra['COUNTRY_ID'], customers_tra['CUST_MAIN_PHONE_NUMBER'],
                customers_tra['CUST_INCOME_LEVEL'], customers_tra['CUST_CREDIT_LIMIT'],
                customers_tra['CUST_EMAIL']):
                customers_dict["CUST_ID"].append(id)
                customers_dict["CUST_FIRST_NAME"].append(nom)
                customers_dict["CUST_LAST_NAME"].append(last)
                customers_dict["CUST_GENDER"].append(gen)
                customers_dict["CUST_YEAR_OF_BIRTH"].append(yea)
                customers_dict["CUST_MARITAL_STATUS"].append(mar)
                customers_dict["CUST_STREET_ADDRESS"].append(str)
                customers_dict["CUST_POSTAL_CODE"].append(pos)
                customers_dict["CUST_CITY"].append(cit)
                customers_dict["CUST_STATE_PROVINCE"].append(sta)
                customers_dict["COUNTRY_ID"].append(cust_dict[cou])
                customers_dict["CUST_MAIN_PHONE_NUMBER"].append(mai)
                customers_dict["CUST_INCOME_LEVEL"].append(inc)
                customers_dict["CUST_CREDIT_LIMIT"].append(cre)
                customers_dict["CUST_EMAIL"].append(ema)


        if customers_dict['CUST_ID']:
            df_customers_load=pd.DataFrame(customers_dict)
            df_customers_merge = df_customers_load.merge(customers_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_customers_merge.to_sql('dim_customers', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass