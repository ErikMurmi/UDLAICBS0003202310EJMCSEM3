import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def loadSales(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        sales_tra = pd.read_sql(f'SELECT PROD_ID,CUST_ID,TIME_ID,\
        CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales where LOAD_ID={load_id}', ses_db_stg)

        sales_loaded = pd.read_sql(f'SELECT PROD_ID,CUST_ID,TIME_ID,\
        CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM dim_sales', ses_db_sor)
        sales_dict = {
            "PROD_ID":[],
            "CUST_ID":[],
            "TIME_ID":[],
            "CHANNEL_ID":[],
            "PROMO_ID":[],
            "QUANTITY_SOLD":[],
            "AMOUNT_SOLD":[]
        }

        if not sales_tra.empty:
            for id,cus,tim,cha,pro,qua_sol,amo_sol \
                in zip(sales_tra["PROD_ID"],sales_tra["CUST_ID"],
                sales_tra["TIME_ID"],sales_tra["CHANNEL_ID"],
                sales_tra["PROMO_ID"],sales_tra["QUANTITY_SOLD"],
                sales_tra["AMOUNT_SOLD"]):
                sales_dict["PROD_ID"].append(id),
                sales_dict["CUST_ID"].append(cus),
                sales_dict["TIME_ID"].append(tim),
                sales_dict["CHANNEL_ID"].append(cha),
                sales_dict["PROMO_ID"].append(pro),
                sales_dict["QUANTITY_SOLD"].append(qua_sol),
                sales_dict["AMOUNT_SOLD"].append(amo_sol)

        if sales_dict["PROD_ID"]:
            df_sales_load = pd.DataFrame(sales_dict)
            df_sales_merge = df_sales_load.merge(sales_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_sales_merge.to_sql('dim_sales',ses_db_sor,if_exists='append',index=False)
        print(df_sales_merge)
    except:
        traceback.print_exc()
    finally:
        pass