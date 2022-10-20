import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def extSales():
    try:

        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        sales_csv = pd.read_csv("data/sales.csv")
        
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
        }

        if not sales_csv.empty:
            for id,cus,tim,cha,pro,qua_sol,amo_sol \
                in zip(sales_csv["PROD_ID"],sales_csv["CUST_ID"],
                sales_csv["TIME_ID"],sales_csv["CHANNEL_ID"],
                sales_csv["PROMO_ID"],sales_csv["QUANTITY_SOLD"],
                sales_csv["AMOUNT_SOLD"]):
                sales_dict["prod_id"].append(id),
                sales_dict["cust_id"].append(cus),
                sales_dict["time_id"].append(tim),
                sales_dict["channel_id"].append(cha),
                sales_dict["promo_id"].append(pro),
                sales_dict["quantity_sold"].append(qua_sol),
                sales_dict["amount_sold"].append(amo_sol),

        if sales_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE SALES")
            df_sales_ext = pd.DataFrame(sales_dict)
            df_sales_ext.to_sql('sales',ses_db_stg,if_exists='append',index=False)
        print(sales_csv)
    except:
        traceback.print_exc()
    finally:
        pass