import traceback
from util.db_connection import Db_Connection
import pandas as pd 
from transform.transforms import date_str_month, obt_date, str_float_format

def extSales():
    try:

        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
        }
        
        sales_tra = pd.read_sql('SELECT PROD_ID,CUST_ID,TIME_ID,\
        CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales', ses_db_stg)

        if not sales_tra.empty:
            for id,cus,tim,cha,pro,qua_sol,amo_sol \
                in zip(sales_tra["PROD_ID"],sales_tra["CUST_ID"],
                sales_tra["TIME_ID"],sales_tra["CHANNEL_ID"],
                sales_tra["PROMO_ID"],sales_tra["QUANTITY_SOLD"],
                sales_tra["AMOUNT_SOLD"]):
                sales_dict["prod_id"].append(int(id)),
                sales_dict["cust_id"].append(int(cus)),
                sales_dict["time_id"].append(date_str_month(tim)),
                sales_dict["channel_id"].append(int(cha)),
                sales_dict["promo_id"].append(int(pro)),
                sales_dict["quantity_sold"].append(str_float_format(qua_sol)),
                sales_dict["amount_sold"].append(str_float_format(amo_sol)),

        if sales_dict["prod_id"]:
            df_sales_tra = pd.DataFrame(sales_dict)
            df_sales_tra.to_sql('tra_sales',ses_db_stg,if_exists='append',index=False)
        print(df_sales_tra)
    except:
        traceback.print_exc()
    finally:
        pass