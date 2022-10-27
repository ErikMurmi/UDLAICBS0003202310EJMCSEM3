import traceback
from util.db_connection import Db_Connection
import pandas as pd 
from transform.transforms import obt_date,str_float_format

def extProducts(load_id):
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        products_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[],
            "LOAD_ID":[]
        }

        products_tra = pd.read_sql('SELECT PROD_ID,\
        PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,\
        PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE\
         from products',ses_db_stg)

        if not products_tra.empty:
            for id,nam,des,cat,cat_id,cat_des,wei_cla,sup,sta,lis_pri,min_pri \
                in zip(products_tra["PROD_ID"],products_tra["PROD_NAME"],
                products_tra["PROD_DESC"],products_tra["PROD_CATEGORY"],
                products_tra["PROD_CATEGORY_ID"],products_tra["PROD_CATEGORY_DESC"],
                products_tra["PROD_WEIGHT_CLASS"],products_tra["SUPPLIER_ID"],
                products_tra["PROD_STATUS"],products_tra["PROD_LIST_PRICE"]
                ,products_tra["PROD_MIN_PRICE"]):
                products_dict["prod_id"].append(int(id)),
                products_dict["prod_name"].append(nam),
                products_dict["prod_desc"].append(des),
                products_dict["prod_category"].append(cat),
                products_dict["prod_category_id"].append(int(cat_id)),
                products_dict["prod_category_desc"].append(cat_des),
                products_dict["prod_weight_class"].append(int(wei_cla)),
                products_dict["supplier_id"].append(int(sup)),
                products_dict["prod_status"].append(sta),
                products_dict["prod_list_price"].append(str_float_format(lis_pri)),
                products_dict["prod_min_price"].append(str_float_format(min_pri)),
                products_dict["LOAD_ID"].append(load_id)

        if products_dict["prod_id"]:
            df_channels_tra = pd.DataFrame(products_dict)
            df_channels_tra.to_sql('tra_products',ses_db_stg,if_exists='append',index=False)
        print(df_channels_tra)
    except:
        traceback.print_exc()
    finally:
        pass