import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def extProducts():
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        products_csv = pd.read_csv("data/products.csv")
        
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
            "prod_min_price":[]
        }

        if not products_csv.empty:
            for id,nam,des,cat,cat_id,cat_des,wei_cla,sup,sta,lis_pri,min_pri \
                in zip(products_csv["PROD_ID"],products_csv["PROD_NAME"],
                products_csv["PROD_DESC"],products_csv["PROD_CATEGORY"],
                products_csv["PROD_CATEGORY_ID"],products_csv["PROD_CATEGORY_DESC"],
                products_csv["PROD_WEIGHT_CLASS"],products_csv["SUPPLIER_ID"],
                products_csv["PROD_STATUS"],products_csv["PROD_LIST_PRICE"]
                ,products_csv["PROD_MIN_PRICE"]):
                products_dict["prod_id"].append(id),
                products_dict["prod_name"].append(nam),
                products_dict["prod_desc"].append(des),
                products_dict["prod_category"].append(cat),
                products_dict["prod_category_id"].append(cat_id),
                products_dict["prod_category_desc"].append(cat_des),
                products_dict["prod_weight_class"].append(wei_cla),
                products_dict["supplier_id"].append(sup),
                products_dict["prod_status"].append(sta),
                products_dict["prod_list_price"].append(lis_pri)
                products_dict["prod_min_price"].append(min_pri)

        if products_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE PRODUCTS")
            df_products_ext = pd.DataFrame(products_dict)
            df_products_ext.to_sql('products',ses_db_stg,if_exists='append',index=False)
        print(products_csv)
    except:
        traceback.print_exc()
    finally:
        pass