import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def loadProducts(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        products_dict = {
            "PROD_ID":[],
            "PROD_NAME":[],
            "PROD_DESC":[],
            "PROD_CATEGORY":[],
            "PROD_CATEGORY_ID":[],
            "PROD_CATEGORY_DESC":[],
            "PROD_WEIGHT_CLASS":[],
            "SUPPLIER_ID":[],
            "PROD_STATUS":[],
            "PROD_LIST_PRICE":[],
            "PROD_MIN_PRICE":[]
        }

        products_tra = pd.read_sql(f'SELECT PROD_ID,\
        PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,\
        PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE\
         from tra_products where LOAD_ID={load_id}',ses_db_stg)

        products_loaded = pd.read_sql(f'SELECT PROD_ID,\
        PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,\
        PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE\
         from dim_products',ses_db_sor)

        if not products_tra.empty:
            for id,nam,des,cat,cat_id,cat_des,wei_cla,sup,sta,lis_pri,min_pri \
                in zip(products_tra["PROD_ID"],products_tra["PROD_NAME"],
                products_tra["PROD_DESC"],products_tra["PROD_CATEGORY"],
                products_tra["PROD_CATEGORY_ID"],products_tra["PROD_CATEGORY_DESC"],
                products_tra["PROD_WEIGHT_CLASS"],products_tra["SUPPLIER_ID"],
                products_tra["PROD_STATUS"],products_tra["PROD_LIST_PRICE"]
                ,products_tra["PROD_MIN_PRICE"]):
                products_dict["PROD_ID"].append(id),
                products_dict["PROD_NAME"].append(nam),
                products_dict["PROD_DESC"].append(des),
                products_dict["PROD_CATEGORY"].append(cat),
                products_dict["PROD_CATEGORY_ID"].append(cat_id),
                products_dict["PROD_CATEGORY_DESC"].append(cat_des),
                products_dict["PROD_WEIGHT_CLASS"].append(wei_cla),
                products_dict["SUPPLIER_ID"].append(sup),
                products_dict["PROD_STATUS"].append(sta),
                products_dict["PROD_LIST_PRICE"].append(lis_pri),
                products_dict["PROD_MIN_PRICE"].append(min_pri)

        if products_dict["PROD_ID"]:
            df_products_load = pd.DataFrame(products_dict)
            df_products_merge = df_products_load.merge(products_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_products_merge.to_sql('dim_products',ses_db_sor,if_exists='append',index=False)
        print('Datos agregados: ',df_products_merge)
    except:
        traceback.print_exc()
    finally:
        pass