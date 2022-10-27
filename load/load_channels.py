import traceback
from util.db_connection import Db_Connection
import pandas as pd 
from transform.transforms import *

def loadChannel(load_id):
    try:
        con_db_stg = Db_Connection()
        con_db_sor = Db_Connection(database='ejmcdbsor')
        ses_db_sor = con_db_sor.start()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        channels_dict = {
            "CHANNEL_ID":[],
            "CHANNEL_DESC":[],
            "CHANNEL_CLASS":[],
            "CHANNEL_CLASS_ID":[]
        }
        
        chan_tra = pd.read_sql(f'SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID from tra_channel \
            where LOAD_ID={load_id}',ses_db_stg)
        chan_loaded = pd.read_sql(f'SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID from dim_channel',ses_db_sor)

        if not chan_tra.empty:
            for id,des,cla,cla_id \
                in zip(chan_tra["CHANNEL_ID"],chan_tra["CHANNEL_DESC"],
                chan_tra["CHANNEL_CLASS"],chan_tra["CHANNEL_CLASS_ID"]):
                channels_dict["CHANNEL_ID"].append(id),
                channels_dict["CHANNEL_DESC"].append(des),
                channels_dict["CHANNEL_CLASS"].append(cla),
                channels_dict["CHANNEL_CLASS_ID"].append(cla_id),
                
        if channels_dict["CHANNEL_ID"]:
            df_channels_load = pd.DataFrame(channels_dict)
            df_channels_merge = df_channels_load.merge(chan_loaded, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_channels_merge.to_sql('dim_channel',ses_db_sor,if_exists='append',index=False)
        print('Datos agregados: ',df_channels_merge)
    except:
        traceback.print_exc()
    finally:
        pass