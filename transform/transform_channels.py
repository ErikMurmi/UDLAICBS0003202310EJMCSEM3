import traceback
from util.db_connection import Db_Connection
import pandas as pd 
from transform.transforms import *

def transformChannel(load_id):
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        
        channels_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "LOAD_ID":[]
        }
        
        chan_tra = pd.read_sql('SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID from channels',ses_db_stg)
        if not chan_tra.empty:
            for id,des,cla,cla_id \
                in zip(chan_tra["CHANNEL_ID"],chan_tra["CHANNEL_DESC"],
                chan_tra["CHANNEL_CLASS"],chan_tra["CHANNEL_CLASS_ID"]):
                channels_dict["channel_id"].append(int(id)),
                channels_dict["channel_desc"].append(des),
                channels_dict["channel_class"].append(cla),
                channels_dict["channel_class_id"].append(int(cla_id)),
                channels_dict["LOAD_ID"].append(load_id)
        if channels_dict["channel_id"]:
            df_channels_tra = pd.DataFrame(channels_dict)
            df_channels_tra.to_sql('tra_channel',ses_db_stg,if_exists='append',index=False)
        print(df_channels_tra)
    except:
        traceback.print_exc()
    finally:
        pass