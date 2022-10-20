import traceback
from util.db_connection import Db_Connection
import pandas as pd 

def extChannel():
    try:
        con_db_stg = Db_Connection()
        ses_db_stg = con_db_stg.start()

        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging")
        channels_csv = pd.read_csv("data/channels.csv")
        
        channels_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }

        if not channels_csv.empty:
            for id,des,cla,cla_id \
                in zip(channels_csv["CHANNEL_ID"],channels_csv["CHANNEL_DESC"],
                channels_csv["CHANNEL_CLASS"],channels_csv["CHANNEL_CLASS_ID"]):
                channels_dict["channel_id"].append(id),
                channels_dict["channel_desc"].append(des),
                channels_dict["channel_class"].append(cla),
                channels_dict["channel_class_id"].append(cla_id)

        if channels_dict["channel_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE CHANNELS")
            df_channels_ext = pd.DataFrame(channels_dict)
            df_channels_ext.to_sql('channels',ses_db_stg,if_exists='append',index=False)
        print(channels_csv)
    except:
        traceback.print_exc()
    finally:
        pass