from util.db_connection import Db_Connection
import traceback

def getNewLoadId():
    try:
        connection=Db_Connection()
        db = connection.start()
        db.execute('INSERT INTO etl_hist values ()')
        load_id= db.execute('SELECT load_id  FROM etl_hist ORDER BY load_id DESC limit 1').scalar()
        return load_id
    except:
        traceback.print_exc()
    finally:
        pass