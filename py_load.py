from load.loadTables import *
from services.etls import getActualLoad

import traceback

try:
    load_id = getActualLoad()
    loadChannel(load_id)
    loadTimes(load_id)
    loadPromotions(load_id)
    loadProducts(load_id)
    load_countries(load_id)
except:
    traceback.print_exc()
finally:
    pass