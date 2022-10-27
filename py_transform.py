from transform.tablesTransform import *
from services.etls import getNewLoadId

import traceback

try:
    load_id = getNewLoadId()
    transformChannel(load_id)
    transform_countries(load_id)
    transformCustomers(load_id)
    transformTimes(load_id)
    transformProducts(load_id)
    transformSales(load_id)
    transformPromotions(load_id)
except:
    traceback.print_exc()
finally:
    pass