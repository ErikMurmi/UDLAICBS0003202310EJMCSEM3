from util.db_connection import Db_Connection
from transform.transform_channels import extChannel
from extract.countries import extCountries
from extract.customers import extCustomers
from transform.transform_times import extTimes
from transform.transform_products import extProducts
from transform.transform_sales import extSales
from extract.promotions import extPromotions
from datetime import datetime
from services.etls import getNewLoadId
import pandas as pd

import traceback

try:
    load_id = getNewLoadId()
    extChannel(load_id)
    # extCountries(load_id)
    # extCustomers(load_id)
    extTimes(load_id)
    extProducts(load_id)
    extSales(load_id)
    # extPromotions(load_id)
except:
    traceback.print_exc()
finally:
    pass