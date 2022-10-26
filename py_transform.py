from util.db_connection import Db_Connection
from transform.transform_channels import extChannel
from extract.countries import extCountries
from extract.customers import extCustomers
from extract.times import extTimes
from transform.transform_products import extProducts
from transform.transform_sales import extSales
from extract.promotions import extPromotions
from datetime import datetime
import pandas as pd

import traceback

try:
    #extChannel()
    # extCountries()
    # extCustomers()
    # extTimes()
    # extProducts()
     extSales()
    # extPromotions()
except:
    traceback.print_exc()
finally:
    pass