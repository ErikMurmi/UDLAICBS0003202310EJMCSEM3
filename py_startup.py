from util.db_connection import Db_Connection
from extract.channels import extChannel
from extract.countries import extCountries
from extract.customers import extCustomers
from extract.times import extTimes
from extract.products import extProducts
from extract.sales import extSales
from extract.promotions import extPromotions
from datetime import datetime
import pandas as pd

import traceback

try:
    extChannel()
    extCountries()
    extCustomers()
    extTimes()
    extProducts()
    extSales()
    extPromotions()
except:
    traceback.print_exc()
finally:
    pass