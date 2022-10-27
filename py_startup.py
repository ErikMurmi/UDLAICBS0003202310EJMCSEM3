from util.db_connection import Db_Connection
from extract.channels import extChannel
from extract.countries import extCountries
from extract.customers import extCustomers
from extract.times import extTimes
from extract.products import extProducts
from extract.sales import extSales
from extract.promotions import extPromotions
from services.etls import getNewLoadId
from transform.tablesTransform import *
from load.loadTables import *
from services.etls import getActualLoad

import traceback

try:
    extChannel()
    extCountries()
    extCustomers()
    extTimes()
    extProducts()
    extSales()
    extPromotions()
    load_id = getNewLoadId()
    transformChannel(load_id)
    transform_countries(load_id)
    transformCustomers(load_id)
    transformTimes(load_id)
    transformProducts(load_id)
    transformSales(load_id)
    transformPromotions(load_id)
    load_id = getActualLoad()
    loadChannel(load_id)
    loadTimes(load_id)
    loadPromotions(load_id)
    load_customers(load_id)
    loadProducts(load_id)
    load_countries(load_id)
except:
    traceback.print_exc()
finally:
    pass