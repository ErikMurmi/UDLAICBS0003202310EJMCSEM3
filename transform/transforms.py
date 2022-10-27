from datetime import datetime

def join_2_str(string,string2):
    return string+string2

def get_month_str(month_num):
    datetime_object = datetime.strptime(month_num, "%m")
    return datetime_object.strftime("%B")

def obt_date(date_str):
    print(f'fecha str: {date_str}')
    return datetime.date(date_str,'%Y-%m-%d')

def date_str_month(date_str):
    return datetime.strptime(date_str,'%d-%b-%y')

def str_float_format(num):
    return float("{:.2f}".format(float(num)))

def date_for_fk(date_str):
    return int(datetime.strptime(date_str,'%d-%b-%y').strftime('%d%m%y'))