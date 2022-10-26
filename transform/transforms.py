from datetime import datetime

def join_2_str(string,string2):
    return string+string2

def obt_date(date_str):
    print(f'fecha str: {date_str}')
    return datetime.date(date_str,'%Y-%m-%d')

def date_str_month(date_str):
    fecha =  datetime.strptime(date_str,'%d-%b-%y')
    return (fecha)

def str_float_format(num):
    return float("{:.2f}".format(float(num)))