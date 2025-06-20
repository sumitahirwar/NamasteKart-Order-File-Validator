from datetime import datetime,date

def validate_order_date( line ,today):
    order_date_str = line.strip().split(",")[1]
    if len(order_date_str.split('-')[0]) == 4:
        format = "%Y-%m-%d"
    else:  
        format = "%d-%m-%Y"
    
    order_date = datetime.strptime(order_date_str, format).date()

    if order_date > today :
        return "future order not valid"
