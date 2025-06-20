    
def validate_sales (product_master , line ):

    product_id = line.split(",")[2]
    quantity = int(line.split(",")[3])
    sales = float(line.split(",")[4])
    
    product_price_master = float(product_master[product_id])
    exp_sales = quantity * product_price_master

    if exp_sales != sales :
        return "not valid sales"

