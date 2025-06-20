
    
def validate_product (product_master , product_id ):
    if ( product_id not in product_master.keys() ) :
        return "invalid product"
