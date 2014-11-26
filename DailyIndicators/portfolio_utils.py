
# TODO move this to a config file
def get_products_from_portfolio_string ( portfolio_string ):
    return ( portfolio_string.split(',') )

def make_portfolio_string_from_products ( products ):
    return ( ','.join(products) )
