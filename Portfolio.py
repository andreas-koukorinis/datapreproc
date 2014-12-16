import sys
from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from Utils.Regular import shift_future_symbols, get_first_futures_contract, is_margin_product

class Portfolio(BackTesterListener):

    def __init__(self, products, _start_date, _end_date, _config):
        self.cash = _config.getfloat('Parameters', 'initial_capital')
        self.products = products
        self.open_equity = dict([(_product, 0) for _product in self.products]) # Map from product to current open equity
        self.num_shares = {}

        for product in products:
            self.num_shares[product] = 0
            BackTester.get_unique_instance(product, _start_date, _end_date, _config).add_listener(self)

    # Return the portfolio variables as a dictionary
    def get_portfolio(self):
        return { 'cash' : self.cash, 'open_equity' : self.open_equity, 'num_shares' : self.num_shares, 'products' : self.products }

    # cost_of_order : the commission and slippage cost combined
    # value_of order : price*number_of_shares_bought/sold (-ve for sold,+ve for bought)
    # num_shares : +ve for buy,-ve for sell
    def on_order_update(self, filled_orders, date):
        for order in filled_orders:
            pass
            ''''_product = order['product']
            if is_margin_product(_product): # If we are not required to post only the margin for the product
                self.cash -= order['cost']
            else:
                self.cash -= (order['value'] + order['cost'])
            self.num_shares[_product] = self.num_shares[_product] + order['amount']'''

    def on_last_trading_day(self, _base_symbol, future_mappings):
        _first_futures_contract = get_first_futures_contract(_base_symbol)
        if _first_futures_contract in self.products and self.num_shares[_first_futures_contract] != 0:
            sys.exit( 'ERROR : Portfolio -> on_last_trading_day -> orders not placed properly -> first futures contract of %s has non zero shares after last trading day' % _base_symbol )
        else:
            shift_future_symbols(self.num_shares, future_mappings)
