from numpy import *
from BackTester.BackTester_Listeners import BackTesterListener
from BackTester.BackTester import BackTester
from BookBuilder.Bookbuilder_Listeners import SettlementListener
from BookBuilder.Bookbuilder import Bookbuilder
from Utils.DbQueries import conv_factor

class Portfolio( BackTesterListener, SettlementListener ):

    def __init__( self, products, _start_date, _end_date, _config ):
        self.cash = _config.getfloat( 'Parameters', 'initial_capital' )
        self.products = products
        self.num_shares = {}
        self.value = {}
        for product in products:
            self.num_shares[product] = 0
            self.value[product]=0
            backtester = BackTester.get_unique_instance( product, _start_date, _end_date, _config )
            backtester.add_listener( self )
            if(product[0]=='f'): # If it is a futures contract
                bookbuilder = BookBuilder.get_unique_instance( product, _start_date, _end_date, _config )
                bookbuilder.add_settlement_listener( self )       

    # Return the portfolio variables as a dictionary
    def get_portfolio( self ):
        return { 'cash' : self.cash, 'num_shares' : self.num_shares, 'products' : self.products }

    # cost_of_order : the commission and execution cost combined
    # value_of order : price*number_of_shares_bought/sold (-ve for sold,+ve for bought)
    # num_shares : +ve for buy,-ve for sell
    def on_order_update( self, filled_orders, date ):
        for order in filled_orders:
            # TODO {gchak} change the framework for futures. We have currently assumed futures to be fully cash settled I believe.
            # i.e. We have to pay the full notional value in cash
            self.cash = self.cash - order['value'] - order['cost']
            self.num_shares[order['product']] = self.num_shares[order['product']] + order['amount']

    def after_settlement_day( self, product ):
        p1 = product.rstrip('12')+'1'
        p2 = product.rstrip('12')+'2'
        if(self.num_shares[p1]==0 and self.num_shares[p2]!=0):
            self.num_shares[p1] = self.num_shares[p2]
            self.num_shares[p2] = 0
