import sys
import datetime
from backtester.backtester import BackTester
from backtester.backtester_listeners import BackTesterListener
from utils.regular import shift_future_symbols, get_first_futures_contract, is_margin_product

class Portfolio(BackTesterListener):
    """ Tracks the current portfolio of the strategy
    
    Description:
        The portfolio includes:
            cash (float): The current amount of cash in USD left with us
            products (list): The list of products in our portfolio
            num_shares (dict from products to positions): Tracks the current open position in each product of the portfolio
            open_equity (dict from products to floats): For each margin product, the amount of unrealized pnl in the product's local currency
        The portfolio instance is mainly used by execlogic, performance_tracker and trade_algorithm

    Listeners: None

    Listening to: Backtester for order updates

    Note:
        1) The cash can go negative since the fill_price may be different from order_price #TODO change this 
        2) Currently the portfolio is updated by the performance tracker on filled orders, so we are listening to backtester unnecessarily
    """    

    def __init__(self, products, _start_date, _end_date, _config):
        """ Initializes the portfolio components (cash, products, num_shares, open_equity)
        and registers as a listener to backtester for order updates

        Args:
            products: The list of products that the strategy will trade
            start_date: The start_date of the simulation
            end_date: The end_date of the simulation
            _config: The handle to the config file of the strategy
    
        Returns: Nothing 
        """

        self.cash = _config.getfloat('Parameters', 'initial_capital')
        self.products = products
        self.open_equity = dict([(_product, 0) for _product in self.products]) # Map from product to current open equity
        self.num_shares = {}

        for product in products:
            self.num_shares[product] = 0
            BackTester.get_unique_instance(product, _start_date, _end_date, _config).add_listener(self)

    def get_portfolio(self):
        """ Returns a new dictionary with all the portfolio components """ # TODO not really needed, because portfolio can be modified through the new dict too
        return { 'cash' : self.cash, 'open_equity' : self.open_equity, 'num_shares' : self.num_shares, 'products' : self.products }

    def on_order_update(self, filled_orders, date):
        """ Currently of no use, Not updating the portfolio here currently, instead updating in the performance tracker  #TODO update portfolio here
        
        Args:
            filled_orders (list of dicts): list of filled orders, each order is a dict
            date (date object): The date on which we have recieved the order update

        Returns: Nothing
        """
        pass
        '''for order in filled_orders:
            _product = order['product']
            if is_margin_product(_product): # If we are not required to post only the margin for the product
                self.cash -= order['cost']
            else:
                self.cash -= (order['value'] + order['cost'])
            self.num_shares[_product] = self.num_shares[_product] + order['amount']'''

    def on_last_trading_day(self, _base_symbol, future_mappings):
        """ Shifts the symbols for the furute contracts in the dict num_shares on the last trading day
            
        Args: 
            _base_symbol (string): The base symbol of the future contract for which it as a last trading day.Eg: fES
            future_mappings (list): The list of future contracts being traded corresponding to this base symbol.Eg: fES_1,fES_2

        Example: num_shares = {'fES_1': 0.0, 'fES_2': 10.0, 'fES_3': 20.0} -> num_shares = {'fES_1': 10.0, 'fES_2': 20.0, 'fES_3': 0.0}

        Note: Ideally we should shift symbols for open_equity also here, but it is currently being done in performace_tracker #TODO shift open_equity here

        Returns: Nothing
        """ 

        _first_futures_contract = get_first_futures_contract(_base_symbol)
        if _first_futures_contract in self.products and self.num_shares[_first_futures_contract] != 0:
            sys.exit( 'ERROR : Portfolio -> on_last_trading_day -> orders not placed properly -> first futures contract of %s has non zero shares after last trading day' % _base_symbol )
        else:
            shift_future_symbols(self.num_shares, future_mappings)
