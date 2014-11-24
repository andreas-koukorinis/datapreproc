import sys
import pickle
import numpy as np
from datetime import datetime
from RiskManagement.RiskManager import RiskManager
from Utils.Calculate import get_current_prices, get_worth, get_current_notional_amounts
from Utils.DbQueries import conv_factor
from Utils.Regular import is_future, is_future_entity, get_base_symbol, get_first_futures_contract, get_next_futures_contract, get_future_mappings, shift_future_symbols

class ExecLogic():
    def __init__( self, trade_products, all_products, order_manager, portfolio, bb_objects, performance_tracker, _startdate, _enddate, _config ):
        self.trade_products = trade_products
        self.all_products = all_products
        self.future_mappings = get_future_mappings( all_products )
        self.order_manager = order_manager
        self.portfolio = portfolio
        self.bb_objects = bb_objects
        self.conversion_factor = conv_factor( self.all_products )
        self.capital_reduction = 1.0
        self.risk_manager = RiskManager( performance_tracker, _config )
        self.trading_status = True
        self.leverage = []
        self.end_date = _enddate
        self.current_date = datetime.strptime(_startdate, "%Y-%m-%d").date()
        self.leverage_file = open('logs/'+self.order_manager.log_filename+'/leverage.txt','w')
        self.weights_file = open('logs/'+self.order_manager.log_filename+'/weights.txt','w')
        self.leverage_file.write('date,leverage\n')
        self.weights_file.write( 'date,%s\n' % ( ','.join( self.trade_products ) ) )
        self.orders_to_place = {} # The net order amount(in number of shares) which are to be placed on the next trading day
        for product in all_products:
            self.orders_to_place[product] = 0 # Initially there is no pending order for any product

    # Place pending (self.orders_to_place) and rollover orders
    def rollover( self, dt ):
        self.current_date = dt.date()
        self.print_weights_info(dt)
        if not self.trading_status: return
        self.update_risk_status(dt)
        if self.trading_status:
            current_prices = get_current_prices( self.bb_objects )
            _orders_to_place = dict( [ ( product, 0 ) for product in self.all_products ] )

            #Adjust positions for settlements and pending orders
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product
                    _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )
                    if is_future( product ) and _is_last_trading_day:
                        p1 = product  # Example: 'fES_1'
                        p2 = get_next_futures_contract(p1)  # Example: 'fES_2'
                        positions_to_take_p1 = self.portfolio.num_shares[p1] + self.order_manager.to_be_filled[p1] + self.orders_to_place[p1]
                        if p2 not in self.all_products and positions_to_take_p1 > 0:
                            sys.exit( 'exec_logic -> adjust_positions_for_settlements : Product %s not present' %p2 )
                        elif positions_to_take_p1 > 0:
                            positions_to_take_p2 = (positions_to_take_p1*current_prices[p1]*self.conversion_factor[p1])/(current_prices[p2]*self.conversion_factor[p2])
                            _orders_to_place[p2] += positions_to_take_p2 # TODO check if = will do # TODO check why should this be different
                            _orders_to_place[p1] += - ( self.order_manager.to_be_filled[p1] + self.portfolio.num_shares[p1] )
                    else:
                        _orders_to_place[product] += self.orders_to_place[product] # TODO check if = will do
                else: # Dont do anything if today is not a trading day for the product
                    pass
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product,then place order      
                    _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )
                    if is_future( product ) and _is_last_trading_day:
                        self.place_order_agg( dt, product, _orders_to_place[product] ) # If today is the settlement day,then fill order immediately
                    else:
                        self.place_order( dt, product, _orders_to_place[product] )
                    self.orders_to_place[product] = 0 # Since today is a trading day for this product,so we should have no pending orders left
            #print 'rollover',dt,self.orders_to_place

        else: # Liquidate the portfolio
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product,then place order 
                    self.place_order_target( dt, product, 0 )
                    self.orders_to_place[product] = 0 # no pending orders left for this product
                else: # Remember this order,should be placed on the next trading day for the product
                    self.orders_to_place[product] = - ( self.order_manager.to_be_filled[product] + self.portfolio.num_shares[product] ) # TODO should cancel to_be_filled_orders instead of placing orders on the opposite side
        self.notify_last_trading_day()    

    def update_positions( self, dt, weights ):
        self.current_date = dt.date()
        self.print_weights_info(dt)
        if not self.trading_status: return
        self.update_risk_status(dt)
        if self.trading_status:
            current_portfolio = self.portfolio.get_portfolio()
            current_prices = get_current_prices( self.bb_objects )
            current_worth = get_worth( current_prices, self.conversion_factor, current_portfolio )
            positions_to_take = self.get_positions_from_weights( weights, current_worth * self.capital_reduction,current_prices )

            _orders_to_place = dict( [ ( product, 0 ) for product in self.all_products ] )  
            #Adjust positions for settlements
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product
                    _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )
                    if is_future( product ) and _is_last_trading_day:
                        p1 = product  # Example: 'fES_1'
                        p2 = get_next_futures_contract(p1)  # Example: 'fES_2'
                        if p2 not in self.all_products and positions_to_take[p1] > 0:
                            sys.exit( 'exec_logic -> adjust_positions_for_settlements : Product %s not present' %p2 )
                        elif positions_to_take[p1] > 0:
                            positions_to_take_p2 = ( positions_to_take[p1] * current_prices[p1] * self.conversion_factor[p1] ) / ( current_prices[p2] * self.conversion_factor[p2] )
                            _orders_to_place[p2] +=  positions_to_take_p2  # TODO check if = will do
                            _orders_to_place[p1] += - ( self.order_manager.to_be_filled[p1] + self.portfolio.num_shares[p1] )
                    else:
                        _orders_to_place[product] = positions_to_take[product] - ( self.order_manager.to_be_filled[product] + self.portfolio.num_shares[product] ) 
                    self.orders_to_place[product] = 0 # Since today is a trading day for this product,so we should have no pending orders left
                else:
                    self.orders_to_place[product] = positions_to_take[product] - ( self.order_manager.to_be_filled[product] + self.portfolio.num_shares[product] )
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product,then place order
                    _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )      
                    if is_future( product ) and _is_last_trading_day:   
                        self.place_order_agg( dt, product, _orders_to_place[product] ) # If today is the settlement day,then fill order immediately
                    else:
                        self.place_order( dt, product, _orders_to_place[product] )
            #print 'update_positions',dt,self.orders_to_place
        else: # Liquidate the portfolio
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product,then place order 
                    self.place_order_target( dt, product, 0 ) # Make target position as 0
                    self.orders_to_place[product] = 0 # no pending orders left for this product
                else: # Remember this order,should be placed on the next trading day for the product
                    self.orders_to_place[product] = - ( self.order_manager.to_be_filled[product] + self.portfolio.num_shares[product] ) # TODO should cancel to_be_filled_orders instead of placing orders on the opposite side
        self.notify_last_trading_day()

    def update_risk_status( self, dt ):
        status = self.risk_manager.check_status( dt )
        if status['stop_trading']:
            self.trading_status = False
        elif status['reduce_capital'][0]:
            self.capital_reduction = self.capital_reduction*(1.0 - status['reduce_capital'][1])

    def get_positions_from_weights( self, weights, current_worth, current_prices ):
        positions_to_take = dict( [ ( product, 0 ) for product in self.all_products ] )
        for product in weights.keys():
            if is_future_entity( product ): #If it is a futures entity like fES
                first_contract = get_first_futures_contract( product )
                positions_to_take[first_contract] = positions_to_take[first_contract] + ( weights[product] * current_worth )/( current_prices[first_contract] * self.conversion_factor[first_contract] ) # This execlogic invests in the first futures contract for a future entity
            else:
                positions_to_take[product] = positions_to_take[product] + ( weights[product] * current_worth )/( current_prices[product] * self.conversion_factor[product] )
        return positions_to_take

    def notify_last_trading_day( self ):
        _last_trading_day_base_symbols = []
        for product in self.all_products:
            _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )
            if is_future( product ) and _is_last_trading_day:
                _last_trading_day_base_symbols.append(get_base_symbol(product))
        _last_trading_day_base_symbols = list( set ( _last_trading_day_base_symbols ) ) # Give unique base symbols
        for _base_symbol in _last_trading_day_base_symbols:
            self.on_last_trading_day(_base_symbol)

    # Shift symbols on last trading day of a future product
    def on_last_trading_day( self, _base_symbol ):
        if get_first_futures_contract(_base_symbol) in self.all_products and self.portfolio.num_shares[get_first_futures_contract(_base_symbol)] != 0:
            sys.exit( 'ERROR : exec_logic -> after_settlement_day -> orders not placed properly -> first futures contract of %s has non zero shares after settlement day' % _base_symbol )
        else:
            shift_future_symbols( self.portfolio, self.future_mappings[_base_symbol] )

    def is_trading_day( self, dt, product ):
        return self.bb_objects[product].dailybook[-1][0].date() == dt.date() # If the closing price for a product is available for a date,then the product is tradable on that date

    def print_weights_info(self, dt):
        sum_wts = 0.0
        s = str(dt.date())
        (notional_amounts, net_value) = get_current_notional_amounts(self.bb_objects, self.portfolio, self.conversion_factor, dt.date())
        for product in self.portfolio.num_shares.keys():
            _weight = notional_amounts[product]/net_value
            sum_wts += abs(_weight)
            s = s + ',%f'% (_weight)
        self.weights_file.write(s + '\n')
        self.leverage_file.write('%s,%f\n' % (str(dt.date()), sum_wts))

    # Place an order to buy/sell 'num_shares' shares of 'product'
    # If num_shares is +ve -> it is a buy trade
    # If num_shares is -ve -> it is a sell trade
    def place_order( self, dt, product, num_shares ):
        if num_shares != 0:
            self.order_manager.send_order( dt, product, num_shares )
   
    # These order are sent directly yo the backtester by the order manager
    def place_order_agg( self, dt, product, num_shares ):
        if num_shares != 0:
            self.order_manager.send_order_agg( dt, product, num_shares )

    # Place an order to make the total number of shares of 'product' = 'target'
    # It can be a buy or a sell order depending on the current number of shares in the portfolio and the value of the target
    def place_order_target( self, dt, product, target ):
        current_num = self.portfolio.get_portfolio()['num_shares'][product] + self.order_manager.to_be_filled[product]
        to_place = target-current_num
        self.place_order( dt, product, to_place )
