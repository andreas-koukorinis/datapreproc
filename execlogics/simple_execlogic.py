import sys
import numpy as np
from datetime import datetime
from execlogics.execlogic_algorithm import ExecLogicAlgo
from Utils.Calculate import get_current_prices, get_mark_to_market, get_current_notional_amounts
from Utils.Regular import is_future, is_future_entity, get_base_symbol, get_first_futures_contract, get_next_futures_contract, get_future_mappings, shift_future_symbols

'''This execlogic switches to the next futures contract on the last trading day(based on volume) and places aggressive orders for rollover'''
class SimpleExecLogic(ExecLogicAlgo):
    def init(self, _config):
        pass

    # Place pending (self.orders_to_place) and rollover orders
    def rollover(self, dt):
        self.current_date = dt.date()
        if self.debug_level > 1:
            self.print_weights_info(dt)
        #if self.risk_level == 0: 
        #    return
        #self.risk_level = self.update_risk_level(self.current_date, {})
        #if self.risk_level > 0:
        if True:
            current_prices = get_current_prices(self.bb_objects)
            _orders_to_place = dict( [ ( product, 0 ) for product in self.all_products ] )

            #Adjust positions for settlements and pending orders
            for product in self.all_products:
                if self.is_trading_day(dt, product): # If today is a trading day for the product
                    _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )
                    if is_future(product) and _is_last_trading_day:
                        p1 = product  # Example: 'fES_1'
                        p2 = get_next_futures_contract(p1)  # Example: 'fES_2'
                        positions_to_take_p1 = self.portfolio.num_shares[p1] + self.order_manager.to_be_filled[p1] + self.orders_to_place[p1]
                        if p2 not in self.all_products and positions_to_take_p1 != 0:
                            sys.exit( 'exec_logic -> adjust_positions_for_settlements : Product %s not present' %p2 )
                        else:
                            if positions_to_take_p1 != 0:
                                positions_to_take_p2 = (positions_to_take_p1*current_prices[p1])/current_prices[p2]
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

        else: # Liquidate the portfolio
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product,then place order 
                    self.place_order_target( dt, product, 0 )
                    self.orders_to_place[product] = 0 # no pending orders left for this product
                else: # Remember this order,should be placed on the next trading day for the product
                    self.orders_to_place[product] = - ( self.order_manager.to_be_filled[product] + self.portfolio.num_shares[product] ) # TODO should cancel to_be_filled_orders instead of placing orders on the opposite side
        self.notify_last_trading_day()    

    def update_positions(self, dt, weights):
        self.current_date = dt.date()
        if self.debug_level > 1:
            self.print_weights_info(dt)
        if self.risk_level == 0:
            return
        #self.update_risk_level(self.current_date, weights)
        if self.risk_level > 0:
            #print 'inside', self.risk_level
            current_portfolio = self.portfolio.get_portfolio()
            current_prices = get_current_prices(self.bb_objects)
            self.performance_tracker.update_open_equity(self.current_date) # TODO{sanchit} Need to change this update
            current_worth = get_mark_to_market(self.current_date, current_prices, self.conversion_factor, self.currency_factor, self.product_to_currency, current_portfolio)
            positions_to_take = self.get_positions_from_weights(self.current_date, weights, current_worth * self.risk_level, current_prices)

            _orders_to_place = dict([(product, 0) for product in self.all_products ])  
            #Adjust positions for settlements
            for product in self.all_products:
                if self.is_trading_day(dt, product): # If today is a trading day for the product
                    _is_last_trading_day = self.bb_objects[product].dailybook[-1][2] and ( self.current_date == self.bb_objects[product].dailybook[-1][0].date() )
                    if is_future(product) and _is_last_trading_day:
                        p1 = product  # Example: 'fES_1'
                        p2 = get_next_futures_contract(p1)  # Example: 'fES_2'
                        if p2 not in self.all_products and positions_to_take[p1] != 0:
                            sys.exit( 'exec_logic -> adjust_positions_for_settlements : Product %s not present' %p2 )
                        else:
                            if positions_to_take[p1] != 0:
                                positions_to_take_p2 = (positions_to_take[p1] * current_prices[p1])/current_prices[p2]
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
        else: # Liquidate the portfolio
            for product in self.all_products:
                if self.is_trading_day( dt, product ): # If today is a trading day for the product,then place order 
                    self.place_order_target( dt, product, 0 ) # Make target position as 0
                    self.orders_to_place[product] = 0 # no pending orders left for this product
                else: # Remember this order,should be placed on the next trading day for the product
                    self.orders_to_place[product] = - ( self.order_manager.to_be_filled[product] + self.portfolio.num_shares[product] ) # TODO should cancel to_be_filled_orders instead of placing orders on the opposite side
        self.notify_last_trading_day()

    def get_positions_from_weights(self, date, weights, current_worth, current_prices): 
        positions_to_take = dict([(product, 0) for product in self.all_products])
        for product in weights.keys():
            if is_future_entity(product): #If it is a futures entity like fES
                # This execlogic invests in the first futures contract for a future entity
                first_contract = get_first_futures_contract(product)
                _conv_factor = self.conversion_factor[first_contract] * self.currency_factor[self.product_to_currency[first_contract]][date]                
                positions_to_take[first_contract] = positions_to_take[first_contract] + (weights[product] * current_worth)/(current_prices[first_contract] * _conv_factor)
            else:
                _conv_factor = self.conversion_factor[product] * self.currency_factor[self.product_to_currency[product]][date]
                positions_to_take[product] = positions_to_take[product] + (weights[product] * current_worth)/(current_prices[product] * _conv_factor)
        return positions_to_take
