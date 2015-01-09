import sys
from datetime import datetime
from execlogics.execlogic_algorithm import ExecLogicAlgo
from utils.calculate import get_current_prices, get_mark_to_market, get_current_notional_amounts, find_most_recent_price, find_most_recent_price_future
from utils.regular import is_future, is_future_entity, get_base_symbol, get_first_futures_contract, get_next_futures_contract, get_future_mappings, shift_future_symbols

'''This execlogic switches to the next futures contract on the last trading day(based on volume) and places aggressive orders for rollover'''
class SimpleExecLogic(ExecLogicAlgo):
    def init(self, _config):
        pass

    # Place pending (self.orders_to_place) and rollover orders
    def rollover(self, dt):
        self.current_date = dt.date()
        current_prices = get_current_prices(self.bb_objects)
        self.reinvest_pending_distributions(dt, self.distributions_to_reinvest, current_prices)
        _orders_to_place = dict( [ ( product, 0 ) for product in self.all_products ] )
        _old_risk_level = self.risk_level
        _new_risk_level = self.risk_manager.get_current_risk_level(self.current_date)
        if _new_risk_level != _old_risk_level:
            self.update_positions(dt, self.get_current_weights(self.current_date, current_prices))
        else:
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
                                _orders_to_place[p2] += positions_to_take_p2
                            _orders_to_place[p1] += - ( self.order_manager.to_be_filled[p1] + self.portfolio.num_shares[p1] )
                    else:
                        _orders_to_place[product] += self.orders_to_place[product]
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
            self.notify_last_trading_day()

    def update_positions(self, dt, weights):
        self.current_date = dt.date()
        _prior_risk_level = self.risk_level
        self.risk_level = self.risk_manager.get_current_risk_level(self.current_date)
        if not (self.risk_level == 0 and _prior_risk_level == 0):
            current_portfolio = self.portfolio.get_portfolio()
            current_prices = get_current_prices(self.bb_objects)
            current_worth = get_mark_to_market(self.current_date, current_prices, self.conversion_factor, self.currency_factor, self.product_to_currency, self.performance_tracker, current_portfolio)
            positions_to_take = self.get_positions_from_weights(self.current_date, weights, current_worth * self.risk_level/100.0, current_prices)
            #print 'execlogic', current_worth, self.current_date,current_prices,current_portfolio

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

    def get_current_weights(self, date, current_prices):
        #_net_portfolio_value = self.performance_tracker.value[-1]
        # TODO should not recompute
        _net_portfolio_value = get_mark_to_market(date, current_prices, self.conversion_factor, self.currency_factor,self. product_to_currency, self.performance_tracker, self.portfolio.get_portfolio())
        weights = {}
        for _product in self.portfolio.num_shares.keys():
            _desired_num_shares = self.portfolio.num_shares[_product] + self.order_manager.to_be_filled[_product]
            if _desired_num_shares != 0:
                if is_future(_product):
                    _price = find_most_recent_price_future(self.bb_objects[_product].dailybook, self.bb_objects[get_next_futures_contract(_product)].dailybook, date)
                else:
                    _price = find_most_recent_price(self.bb_objects[_product].dailybook, date)
                _notional_value_product = _price * _desired_num_shares * self.conversion_factor[_product] * self.currency_factor[self.product_to_currency[_product]][date]
            else:
                _notional_value_product = 0.0
            weights[_product] = _notional_value_product/_net_portfolio_value
        return weights

    def reinvest_pending_distributions(self, dt, distributions_to_reinvest, current_prices):
        for _product in distributions_to_reinvest.keys():
            if distributions_to_reinvest[_product] > 0:
                self.place_order(dt, _product, distributions_to_reinvest[_product]/current_prices[_product])
                self.distributions_to_reinvest[_product] = 0

    def on_distribution_day(self, event):
        _product = event['product']
        _dt = event['dt']
        _type = event['distribution_type']
        _distribution = event['quote']
        if _type == 'DIVIDEND':
            _after_tax_net_payout = _distribution*self.portfolio.num_shares[_product]*(1-self.performance_tracker.dividend_tax_rate)
        elif _type == 'CAPITALGAIN': # TODO split into short term and long term based on bbg data
            _after_tax_net_payout = _distribution*self.portfolio.num_shares[_product]*(1-(self.performance_tracker.long_term_tax_rate + self.performance_tracker.short_term_tax_rate)/2.0)
        self.distributions_to_reinvest[_product] += _after_tax_net_payout
