import sys
from RiskManagement.RiskManager import RiskManager
from BookBuilder.BookBuilder_Listeners import SettlementListener
from BookBuilder.BookBuilder import BookBuilder
from Utils.Calculate import get_current_prices,get_worth
from Utils.DbQueries import conv_factor
from Utils.Regular import is_future,is_future_entity,get_base_symbol,get_first_futures_contract,get_next_futures_contract,get_future_mappings,shift_future_symbols

class ExecLogic( SettlementListener ):
    def __init__( self, trade_products, all_products, order_manager, portfolio, bb_objects, performance_tracker, _startdate, _enddate, _config ):
        self.trade_products = trade_products
        self.all_products = all_products
        self.future_mappings = get_future_mappings( all_products )
        self.order_manager = order_manager
        self.portfolio = portfolio
        self.bb_objects = bb_objects
        self.conversion_factor = conv_factor( self.all_products )
        self.to_flip_settlement = dict( [ ( product, False ) for product in self.all_products ] )
        self.capital_reduction = 1.0
        self.risk_manager = RiskManager( performance_tracker, _config )
        self.trading_status = True
        for product in all_products:
            if is_future( product ):
                BookBuilder.get_unique_instance( product, _startdate, _enddate, _config ).add_settlement_listener( self )

    def rollover( self, dt ):
        if not self.trading_status: return
        self.update_risk_status( dt )
        if self.trading_status:
            positions_to_take = dict( [ ( product, self.portfolio.num_shares[product]* self.capital_reduction ) for product in self.all_products ] )
            current_prices = get_current_prices( self.bb_objects )
            new_positions_to_take = self.adjust_positions_for_settlements ( current_prices, positions_to_take )
            for product in self.all_products:
                self.place_order_target( dt, product, new_positions_to_take[product] )
        else: # Liquidate the portfolio
            for product in self.all_products:
                self.place_order_target( dt, product, 0 )
    
    def update_positions( self, dt, weights ):
        if not self.trading_status: return
        self.update_risk_status( dt )
        if self.trading_status:
            current_portfolio = self.portfolio.get_portfolio()
            current_prices = get_current_prices( self.bb_objects )
            current_worth = get_worth( current_prices, self.conversion_factor, current_portfolio )
            positions_to_take = self.get_positions_from_weights( weights, current_worth * self.capital_reduction,current_prices )
            for product in self.all_products:
                self.place_order_target( dt, product, positions_to_take[product] )
        else: # Liquidate the portfolio
            for product in self.all_products:
                self.place_order_target( dt, product, 0 )

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
        return self.adjust_positions_for_settlements ( current_prices, positions_to_take ) 

    # Shift the positions from 'k'th futures contract to 'k+1'th futures contract on the settlement day
    def adjust_positions_for_settlements( self, current_price, positions_to_take ):
        new_positions_to_take = dict( [ ( product, 0 ) for product in self.all_products ] )
        for product in self.all_products:
            is_last_trading_day = self.bb_objects[product].dailybook[-1][2]
            if( is_future( product ) and is_last_trading_day ):
                p1 = product  # Example: 'ES1'
                p2 = get_next_futures_contract(p1)  # Example: 'ES2'
                if p2 not in self.all_products and positions_to_take[p1] > 0:
                    sys.exit( 'exec_logic -> adjust_positions_for_settlements : Product %s not present' %p2 )
                elif positions_to_take[p1] > 0: 
                    new_positions_to_take[p2] = (positions_to_take[p1]*current_price[p1]*self.conversion_factor[p1])/(current_price[p2]*self.conversion_factor[p2])
            else:
                new_positions_to_take[product] = positions_to_take[product]
        return new_positions_to_take

    def get_current_weights( self ):
        net_portfolio = { 'cash' : self.portfolio.cash, 'num_shares' : dict( [ ( product, self.portfolio.get_portfolio()['num_shares'][product] ) for product in self.all_products ] ) }
        weights = {}
        for product in self.all_products:
            to_be_filled = 0
            for order in self.order_manager.backtesters[product].pending_orders:
                to_be_filled = to_be_filled + order['amount']
            net_portfolio['num_shares'][product] = net_portfolio['num_shares'][product] + to_be_filled
        current_prices = get_current_prices( self.bb_objects )
        current_worth = get_worth( current_prices, self.conversion_factor, net_portfolio )
        for product in self.all_products:
            weights[product] = current_worth/( current_prices[product] * self.conversion_factor[product] )
        return weights
        
    def after_settlement_day( self, product ):
        self.to_flip_settlement[product] = True
        _base_symbol = get_base_symbol( product )
        all_done = True
        for product in self.future_mappings[_base_symbol]:
            all_done = self.to_flip_settlement[product] and all_done
        if all_done:
            if self.portfolio.num_shares[get_first_futures_contract(_base_symbol)] != 0:
                sys.exit( 'ERROR : exec_logic -> after_settlement_day -> orders not placed properly -> first futures contract of %s has non zero shares after settlement day' % _base_symbol )
            shift_future_symbols( self.portfolio, self.future_mappings[_base_symbol] )
            for product in self.future_mappings[_base_symbol]:
                self.to_flip_settlement[product] = False

    # Place an order to buy/sell 'num_shares' shares of 'product'
    # If num_shares is +ve -> it is a buy trade
    # If num_shares is -ve -> it is a sell trade
    def place_order( self, dt, product, num_shares ):
        self.order_manager.send_order( dt, product, num_shares )

    # Place an order to make the total number of shares of 'product' = 'target'
    # It can be a buy or a sell order depending on the current number of shares in the portfolio and the value of the target
    def place_order_target( self, dt, product, target ):
        to_be_filled = 0
        for order in self.order_manager.backtesters[product].pending_orders:
            to_be_filled = to_be_filled + order['amount']
        current_num = self.portfolio.get_portfolio()['num_shares'][product] + to_be_filled
        to_place = target-current_num
        if(to_place!=0):
            self.place_order( dt, product, to_place )
