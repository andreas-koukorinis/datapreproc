from Utils.Calculate import get_current_prices,get_worth
from Utils.DbQueries import conv_factor
from Utils.Regular import is_future_entity

class ExecLogic():
    def __init__( self, trade_products, all_products, order_manager, portfolio, bb_objects ):
        self.trade_products = trade_products
        self.all_products = all_products
        self.order_manager = order_manager
        self.portfolio = portfolio
        self.bb_objects = bb_objects
        self.conversion_factor = conv_factor( self.all_products )
     
    def update_positions( self, dt, weights ):
        current_portfolio = self.portfolio.get_portfolio()
        current_prices = get_current_prices( self.bb_objects )
        current_worth = get_worth( current_prices, self.conversion_factor, current_portfolio )
        positions_to_take = get_positions_from_weights( weights, current_worth ,current_prices )
        for product in self.all_products:
            self.place_order_target( dt, product, positions_to_take[product] )

    def get_positions_from_weights( self, weights, current_worth, current_prices ):
        positions_to_take = dict( [ ( product, [] ) for product in self.all_products ] )
        for product in weights.keys():
            if is_future_entity( product ): #If it is a futures entity like fES
                    
            else:
                position = ( weights[symbol] * current_worth )/( current_prices[product] * self.conversion_factor[product] )
        self.adjust_positions_for_settlements ( self, current_prices, positions_to_take ) 

    # Shift the positions from 'k'th futures contract to 'k+1'th futures contract on the settlement day
    def adjust_positions_for_settlements(self,current_price,positions_to_take):
        settlement_products=[]
        for product in self.all_products:
            is_last_trading_day = self.bb_objects[product].dailybook[-1][2]
            if( product[0] == 'f' and is_last_trading_day and product[-1]=='1' ):
                p1 = product  # Example: 'ES1'
                p2 = product.rstrip('1')+'2'  # Example: 'ES2'
                positions_to_take[p2] = (positions_to_take[p1]*current_price[p1]*self.conversion_factor[p1])/(current_price[p2]*self.conversion_factor[p2])
                positions_to_take[p1] = 0
        return positions_to_take

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
