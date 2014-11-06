import sys
import datetime

def get_current_prices( bb_objects ):
    current_prices = {}
    for product in bb_objects.keys():
        current_prices[product] = bb_objects[product].dailybook[-1][1]
    return current_prices

#Getthe current worth of the portfolio based on the most recent daily closing prices
def get_worth(current_price,conversion_factor,current_portfolio):
    net_worth = current_portfolio['cash']
    num_shares = current_portfolio['num_shares']
    for product in current_price.keys():
        net_worth = net_worth + current_price[product]*conversion_factor[product]*num_shares[product]
    return net_worth

#Given the weights to assign to each product,calculate how many target number shares of the products we want (weight -ve implies short selling)
def get_positions_from_weights(weight,current_worth,current_price,conversion_factor):
    positions_to_take = {}
    for product in current_price.keys():
        money_allocated = weight[product]*current_worth
        positions_to_take[product] = money_allocated/(current_price[product]*conversion_factor[product])
    return positions_to_take

