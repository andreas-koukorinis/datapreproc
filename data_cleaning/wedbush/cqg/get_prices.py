import argparse
import calendar
import sys
import time
from webapi_1_pb2 import *
import webapi_client

host_name = 'wss://demoapi.cqg.com:443'
user_name = 'msinghalwapi'
password = 'pass'
csi_to_cqg = {'6A':'DA6','6B':'BP6','6C':'CA6','6J':'JY6','6M':'MX6','6N':'NE6','GC':'GCE','SI':'SIE','HG':'CPE','PL':'PLE','PA':'PAE','ZC':'ZCE','ZW':'KWE','ZS':'SE','ZM':'ZME','ZL':'ZLE','LH':'HE','ZN':'TYA','ZF':'FVA','ZB':'USA','ZT':'TUA','FGBL':'DB','FGBM':'DL','FGBS':'DG','LFR':'QGA','ES':'EP','EMD':'EMD','NIY':'NIY','NKD':'NKD','NQ':'ENQ','LFZ':'F100','FDAX':'DD','FESX':'DSX','FSMI':'SW','SXF':'TP','CGB':'CB'}

def logon(client, user_name, password, client_id='WebApiTest', client_version='python-client'):
    client_msg = ClientMsg()
    client_msg.logon.user_name = user_name
    client_msg.logon.password = password
    client_msg.logon.client_id = client_id
    client_msg.logon.client_version = client_version
    client.send_client_message(client_msg)

    server_msg = client.receive_server_message()
    if server_msg.logon_result.result_code == 0:
        return server_msg.logon_result.base_time
    else:
        raise Exception("Can't login: " + server_msg.logon_result.text_message)

def get_currency_rates(msg_id=1, subscribe=None):
    client = webapi_client.WebApiClient()
    client.connect(host_name)
    base_time = time.strptime(logon(client,user_name, password),'%Y-%m-%dT%H:%M:%S')

    client_msg = ClientMsg()
    information_request = client_msg.information_request.add()
    information_request.id = msg_id
    if subscribe is not None:
        information_request.subscribe = subscribe
    information_request.currency_rates_request.SetInParent()
    client.send_client_message(client_msg)
    server_msg = client.receive_server_message()


def get_prices(symbol_name, msg_id=1, subscribe=None):
    client = webapi_client.WebApiClient()
    client.connect(host_name)
    base_time = time.strptime(logon(client,user_name, password),'%Y-%m-%dT%H:%M:%S')

    client_msg = ClientMsg()
    information_request = client_msg.information_request.add()
    information_request.id = msg_id
    if subscribe is not None:
        information_request.subscribe = subscribe
    information_request.symbol_resolution_request.symbol = symbol_name
    client.send_client_message(client_msg)
    server_msg = client.receive_server_message()

    contract_id = server_msg.information_report[0].symbol_resolution_report.contract_metadata.contract_id
    correct_price_scale = server_msg.information_report[0].symbol_resolution_report.contract_metadata.correct_price_scale
    tick_value = server_msg.information_report[0].symbol_resolution_report.contract_metadata.tick_value
    tick_size = server_msg.information_report[0].symbol_resolution_report.contract_metadata.tick_size

    last_min_bar_close_price = 0
    
    while last_min_bar_close_price == 0:
        client_msg = ClientMsg()
        time_bar_request = client_msg.time_bar_request.add()
        time_bar_request.request_id = 1
        time_bar_request.time_bar_parameters.contract_id = contract_id
        time_bar_request.time_bar_parameters.bar_unit = TimeBarParameters.MIN
        time_bar_request.time_bar_parameters.from_utc_time = int((time.time()-calendar.timegm(base_time)-600)*1000)
        time_bar_request.time_bar_parameters.to_utc_time = int((time.time()-calendar.timegm(base_time)-60)*1000)
        
        client.send_client_message(client_msg)
        server_msg = client.receive_server_message()

        if server_msg.time_bar_report[0].status_code == 103:
            return ('NA','NA')
        elif server_msg.time_bar_report[0].status_code != 0:
            continue
        i = 0
        while i < len(server_msg.time_bar_report[0].time_bar) and last_min_bar_close_price == 0:
            last_min_bar_close_price = server_msg.time_bar_report[0].time_bar[i].close_price
            i += 1
        
    client.disconnect()

    return (last_min_bar_close_price * correct_price_scale, tick_value/tick_size)

    # Get rtealtime updates using the following snippet
    # client_msg = ClientMsg()
    # market_data_subscription = client_msg.market_data_subscription.add()
    # market_data_subscription.contract_id = contract_id
    # market_data_subscription.level = MarketDataSubscription.TRADES_BBA
    # client.send_client_message(client_msg)
    # server_msg = client.receive_server_message()
    # client_msg = ClientMsg()
    # time_and_sales_request = client_msg.time_and_sales_request.add()
    # time_and_sales_request.request_id = 1
    # time_and_sales_request.time_and_sales_parameters.contract_id = contract_id
    # time_and_sales_request.time_and_sales_parameters.level = TimeAndSalesParameters.TRADES_BBA_VOLUMES
    # time_and_sales_request.time_and_sales_parameters.from_utc_time = 
    # client.send_client_message(client_msg)
    # server_msg = client.receive_server_message()
    # client.disconnect()

# def place_order():
    # client_msg = ClientMsg()
    # trade_subscription = client_msg.trade_subscription.add()
    # trade_subscription.id = 1212
    # subscription_scope = trade_subscription.subscription_scope.append(TradeSubscription.ORDERS)
    # trade_subscription.subscribe = True
    # client.send_client_message(client_msg)
    # server_msg = client.receive_server_message()
    # client_msg = ClientMsg()
    # order_request = client_msg.order_request.add()
    # order_request.request_id = 1
    # order_request.new_order.order.account_id = 16862428
    # order_request.new_order.order.when_utc_time = 
    # order_request.new_order.order.contract_id = 1
    # order_request.new_order.order.duration = Order.DAY
    # order_request.new_order.order.side = Order.SELL
    # order_request.new_order.order.is_manual = True
    # order_request.new_order.order.qty = 1
    # order_request.new_order.order.order_type = Order.MKT
    # order_request.new_order.order.cl_order_id = "YO"
    # client.send_client_message(client_msg)
    # server_msg = client.receive_server_message()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', type=str, nargs='+', help='Products to be fetched from CQG\nEg: -p EPM15 DA6M15\n', default=None, dest='products')
    parser.add_argument('-o', help='Path of output file\nEg: -o prices.csv\n', default='prices.csv', dest='output_path')
    args = parser.parse_args()

    prices = dict.fromkeys(args.products)
    
    for product in args.products:
        if product[:-3] in csi_to_cqg.keys():
            prices[product] = get_prices(csi_to_cqg[product[:-3]] + product[-3:])
        else:
            prices[product] = get_prices(product)

    with open(args.output_path,'w') as f:
        f.write("Product,Current Price,Conversion Factor\n")
        for k,v in prices.items():
            f.write("%s,%s,%s\n"%(k,v[0],v[1]))
    #get_currency_rates()