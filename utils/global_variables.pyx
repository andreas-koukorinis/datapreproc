# cython: profile=True
class Globals:
    conversion_factor = {}
    currency_factor = {}
    product_to_currency = {}
    product_type = {}
    trade_products = []
    all_products = []
    debug_level = None
    positions_file = None
    returns_file = None
    stats_file = None
    weights_file = None
    leverage_file = None
    amount_transacted_file = None

    #  Instance tracking
    dispatcher_instance = None
    ordermanager_instance = None
    backtester_instances = {}
    bookbuilder_instances = {}
    printindicators_instance = None
    average_discretized_trend_instances = {}
    average_stdev_instances = {}
    correlation_log_returns_instances = {}
    crossover_instances = {}
    daily_log_returns_instances = {}
    daily_prices_instances = {}
    expected_returns_instances = {}
    moving_average_instances = {}
    stddev_instances = {}
    stddev_crossover_instances = {}
    trend_instances = {}

    @classmethod
    def reset(cls):
        cls.econversion_factor = {}
        cls.currency_factor = {}
        cls.product_to_currency = {}
        cls.product_type = {}
        cls.trade_products = []
        cls.all_products = []
        cls.debug_level = None
        cls.positions_file = None
        cls.returns_file = None
        cls.stats_file = None
        cls.weights_file = None
        cls.leverage_file = None
        cls.amount_transacted_file = None

        #  Reset instance variables to remove referrents
        cls.dispatcher_instance = None
        cls.ordermanager_instance = None
        cls.backtester_instances = {}
        cls.bookbuilder_instances = {}
        cls.printindicators_instance = None
        cls.average_discretized_trend_instances = {}
        cls.average_stdev_instances = {}
        cls.correlation_log_returns_instances = {}
        cls.crossover_instances = {}
        cls.daily_log_returns_instances = {}
        cls.daily_prices_instances = {}
        cls.expected_returns_instances = {}
        cls.moving_average_instances = {}
        cls.stddev_instances = {}
        cls.stddev_crossover_instances = {}
        cls.trend_instances = {}
        
