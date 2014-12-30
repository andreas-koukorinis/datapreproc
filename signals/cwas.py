import sys
from signals.signal_algorithm import SignalAlgorithm
from Utils.Regular import check_eod, adjust_file_path_for_home_directory, is_float_zero

class CWAS(SignalAlgorithm):
    """Implementation of the constant weight allcation system, that assigns constant weight to each product as specified in the model file
    The weights are rebalanced to the constant weights on each rebalancing day
    NOTE: In this implementation the weights are normalized (abs sum to 1)

    Items read from config :
        weights:
    """
    def init(self, _config):
        #Defaults
        self.weights = dict([(product, 1.0/len(self.products)) for product in self.products]) # Equal weighted long portfolio by default

        _paramfilepath="/dev/null"
        if _config.has_option('Parameters', 'paramfilepath'):
            _paramfilepath=adjust_file_path_for_home_directory(_config.get('Parameters', 'paramfilepath'))
        self.process_param_file(_paramfilepath, _config)

        _modelfilepath="/dev/null"
        if _config.has_option('Strategy','modelfilepath'):
            _modelfilepath=adjust_file_path_for_home_directory(_config.get('Strategy','modelfilepath'))
        self.process_model_file(_modelfilepath, _config)

        # Normalize for leverage = 1
        _sum_abs_weights = 0.0
        for _product in self.weights.keys():
            _sum_abs_weights += abs(self.weights[_product])
        if is_float_zero(_sum_abs_weights):
            sys.exit('Something wrong! Sum(Abs(weights)) is 0')
        for _product in self.weights.keys():
            self.weights[_product] = self.weights[_product]/_sum_abs_weights
        
    def process_param_file(self, _paramfilepath, _config):
        super(CWAS, self).process_param_file(_paramfilepath, _config)

    def process_model_file(self, _modelfilepath, _config):
        _model_file_handle = open( _modelfilepath, "r" )
        for _model_line in _model_file_handle:
            # We expect lines like the following for each traded product:
            # fES 0.6
            # fZN 0.4
            _model_line_words = _model_line.strip().split(' ')
            if len(_model_line_words) == 2:
                _product = _model_line_words[0]
                _weight = float(_model_line_words[1])
                self.weights[_product] = _weight

    def on_events_update(self, events):
        all_eod = check_eod(events)  # Check whether all the events are ENDOFDAY
        if all_eod: self.day += 1  # Track the current day number

        if all_eod and (self.day - self.last_rebalanced_day >= self.rebalance_frequency):
            self.update_positions( events[0]['dt'], self.weights )
            self.last_rebalanced_day = self.day
        else:
            self.rollover( events[0]['dt'] )
