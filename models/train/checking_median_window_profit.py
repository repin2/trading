from sklearn import linear_model
from sklearn.svm import SVC
from sklearn.model_selection import cross_validate
from sklearn.preprocessing import normalize
import numpy as np


checking_median_window_profit_model = linear_model.ARDRegression()


class InterFace:
    def __init__(self, **kwargs):
        super().__init__()
        if kwargs.get('train_vars') is not None and kwargs.get('train_profit') is not None:
            self.init_train_data(kwargs['train_vars'], kwargs['train_profit'])
            self.fit(self.train_vars, self.train_profit)

    def init_train_data(self, train_vars: np.array, train_profit: np.array):
        self.train_vars = normalize(train_vars)
        self.train_profit = train_profit

    def test_predict(self):
        predicted = self.predict(self.train_vars)
        predicted = np.sign(predicted)
        res = (predicted * self.train_profit).sum()
        return predicted


class CheckinMedianWindowProfitModel(linear_model.ARDRegression, InterFace):
    pass


class CheckinMedianWindowProfitModelSVM(InterFace, SVC):
    pass
    # def cross_validation(self):
    #     scoring = ['precision_macro', 'recall_macro']
