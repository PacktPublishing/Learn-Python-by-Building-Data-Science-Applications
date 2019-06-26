from sklearn.base import BaseEstimator
import pandas as pd

class TimeTransformer(BaseEstimator):
    cols = None
    
    def __init__(self, cols=None):
        self.cols = cols
    
    def fit(self, X=None, y=None, groups=None):
        
        if self.cols is None:
            self.cols = X.select_dtypes(include=pd.np.datetime64).columns
        return self
    
    def transform(self, X, y=None, groups=None, cols=None):
        
        for col in self.cols:
            dates = X[col]
            X = X.drop(col, axis=1)
            X[f'{col}_dow'] = dates.dt.dayofweek
            X[f'{col}_doy'] = dates.dt.dayofyear
            X[f'{col}_tod'] = dates.dt.second

        return X