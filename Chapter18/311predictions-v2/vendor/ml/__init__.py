from sklearn.base import BaseEstimator
import numpy as np

try:
    import pandas as pd
except ImportError:
    pass



def day_of_week_num(dts):
    return (dts.astype('datetime64[D]').view('int64') - 4) % 7

def day_of_year_num(dts):
    return (dts.astype('datetime64[D]').view('int64') - dts.astype('datetime64[Y]').astype('datetime64[D]').view('int64'))

def time_of_day_num(dts):
    return dts.astype('datetime64[s]').view('int64') - dts.astype('datetime64[D]').astype('datetime64[s]').view('int64')

class TimeTransformer(BaseEstimator):
    cols = None
    indices = None
    
    def __init__(self, cols=None, indices=None):
        self.cols = cols
        self.indices = indices
    
    def fit(self, X=None, y=None, groups=None):
        '''here, X should be a Dataframe, at least for now'''
        
        if self.cols is None:
            self.cols = X.select_dtypes(include=pd.np.datetime64).columns
        
        if self.indices is None:
            self.indices = [X.columns.get_loc(c) for c in self.cols if c in X]

        return self
    
    def transform(self, X, y=None, groups=None, cols=None):
        
        if str(type(X)) == "<class 'numpy.ndarray'>":
            for i in self.indices:
                dates = X[:, i]
                X = np.delete(X, i, 1)
                newcols = np.empty(shape=(X.shape[0], 3), dtype=np.int64)
                
                newcols[:, 0] = day_of_week_num(dates)
                newcols[:, 1] = day_of_year_num(dates)
                newcols[:, 2] = time_of_day_num(dates)

                X = np.append(X, newcols, 1)

        elif isinstance(X, pd.DataFrame):
            for col in self.cols:
                dates = X[col]
                X = X.drop(col, axis=1)
                X[f'{col}_dow'] = dates.dt.dayofweek
                X[f'{col}_doy'] = dates.dt.dayofyear
                X[f'{col}_tod'] = dates.dt.second

        return X