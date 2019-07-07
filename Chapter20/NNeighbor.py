from sklearn.metrics.pairwise import euclidean_distances
from sklearn.metrics import r2_score
import pandas as pd

def _closest_one(X1, X2):
    matrix = euclidean_distances(X1, X2)
    
    d = pd.DataFrame(matrix,
                 index=X1.index, 
                 columns=X2.index)

    return d.idxmin(axis=1)


class NearestNeighbor:
    X = None
    y = None
    
    def __init__(self):
        pass
    
    def fit(self, X, y):
        self.X = X
        self.y = y
    

    def predict(self, X):
        closest = _closest_one(X, self.X)
        
        result = self.y[closest]
        result.index = X.index
        return result
    
    def score(self, X, y):
        y_pred = self.predict(X)
        return r2_score(y_pred, y)
        
        