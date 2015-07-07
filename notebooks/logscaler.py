import pandas as pd
import numpy as np
import sklearn

class LogScaler(sklearn.base.TransformerMixin):
    def __init__(self, base = 10, colnames=None):
        self.base = base
        self.colnames = colnames
        pass
    
    def fit(self, X, y=None, **fit_params):
        return self
    
    def transform(self, X, y=None, copy=None, correction=0.01):
        """Log transform X or columns in X identified by
        colnames kwarg
        
        Arguments
        X         dataframe or numpy array to trasnform
        colnames  names of columns to transform
        base      base of log to transform, (10 if not given)
        """
        
        try:
            new_X_cols = np.log(X.loc[:,self.colnames].replace(0,correction)) / \
                                            np.log(self.base)
            new_X = pd.concat((X.loc[:,[c for c in X.columns if c not in self.colnames]],
                               new_X_cols),axis=1)[X.columns]
            #print new_X.head()
        except AttributeError, ValueError:
            new_X = np.log([np.max(x,correction) for x in X])/np.log(self.base)
        
        return new_X
