from sqlalchemy import create_engine
from sqlalchemy import text
import json
import os
import pandas as pd
from pathlib import Path
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from sklearn.model_selection import train_test_split
from catboost import CatBoostRegressor, Pool, metrics
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from datetime import datetime
import shap

class OfferDF:
    def __init__(self, connection_string, sql_path=None, features_path='./src/features.json'):                
        if sql_path:
            with open(sql_path, 'r', encoding='utf-8') as f:            
                self.sql = f.read()

        with open(features_path, 'r', encoding='utf-8') as f:            
            self.features = json.load(f)
            
        self.target = self.features.get('target')
        self.cat_cols = self.features.get('cat_cols')
        self.int_cols = self.features.get('int_cols')
        self.float_cols = self.features.get('float_cols')
        self.bool_cols = self.features.get('bool_cols')

        self.engine = create_engine(connection_string)
    
    def read_sql(self, sql_text = None):
        if sql_text:
            self.sql = sql_text

        with self.engine.connect().execution_options(autocommit=True) as conn:
            df = pd.read_sql(text(self.sql), con=conn)
        df = df.pivot(index= 'offer_id', columns='prop', values='val')

        for col in self.int_cols: 
            if df.get(col) is not None:   
                df[col] = df.get(col).fillna(0)
                df[col] = df.get(col).astype('int')
            else:
                df[col] = 0 
        for col in self.float_cols:
            if df.get(col) is not None:  
                df[col] = df.get(col).astype('float')#.replace({pd.NA: np.nan})
            else:
                df[col] = 0 
        for col in self.bool_cols:
            if df.get(col) is not None:
                df[col] = df.get(col).map({'True': 1, 'False': 0, 'true': 1, 'false': 0})
            else:
                df[col] = 0 
        for col in self.cat_cols: 
            if df.get(col) is not None:    
                df[col] = df.get(col).fillna('unknown')
            else:
                df[col] = 'unknown'
        
        return df.dropna(subset=[self.target])
    
class CatBoostModel:
    def __init__(self):
        self.date_time = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
        self.model = CatBoostRegressor(loss_function='RMSE', train_dir='./src/model/' + self.date_time + '/')

        self.grid = {'iterations': [100, 150, 200],
                'learning_rate': [0.03, 0.1],
                'depth': [2, 4, 6, 8],
                'l2_leaf_reg': [0.2, 0.5, 1, 3]}
        
    def fit(self, train_dataset):
        self.model.grid_search(self.grid, train_dataset)

    def predict(self, X_test, y_test):
        pred = self.model.predict(X_test)
        rmse = (np.sqrt(mean_squared_error(y_test, pred)))
        r2 = r2_score(y_test, pred)
        with open('./src/model/' + self.date_time + '/performance.txt', 'w') as f:
            f.writelines(['RMSE: {:.2f}'.format(rmse), '/n' + 'R2: {:.2f}'.format(r2)])
                        
        sorted_feature_importance = self.model.feature_importances_.argsort()
        plt.barh(X_test.columns[sorted_feature_importance], 
                self.model.feature_importances_[sorted_feature_importance], 
                color='turquoise')
        plt.xlabel("CatBoost Feature Importance")
        
        plt.savefig('./src/model/' + self.date_time + '/Feature_Importance.png')

if __name__ == '__main__': 
    train = OfferDF('train.sql')
    train_data = train.read_sql()

    test = OfferDF('test.sql')
    test_data = test.read_sql()

    train_data = train_data.drop(test_data.index)

    X_train = pd.concat([train_data[train.int_cols], train_data[train.float_cols], train_data[train.bool_cols], train_data[train.cat_cols]], axis=1)
    y_train = train_data[train.target]
    
    X_test = pd.concat([test_data[test.int_cols], test_data[test.float_cols], test_data[test.bool_cols], test_data[test.cat_cols]], axis=1)
    y_test = test_data[test.target]
    
    train_dataset = Pool(X_train, y_train, cat_features=train.cat_cols) 
    cat = CatBoostModel()
    cat.fit(train_dataset)
    cat.predict(X_test, y_test)
