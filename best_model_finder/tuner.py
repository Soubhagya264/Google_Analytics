import lightgbm as lgb
from sklearn import metrics
import xgboost
from sklearn.model_selection import RandomizedSearchCV
import numpy as np
import pandas as pd

class Model_Finder:
    """
                This class shall  be used to find the model with best accuracy and AUC score.
                Written By: Soubhagya Nayak
                """

    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.lgb = lgb
        self.xgb = xgboost.XGBRegressor()
        
    def get_best_params_for_lgb(self,train_x,valid_x,train_y,valid_y):
        """
                                Method Name: get_best_params_for_lgb
                                Description: get the parameters for light gradient boosting Algorithm which give the best accuracy.
                                             Use Hyper Parameter Tuning.
                                Output: The model with the best parameters
                                On Failure: Raise Exception

                                Written By: Soubhagya Nayak
                                """ 
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_lgb method of the Model_Finder class')
        try:                
             
            self.tuned_parameters = {'objective':'regression',
                        'metric':'rmse',
                        'num_leaves': 50,
                        'feature_fraction': 0.8,
                        'learning_rate': 0.02,
                        'bagging_fraction': 0.75,
                        'bagging_frequency': 9,
                        'num_iterations': int(4330.479382191752),
                        'max_depth': int(round(9.89309171116382)),
                        'lambda_l1': 3.902645881432277,
                        'lambda_l2': 0.4300598622271392,
                        'min_split_gain': 0.04205153205906184,
                        'min_child_weight': 25.526764949744685}  
            self.lgtrain = self.lgb.Dataset(train_x, label=np.log1p(train_y))
            self.lgvalid = self.lgb.Dataset(valid_x, label=np.log1p(valid_y))
            self.lgb_model = self.lgb.train(self.tuned_parameters, self.lgtrain, valid_sets=[self.lgvalid], early_stopping_rounds=100, verbose_eval=100)
            return self.lgb_model 
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_params_for_lgb method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'LGB Parameter tuning  failed. Exited the get_best_params_for_lgb method of the Model_Finder class')
            raise Exception()
    def get_best_params_for_xgb(self,train_x,valid_x,train_y,valid_y):
               """
                                Method Name: get_best_params_for_xgb
                                Description: get the parameters for XG Boost Algorithm which give the best accuracy.
                                             Use Hyper Parameter Tuning.
                                Output: The model with the best parameters
                                On Failure: Raise Exception

                                Written By: Soubhagya Nayak
                                """ 
               self.logger_object.log(self.file_object,
                               'Entered the get_best_params_for_xgboost method of the Model_Finder class')
               try:          
                    self.params={'booster': ['gbtree', 'gblinear'],
                                'n_estimators': [100, 300, 500, 700, 900],
                                'max_depth': [2, 3, 4, 5, 7, 8, 9, 10],
                                'min_child_weight': [1, 2, 3, 4, 5, 6],
                                'learning_rate': [0.05, 0.1, 0.15, 0.2],
                                'tree_method': ['gpu_hist', 'exact']
                                } 
                    self.random_cv=RandomizedSearchCV(estimator=self.xgb,param_distributions=self.params,cv=5,n_iter=50,scoring="neg_mean_absolute_error",return_train_score=True,random_state=42, n_jobs=-1)
                    self.random_cv.fit(train_x,np.log1p(train_y))
                    self.booster=self.random_cv.best_params_['booster']
                    self.n_estimators=self.random_cv.best_params_['n_estimators']
                    self.max_depth=self.random_cv.best_params_['max_depth']
                    self.min_child_weight=self.random_cv.best_params_['min_child_weight']
                    self.learning_rate=self.random_cv.best_params_['learning_rate']
                    self.tree_method=self.random_cv.best_params_['tree_method']
                     
                    
                    self.xgb=xgboost.XGBRegressor(booster=self.booster,n_estimators=self.n_estimators,max_depth=self.max_depth,min_child_weight=self.min_child_weight,learning_rate=self.learning_rate,tree_method=self.tree_method)
                    self.xgb.fit(train_x,np.log1p(train_y))
                    self.logger_object.log(self.file_object,
                                        'XGBoost best params: ' + str(
                                            self.random_cv.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
                    return self.xgb  
               except Exception as e:
                    self.logger_object.log(self.file_object,
                                        'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(
                                            e))
                    self.logger_object.log(self.file_object,
                                        'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
                    raise Exception()  
    def get_mse(self,fvi,pred_val,valid_x,valid_y):
        
            self.fvi=fvi
            self.pred_val=pred_val
            self.pred_val[self.pred_val<0] = 0
            self.val_pred_df = pd.DataFrame({"fullVisitorId":self.fvi.values})
            self.val_pred_df["transactionRevenue"] = valid_y.values
            self.val_pred_df["transactionRevenue_log"] = np.log1p(valid_y.values)
            self.val_pred_df["Predicted_data"] = self.pred_val
            self.val_pred_df["PredictedRevenue"] = np.expm1(self.pred_val)
            self.val_pred_df = self.val_pred_df.groupby("fullVisitorId")["transactionRevenue", "PredictedRevenue","transactionRevenue_log","Predicted_data"].sum().reset_index()
            self.mse=(np.sqrt(metrics.mean_squared_error(np.log1p(self.val_pred_df["transactionRevenue"].values), np.log1p(self.val_pred_df["PredictedRevenue"].values))))
            return self.val_pred_df, self.mse
                     
    def get_best_model(self,fvi,train_x,valid_x,train_y,valid_y):
        """
                                                Method Name: get_best_model
                                                Description: Find out the Model which has low mean_square_error.
                                                Output: The best model name and the model object
                                                On Failure: Raise Exception

                                                Written By: Soubhagya Nayak

                                        """
        self.logger_object.log(self.file_object,
                               'Entered the get_best_model method of the Model_Finder class')
        
        try:  
            train_x=train_x.drop(['fullVisitorId'],axis=1)
            valid_x=valid_x.drop(['fullVisitorId'],axis=1)
            self.xgboost= self.get_best_params_for_xgb(train_x,valid_x,train_y,valid_y) 
            self.prediction_xgboost = self.xgboost.predict(valid_x) 
            self.xgb_df,self.xgboost_mse=self.get_mse(fvi,self.prediction_xgboost,valid_x,valid_y)  
            self.logger_object.log(self.file_object,
                               'XGboost Prediction done')
            self.lgb_model= self.get_best_params_for_lgb(train_x,valid_x,train_y,valid_y) 
            self.prediction_lgb = self.lgb_model.predict(valid_x,num_iteration=self.lgb_model.best_iteration)
            self.lgb_df,self.lgb_mse=self.get_mse(fvi,self.prediction_lgb,valid_x,valid_y)
            self.logger_object.log(self.file_object,
                               'LGB Prediction done')
            
            if self.xgboost_mse<self.lgb_mse:
                return 'XGBoost',self.xgboost
            else:
                return 'LGB',self.lgb_model
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()    

                
                                     
                           