 
from file_operations import file_methods
from data_preprocessing import preprocessing_prediction
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
import pickle
import pandas as pd
import numpy as np
import os

class prediction:

    def __init__(self,path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path)
    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile() 
            self.log_writer.log(self.file_object,'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data=data_getter.get_data()   
    
            """doing the data preprocessing""" 
            preprocessor=preprocessing_prediction.Preprocessor(self.file_object,self.log_writer)
            self.json_columns = ['device', 'geoNetwork','totals', 'trafficSource']
            data=preprocessor.decode_json(data,self.json_columns)
            
            
            data=preprocessor.add_date_features(data)
            data=preprocessor.del_const_col(data)
            replace_to_NaN=['unknown.unknown','(not set)','(not provided)','not available in demo dataset','(none)','<NA>'] 
            data=preprocessor.replace_to_nan(data,replace_to_NaN)
            
            drop_heavy_missing_val_feature=['geoNetwork_metro','date','visitNumber','trafficSource_adwordsClickInfo.gclId','trafficSource_keyword','trafficSource_campaign','trafficSource_adwordsClickInfo.page','trafficSource_adwordsClickInfo.slot','trafficSource_adwordsClickInfo.adNetworkType','trafficSource_adContent','trafficSource_adwordsClickInfo.isVideoAd','trafficSource_referralPath','trafficSource_isTrueDirect','geoNetwork_city','geoNetwork_region','visitStartTime\r\n','geoNetwork_networkDomain','sessionId']

            data=preprocessor.drop_unnecessary_feature(data,drop_heavy_missing_val_feature)
            
            num_data_df,num_data_col=preprocessor.extract_numerical_feature(data)
            
            cat_data_df,cat_data_col=preprocessor.extract_categorical_feature(data,num_data_col)
            data=preprocessor.missing_val_imputation_num(data)
            data=preprocessor.change_dtype_num_df(data)
            num_data_df=preprocessor.get_num_data_df(data,num_data_col)
            cat_data_df=preprocessor.missing_val_imputation_cat(cat_data_df)
            cat_data_df=preprocessor.Cat_encoding(cat_data_df)
            visit_id=preprocessor.extract_visitorId(cat_data_df)
            
            new_df=preprocessor.Concat_two_df(num_data_df,cat_data_df)
            new_df=preprocessor.normalize_num_col(new_df)
            # train_feature=preprocessor.training_feature(new_df)
            
            
            file_loader=file_methods.File_Operation(self.file_object,self.log_writer)
            self.model = file_loader.load_model()
            
            self.pred_val=self.model.predict(new_df.drop('fullVisitorId',axis=1))
            
            self.other_features_test=new_df['fullVisitorId']
            self.pred_val[self.pred_val<0] = 0
            self.val_pred_df = pd.DataFrame({"fullVisitorId":self.other_features_test.values})
            self.val_pred_df["Predicted_data"] = self.pred_val
            self.val_pred_df["PredictedRevenue"] = np.expm1(self.pred_val)
            self.val_pred_df = self.val_pred_df.groupby("fullVisitorId")["PredictedRevenue","Predicted_data"].sum().reset_index()
            
            path="Prediction_Output_File/Predictions.csv"
            self.val_pred_df.to_csv("Prediction_Output_File/Predictions.csv",header=True,mode='a+') 
            self.log_writer.log(self.file_object,'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path, self.val_pred_df.head(5).to_json(orient="records")         