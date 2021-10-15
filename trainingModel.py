
"""
This is the Entry point for Training the Machine Learning Model.
Written By: Soubhagya Nayak
"""
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger

class trainModel: 
    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
    def trainingModel(self):
        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            data_getter=data_loader.Data_Getter(self.file_object,self.log_writer)
            data=data_getter.get_data() 
            
            """doing the data preprocessing""" 
            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            json_columns = ['device', 'geoNetwork','totals', 'trafficSource']
            data=preprocessor.decode_json(data,json_columns)
            data=preprocessor.drop_nan_from_target(data)
            
            data=preprocessor.add_date_features(data)
            print(data.head())
            
            replace_to_NaN=['unknown.unknown','(not set)','(not provided)','not available in demo dataset','(none)','<NA>'] 
            data=preprocessor.replace_to_nan(data,replace_to_NaN)
            drop_heavy_missing_val_feature=['geoNetwork_metro','date','totals_visits','trafficSource_adwordsClickInfo.gclId','trafficSource_keyword','trafficSource_campaign','trafficSource_adwordsClickInfo.page','trafficSource_adwordsClickInfo.slot','trafficSource_adwordsClickInfo.adNetworkType','trafficSource_adContent','trafficSource_adwordsClickInfo.isVideoAd','trafficSource_referralPath','trafficSource_isTrueDirect','visitNumber','geoNetwork_networkDomain','geoNetwork_city','geoNetwork_region','visitStartTime\r\n','socialEngagementType','device_browserVersion','device_browserSize','device_operatingSystemVersion','device_mobileDeviceBranding','device_mobileDeviceModel','device_mobileInputSelector','device_mobileDeviceInfo','device_mobileDeviceMarketingName','device_flashVersion','device_language','device_screenColors','device_screenResolution','geoNetwork_cityId','geoNetwork_latitude','geoNetwork_longitude','geoNetwork_networkLocation','trafficSource_adwordsClickInfo.criteriaParameters','trafficSource_campaignCode']
            data=preprocessor.drop_unnecessary_feature(data,drop_heavy_missing_val_feature)
            data=preprocessor.change_target_var_to_numeric(data)
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
            train_feature=preprocessor.training_feature(new_df)
            self.log_writer.log(self.file_object, 'Preprocessing done')
            
            
            self.log_writer.log(self.file_object, 'Train Test Split started')
            train_x, valid_x, train_y, valid_y = train_test_split(new_df[train_feature],new_df["totals_transactionRevenue"], test_size=0.25, random_state=20)
            self.log_writer.log(self.file_object, 'Train Test Split done')
            
            model_finder=tuner.Model_Finder(self.file_object,self.log_writer)
            fvi=valid_x['fullVisitorId']
            best_model_name,best_model=model_finder.get_best_model(fvi,train_x,valid_x,train_y,valid_y)
            
            file_op = file_methods.File_Operation(self.file_object,self.log_writer)
            save_model=file_op.save_model(best_model,best_model_name)
            
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()
            return 'Success'

        except Exception:
            
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception    
            
            
            
            
              