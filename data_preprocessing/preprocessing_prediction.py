# import pandas as pd
# import numpy as np
# import json
# from pandas.io.json import json_normalize
# from sklearn.preprocessing import LabelEncoder



import numpy as np
import json
from pandas import json_normalize
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import os



class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.
        Written By: Soubhagya Nayak

        """
    def __init__(self,file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        
        
    def decode_json(self,data,json_col):
        """
                Method Name: decode_json
                Description: This method convert the json object to single column .
                Output: A pandas DataFrame after converting  the specified columns.
                On Failure: Raise Exception
                Written By: Soubhagya Nayak

        """
       
        
        data.to_csv('InputFile2.csv',index=False)
        
        self.data='InputFile2.csv'
        
        self.json_col=json_col
        
        self.logger_object.log(self.file_object, 'Json data convert into new rows')
        try:
            self.data1 = pd.read_csv(self.data, converters={column: json.loads for column in self.json_col}, 
                     dtype={'fullVisitorId': 'str'})
            print(self.data1.head())

            for column in self.json_col:
                column_as_df=json_normalize(self.data1[column])
                column_as_df.columns=[f"{column}_{subcolumn}" for subcolumn in column_as_df.columns] 
                self.data1 = self.data1.drop(column, axis=1).merge(column_as_df, right_index=True, left_index=True)  
            return self.data1
        except Exception as e:
           self.logger_object.log(self.file_object,'Exception occured in conversion json to normal. Exception message:  '+str(e))
           self.logger_object.log(self.file_object,
                                   'json normalization Unsuccessful.')
                   
           raise Exception()    
    
    # def drop_nan_from_target(self,data):
    #     """
    #             Method Name: drop_nan_from_target
    #             Description: This method drop the nan value from the target variable .
    #             Output: A cleaned or without any null value in target feature after removing  the null values.
    #             On Failure: Raise Exception
    #             Written By: Soubhagya Nayak

    #     """
        
    #     self.logger_object.log(self.file_object, 'Started droping nan value from target variable')
    #     data.dropna(subset=['totals_transactionRevenue'],inplace=True)
    #     self.logger_object.log(self.file_object, 'removed nan value from target variable')
    #     return data
    
    def add_date_features(self,df):
        """
                Method Name: add_date_features
                Description: This method is to extract the month week and day from date .
                On Failure: Raise Exception
                Written By: Soubhagya Nayak

        """
        try:
                df['date'] = df['date'].astype(str)
                df["date"] = df["date"].apply(lambda x : x[:4] + "-" + x[4:6] + "-" + x[6:])
                df["date"] = pd.to_datetime(df["date"])

                df["month"]   = df['date'].dt.month
                df["day"]     = df['date'].dt.day
                df["weekday"] = df['date'].dt.weekday
                return df
        except Exception as e:
             self.logger_object.log(self.file_object, 'Error occured during extract feature from date')
                   
        
    def del_const_col(self,test):
        
        
        try:
            self.logger_object.log(self.file_object, 'Started removing const col')  
             
            test = test.loc[:, (test != test.iloc[0]).any()]    
            
            return test
        except Exception as e:
            self.logger_object.log(self.file_object, 'Error occured during removing constant col' )
                             
        
            
    def replace_to_nan(self,test,replace_to_NaN):
        """
                Method Name: replace_to_nan
                Description: This method is to extract the month week and day from date .
                On Failure: Raise Exception
                Written By: Soubhagya Nayak

        """
        self.logger_object.log(self.file_object, 'replacing other value ') 
         
        
        try:
            for col in test.columns:
                for val in test[col]:
                    if val in replace_to_NaN:
                        test[col]=test[col].replace(val,np.nan)
            return test               
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured ',e)                        
        
        self.logger_object.log(self.file_object, 'replaced other value ')                   
        
    
    def drop_unnecessary_feature(self,df,drop_heavy_missing_val_feature):
        self.logger_object.log(self.file_object, 'dropping unnecessary featured ') 
        for fea in drop_heavy_missing_val_feature:
            if fea in df.columns:
                 df.drop(columns=fea,axis=1,inplace=True)
        self.logger_object.log(self.file_object, 'droped unnecessary featured ')          
        return df
    def change_target_var_to_numeric(self,df):
        self.logger_object.log(self.file_object, 'change_target_var_to_numeric ') 
        
        df['totals_transactionRevenue']=pd.to_numeric(df['totals_transactionRevenue'], errors='ignore')
        self.logger_object.log(self.file_object, 'changed_target_var_to_numeric ') 
        return df
    
    def extract_numerical_feature(self,df):
        self.logger_object.log(self.file_object, ' Extracting Neumerical feature from data')
        
        numeric_features_df = df.select_dtypes(include=[np.number])
        num_data_col=[data for data in numeric_features_df.columns ] 
        
        if 'visitId' in num_data_col:
            num_data_col.remove('visitId')
        if 'visitNumber' in num_data_col:   
            num_data_col.remove('visitNumber')
        if 'totals_bounces' not in num_data_col:    
            num_data_col.append('totals_bounces')
        if 'totals_hits' not in num_data_col:  
            num_data_col.append('totals_hits')
        if 'totals_newVisits' not in num_data_col:
             num_data_col.append('totals_newVisits')
        if 'totals_pageviews' not in num_data_col:
              num_data_col.append('totals_pageviews')
        
        
        self.logger_object.log(self.file_object, 'Extracted Neumerical feature from data')        
        return numeric_features_df,num_data_col
    
    def extract_categorical_feature(self,df,num_data_col):
        self.logger_object.log(self.file_object, ' Extracting categorical feature from data')
        
        cat_features_df = df.drop(num_data_col,axis=1)
            
        cat_data_col=[data for data in cat_features_df.columns ] 
        self.logger_object.log(self.file_object, ' Extracted categorical feature from data')        
        return cat_features_df,cat_data_col
    
    def missing_val_imputation_num(self,data):
        
        
        data['totals_bounces'] = data['totals_bounces'].fillna(0)

        data['totals_newVisits'] = data['totals_newVisits'].fillna(0)

        data['totals_pageviews'] = data['totals_pageviews'].fillna(1)
          
        return data
    def get_num_data_df(self,data,num_data_col):
        num_data_df=data[num_data_col]
        return num_data_df
    def change_dtype_num_df(self,data):
        data=data
        for col in ['totals_bounces', 'totals_hits', 'totals_newVisits','totals_pageviews' ]:
             data[col] = data[col].astype(int)
          
        return data 
    def missing_val_imputation_cat(self,data):
        Cat_feature_train=data
        lis=['sessionId','visitId','visitStartTime']
        for i in lis:
            if i in Cat_feature_train.columns:
                Cat_feature_train.drop(i,axis=1,inplace=True)
        
        Cat_feature_train['trafficSource_medium'].fillna('other', inplace=True)


        Cat_feature_train['device_operatingSystem'].fillna('other', inplace=True)


        Cat_feature_train['device_browser'].fillna('other', inplace=True)



        Cat_feature_train['geoNetwork_continent'].fillna('other', inplace=True)


        Cat_feature_train['geoNetwork_subContinent'].fillna('other', inplace=True)


        Cat_feature_train['geoNetwork_country'].fillna('other', inplace=True)


        Cat_feature_train['trafficSource_source'].fillna('other', inplace=True)
                
        return Cat_feature_train  
    def extract_visitorId(self,data):
        Cat_feature_train=data
        other_feature=Cat_feature_train['fullVisitorId']
        #self.Cat_feature_train.drop(['fullVisitorId'],axis=1,inplace=True)
        return other_feature
    def Cat_encoding(self,cat_data):
        Cat_feature_train=cat_data
        for col in Cat_feature_train:
            if col == 'fullVisitorId':
                continue
            print("transform column {}".format(col))
            lbe = LabelEncoder()
            lbe.fit((Cat_feature_train[col]).astype("str"))
            Cat_feature_train[col] = lbe.transform(Cat_feature_train[col].astype("str"))
    
        return Cat_feature_train
    
    def Concat_two_df(self,num_df,cat_df):
        num_df=num_df
        cat_df=cat_df
        train_df=pd.concat([num_df,cat_df],axis=1)
        return train_df
    def normalize_num_col(self,df):
        
        df["totals_hits"] = df["totals_hits"].astype(float)
        df["totals_hits"] = (df["totals_hits"] - min(df["totals_hits"])) / (max(df["totals_hits"]) - min(df["totals_hits"]))

        df["totals_pageviews"] = df["totals_pageviews"].astype(float)
        df["totals_pageviews"] = (df["totals_pageviews"] - min(df["totals_pageviews"])) / (max(df["totals_pageviews"]) - min(df["totals_pageviews"]))
            
        return df
    def training_feature(self,data):
        
        feature=[col for col in data.columns]
        feature.remove("totals_transactionRevenue")
        return feature
    
                    
        



