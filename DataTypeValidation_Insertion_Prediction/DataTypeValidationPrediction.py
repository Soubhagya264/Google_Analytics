import os
import shutil
import csv
from application_logging.logger import App_Logger
import pymongo 
from os import listdir

class dBOperation:
    """
          This class shall be used for handling all the SQL operations.

          Written By: Soubhagya Nayak

          """

    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()
        
    def dataBaseConnection(self,DatabaseName):

        """
                Method Name: dataBaseConnection
                Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
                Output: Connection to the DB
                On Failure: Raise ConnectionError

                 Written By: Soubhagya Nayak

                """  
        try:
               client=pymongo.MongoClient("mongodb://localhost:27017/")
               file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
               db_1 = client[DatabaseName]
               self.logger.log(file, "Opened %s database successfully" % DatabaseName)
               file.close()
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return db_1   
    def createTable_Insert(self,DatabaseName,column_names):
        """
        Method Name: createTableDb
        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception
        Written By: Soubhagya Nayak

        """ 
        
        
        
        db=self.dataBaseConnection(DatabaseName) 
                    
        if ("Good_Raw_Data" in db.list_collection_names()):
            collection=db["Good_Raw_Data"]
            collection.drop()
         
        collection=db["Good_Raw_Data"]    
        file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
        self.logger.log(file, "Tables created successfully!!")
        file.close()
        goodFilePath= self.goodFilePath
        
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')
        for file in onlyfiles:
                try:
                    with open(goodFilePath+'/'+file, "r") as f:
                        header=next(f)
                        print((header))
                        reader = csv.reader(f)
                        try:
                            for row in reader:
                                
                                    doc={}
                                    for n in range(0,len(header.split(','))):
                                        doc[header.split(',')[n]] =  row[n]
                                    collection.insert(doc)
                            self.logger.log(log_file," %s: File loaded successfully!!" % file)          
                                
                        except Exception as e:
                                self.logger.log(log_file," %s: Error while file loading" % file)
                                raise e
                         
                
                except Exception as e:
                    file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
                    self.logger.log(file, "Error while creating table and Inserting data: %s " % e)
                    file.close()
                    shutil.move(goodFilePath+'/' + file, badFilePath)
                    self.logger.log(log_file, "File Moved Successfully %s" % file)
                    log_file.close()
                    
                    file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
                    self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                    file.close()
                    raise e     
    def selectingDatafromtableintocsv(self,Database):
            """
                                Method Name: selectingDatafromtableintocsv
                                Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                                above created .
                                Output: None
                                On Failure: Raise Exception

                                    Written By: Soubhagya Nayak

            """
            self.fileFromDb = 'Prediction_FileFromDB/'
            self.fileName = 'InputFile.csv'
            self.db=self.dataBaseConnection(Database)
            self.collection=self.db["Good_Raw_Data"]
            self.csv_columns=[]
            self.stack=[]
            self.count=0
            for x in self.collection.find():
                print(x.keys())
                self.stack.append(x.keys())
                self.count+=1
                if self.count==1:
                    break
            for i in self.stack[0]:
                if i=="_id":
                    continue
                self.csv_columns.append(i)
                
                
            if not os.path.isdir(self.fileFromDb):
                    os.makedirs(self.fileFromDb)    
            
            with open(self.fileFromDb + self.fileName, 'w') as csvfile:
                try:
                    log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
                    writer = csv.DictWriter(csvfile, fieldnames=self.csv_columns)
                    writer.writeheader()
                    for x in self.collection.find():
                        del x['_id']    
                        writer.writerow(x)
                    self.logger.log(log_file, "File exported successfully!!!")
                    log_file.close()
                
                except Exception as e:
                    log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
                    self.logger.log(log_file, "File exported Unsuccessfully!!!")
                    log_file.close()
                        
            