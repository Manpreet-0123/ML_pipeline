import sys
import os
from datetime import datetime
from dataclasses import dataclass

from src.log import logging
from src.exception import CustomException

from pyspark.sql import SparkSession

@dataclass
class DataIngestionConfig:
    time = f"Data Created at: {datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}"
    raw_data_path: str = os.path.join("artifacts",time,"raw_data.csv")
    train_data_path: str = os.path.join("artifacts",time,"train_data.csv")
    test_data_path: str = os.path.join("artifacts",time,"test_data.csv")

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def datasets_creation(self):
        spark = SparkSession.builder\
        .config("spark.jars","C:\\Users\\manpr\\Desktop\\Work\\Requires\\postgresql-42.7.8.jar")\
        .appName("Experiment")\
        .getOrCreate()        
        logging.info(f"Spark Session {spark.sparkContext.appName} created successfully")
        jdbc_url = "postgresql://zodiac-rds.choegu2w8qpt.ap-south-1.rds.amazonaws.com:5432/zodiac"

        jdbc_prop = {
            "user" : "postgres",
            "password" : "6*HT!99052y628a85a2-9474-475a-b234-f00bbae11964",
        }
        try:
            df = spark.read.jdbc(url=jdbc_url,table="BlinkitBrand",properties=jdbc_prop)

            # Columns required selected with SQL Queries

            if df != None:
                logging.info("Data is Read Successfully !")

                df.write.save(self.ingestion_config.raw_data_path)
                logging.info("Raw Data Saved Successfully !")
                
                train_df, test_df = df.randomSplit([0.8,0.2], seed = 42)

                train_df.write.save(self.ingestion_config.train_data_path)
                test_df.write.save(self.ingestion_config.test_data_path)
                logging.info("Training and Testing Data Saved Successfully !")

                spark.stop()
                return self.ingestion_config.raw_data_path,self.ingestion_config.train_data_path,self.ingestion_config.test_data_path,self.ingestion_config.time
            else:
                logging.info("Data is Empty !")

        except Exception as e:
            raise CustomException(e,sys)