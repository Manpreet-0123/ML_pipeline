import os
import sys
from dataclasses import dataclass

from src.log import logging
from src.exception import CustomException

from synapse.ml.isolationforest import IsolationForest
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

class ModelTrainerConfig:
    def __init__(self,time):
        model_path = os.path.join("artifacts",time,"model.pkl")


class ModelTrainer:
    def __init__(self,time):
        self.model_path_config = ModelTrainerConfig(time)

    def model_train(self,train_df):
        try:
            spark = SparkSession.builder.appName("Model Training Session").getOrCreate()
            logging.info(f"Spark Session: {spark.sparkContext.appName} created Successfully !")

            assembler = VectorAssembler(
                inputCols = train_df.columns,
                outputCol="features"
            )
            logging.info("Data Assembled Successfully")

            train_assembled_df = assembler.transform(train_df)

            isolation_forest = IsolationForest(
                numEstimators=100,
                contamination=0.05,
                featuresCol="features",
                predictionCol="prediction",
                anomalyScoreCol="anomalyScore"
            )

            model = isolation_forest.fit(train_assembled_df)
            logging.info("Model Created")

            model.write.save(self.model_path_config.model_path)
            logging.info("Model Saved")

            spark.stop()
            return self.model_path_config.model_path

        except Exception as e:
            raise CustomException(e,sys)