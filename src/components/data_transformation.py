import os
import sys
from dataclasses import dataclass

from src.log import logging
from src.exception import CustomException

from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.sql.functions import vector_to_array,col

TARGET_NAME = ""

@dataclass
class DataTransformationConfig:
    preprocessor_path: str = os.path.join("artifacts","preprocessor.pkl")

class DataTransformation:
    def __init__(self):
        self.transform_config = DataTransformationConfig()

    def pipeline_creation(self,df):
        try:
            df = df.drop(TARGET_NAME)
            cat_col = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
            num_col = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]

            indexer = [StringIndexer(inputCol=c, outputCol=c + "_index", handeInvalid="keep") for c in cat_col]
            ohe = [OneHotEncoder(inputCol=[c + "_index"], outputCol=c + "_vec") for c in cat_col]

            assembler = VectorAssembler(
                inputCols=num_col,
                outputCol="numeric_features"
            )
            
            scaler = StandardScaler(
                inputCol="numeric_features",
                outputCol="scaled_numeric_features"
            )

            final_assembler = VectorAssembler(
                inputCols=[c + "_vec" for c in cat_col] + ["scaled_numeric_features"],
                outputCols="final_features"
            )

            pipeline = Pipeline(stages= indexer + ohe + [assembler, scaler, final_assembler])

        except Exception as e:
            raise CustomException(e,sys)

    def data_preparation(self,train_path,test_path):
        try:
            spark = SparkSession.builder.appName("Transformation Session").getOrCreate()
            logging.info(f"Spark Session: {spark.sparkContext.appName} created Successfully !")
            train_df = spark.read.csv(train_path)
            test_df = spark.read.csv(test_path)
            if train_df != None and test_df != None:
                logging.info("Training & Testing Data is Read Successfully !")

                raw_pipeline = self.pipeline_creation(train_df)

                pipeline = raw_pipeline.fit(train_df)

                scaled_train_features = pipeline.transform(train_df)
                scaled_test_features = pipeline.transform(test_df)

                pipeline.write.save(self.transform_config.preprocessor_path)

                scaled_train_features = scaled_train_features.withColumn(
                    "final_features_array", vector_to_array("final_features")
                )
                scaled_test_features = scaled_test_features.withColumn(
                    "final_features_array", vector_to_array("final_features")
                )

                train_final = scaled_train_features.select(
                    col("final_features_array").alias("features"),
                    col(TARGET_NAME).alias("label")
                )

                test_final = scaled_test_features.select(
                    col("final_features_array").alias("features"),
                    col(TARGET_NAME).alias("label")
                )

                return train_final,test_final
            else:
                logging.info("Training & Testing Data is Not Read Successfully !")

        except Exception as e:
            raise CustomException(e,sys)