from src.pipeline.training import Training
from src.log import logging
from src.statistical import outlier

from synapse.ml.isolationforest import IsolationForestModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

import streamlit as st

st.set_page_config("Anomalies",layout="wide")

spark = SparkSession.Builder().appName("Testing Session").getOrCreate()
logging.info(f"Spark Session: {spark.sparkContext.appName} created Successfully")

train = Training()
logging.info("Training Started")

model_path, raw_data_path = train.pipeline()
logging.info("Training Completed Successfully !")

model = IsolationForestModel.load(model_path)
df = spark.read.csv(raw_data_path)

assembler = VectorAssembler(
    inputCols = df.columns,
    outputCol="features"
)

df = assembler.transform(df)

prediction = model.transform(df)

anomalies_model = prediction.filter(prediction.prediction == 1)

outlier = outlier()
anomalies_iqr = outlier.Outliers()


col1,col2 = st.columns(2)

with col1:
    st.dataframe(anomalies_model)
with col2:
    st.dataframe(anomalies_iqr)

spark.stop()