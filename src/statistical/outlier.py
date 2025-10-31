from pyspark.sql import SparkSession
from pyspark.sql.functions import col

ANOMALY = ""
class Outliers():
    def find_outliers(self,raw_data_path):
        spark = SparkSession.builder().appName("Outliers").getOrCreate()
        df = spark.read.csv(raw_data_path)
        q1,q3 = df.approxQuantile(ANOMALY,[0.25,0.75],0.0)

        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        anomalies = df.filter((col(ANOMALY) < lower_bound) & (col(ANOMALY) > upper_bound))

        spark.stop()
        return anomalies 