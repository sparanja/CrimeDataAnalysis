import os
import findspark

findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
os.environ["SPARK_HOME"] = "/home/ubuntu/spark-2.1.1-bin-hadoop2.7"

import pyspark
from pyspark.sql import SparkSession
import flask
import pandas

app = flask.Flask(__name__)
app.config["DEBUG"] = True
df = None
spark = None

def initialize_spark_session():
    global spark
    if spark is not None:
        return
    spark = SparkSession.builder.appName('S3CSVRead').getOrCreate()

    accessKeyId="AKIAJYEHZOGXDALZ2KJQ"
    secretAccessKey="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"

    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", accessKeyId)
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", secretAccessKey)

    global df
    if df is None:
        df=spark.read.csv("s3a://crime-data-analysis/Chicago_Crimes_2001_to_2004.csv", header=True)
        df.show(20, False)
    else:
        print('Dataframe already initialized');


@app.route('/homicide_by_year', methods=['GET'])
def home():
    initialize_spark_session()
    frames = df.where("PrimaryType=='HOMICIDE'").groupBy("Year_T").count().orderBy("Year_T")
    pandasDF = frames.toPandas()
    return pandasDF.to_json()

@app.route('/crimes_over_time', methods=['GET'])
def crime_over_time():
    initialize_spark_session()
    frames = df.groupBy("PrimaryType", "Year_T").count().orderBy("Year_T","PrimaryType")
    pandasDF = frames.toPandas()
    return pandasDF.to_json()

@app.route('/arrests_over_time', methods=['GET'])
def arrest_over_time():
    initialize_spark_session()
    frames = df.where("Arrest=='True'").groupBy("Year_T").count().orderBy("Year_T")
    pandasDF = frames.toPandas()
    return pandasDF.to_json()

@app.route('/trends_in_crime', methods=['GET'])
def trends_in_crime():
    initialize_spark_session()
    frames = df.groupBy("PrimaryType").count().orderBy("count")
    pandasDF = frames.toPandas()
    return pandasDF.to_json()

@app.route('/locations_in_crime', methods=['GET'])
def locations_in_crime():
    initialize_spark_session()
    frames = df.groupBy("LocationDescription").count().orderBy("count")
    pandasDF = frames.toPandas()
    return pandasDF.to_json()

@app.route('/high_crime_locations', methods=['GET'])
def high_crime_locations():
    initialize_spark_session()
    frames = df.groupBy("PrimaryType", "LocationDescription").count().sort("PrimaryType", "LocationDescription")
    pandasDF = frames.toPandas()
    return pandasDF.to_json()

app.run(host='0.0.0.0')