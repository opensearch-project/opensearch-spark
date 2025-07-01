from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import time

def try_get_spark_session():
    max_attempts = 3
    for attempt in range(1, max_attempts+1):
        try:
            print(f"Attempting to create Spark session (attempt {attempt} / {max_attempts})")
            return SparkSession.builder \
                       .master("spark://spark:7077") \
                       .appName("API Server") \
                       .getOrCreate()
        except:
            time.sleep(2)

app = Flask(__name__)
spark = try_get_spark_session()


@app.errorhandler(Exception)
def handle_exception(e: Exception):
    return jsonify({
      "type": type(e).__name__,
      "status": "failure",
      "message": str(e).strip('\n').splitlines()
    }), 400

@app.route("/query", methods=["POST"])
def execute_query():
    sql = request.json["query"]
    df = spark.sql(sql)

    schema = df.schema
    column_types = { field.name: field.dataType.simpleString() for field in schema.fields }

    result = df.toPandas().to_dict("records")
    return jsonify({"status": "success", "result": result, "schema": column_types })

@app.route("/load", methods=["POST"])
def load_data():
    data = request.json["data"]
    table = request.json["table"]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView(table)
    return {"status": "success"}

@app.route("/drop", methods=["POST"])
def drop_table():
    table = request.json["table"]
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    return {"status": "success"}
