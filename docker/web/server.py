# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time
from datetime import datetime

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

def extract_spark_schema(schema):
    struct_fields = []
    for col_name, col_type in schema.items():
        type_mapping = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType()
        }
        spark_type = type_mapping.get(col_type.lower(), StringType())
        struct_fields.append(StructField(col_name, spark_type, True))

    return StructType(struct_fields)

def map_complex_types(schema, data):
    processed_data = []
    for row in data:
        processed_row = {}
        for col_name, value in row.items():
            col_type = schema.get(col_name, "string").lower()
            if value is not None:
                if col_type == "date":
                    value = datetime.strptime(value, "%Y-%m-%d").date()
                elif col_type == "timestamp":
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            processed_row[col_name] = value
        processed_data.append(processed_row)
    return processed_data

@app.route("/load", methods=["POST"])
def load_data():
    data = request.json["data"]
    table = request.json["table"]
    schema = request.json.get("schema") # Optional schema

    if schema:
        spark_schema = extract_spark_schema(schema)
        data = map_complex_types(schema, data)
        df = spark.createDataFrame(data, schema=spark_schema)
    else:
        df = spark.createDataFrame(data)

    df.createOrReplaceTempView(table)
    return {"status": "success"}

@app.route("/drop", methods=["POST"])
def drop_table():
    table = request.json["table"]
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    return {"status": "success"}
