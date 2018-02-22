"""Filename: server.py
"""
import os
from flask import Flask, jsonify, request, abort
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
 
#os.system('aws s3 cp s3://lusparkpoc/subscriptionModel ./subscriptionModel --recursive')

# Initiate Spark session
spark = SparkSession.builder.appName('subscriptionModel1').getOrCreate()
 
app = Flask('subscriptionModel')
 
@app.route('/lu_wang/subscriptionModel/', methods=['POST'])
def apicall():
    """Spark dataframe (sent as a payload) from API Call
    """
    try:
        test_json = [request.get_json()]
        test = spark.createDataFrame(test_json)
        cookie_id = test.select('cookie_id').collect()[0][0]
        print(cookie_id)
 
    except Exception:
        abort(400, 'Nah Nah Nah Nah Nah, this is a bad request')
 
    print("Loading the model...")
    loaded_model = PipelineModel.load('./subscriptionModel')
 
    print("The model has been loaded...doing predictions now...")
    prediction = loaded_model.transform(test).select('prediction').collect()[0][0]
    responses = jsonify({'sub_flg':prediction, 'cookie_id':cookie_id})
    responses.status_code = 200
    return responses
 
app.run(debug=True, port=4042)