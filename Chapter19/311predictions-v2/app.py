import pickle

import boto3
import numpy as np
from chalice import Chalice, Response

from ml import TimeTransformer
BUCKET, KEY = 'philipp-packt', 'model.pkl'


def _load_pickle(bucket, key):
    S3 = boto3.client('s3', region_name='us-east-1')
    response = S3.get_object(Bucket=bucket, Key=key)
    
    body = response['Body'].read()
    return pickle.loads(body)

model = _load_pickle(BUCKET, KEY)

app = Chalice(app_name='311predictions-v2')
singleton = np.empty(shape=(1, 4), dtype='object')

dtypes = {
    'lon': float,
    'lat': float,
    'date': np.datetime64
}

@app.route('/predict/{complaint_type}', methods=['GET'])
def index(complaint_type:str):
    app.log.debug(app.current_request.query_params)
    singleton[0, 0] = complaint_type

    for i, col in enumerate(dtypes.keys(), 1):
        singleton[0, i] = dtypes[col](app.current_request.query_params.get(col, np.nan))

    app.log.debug(singleton.astype(str).tolist())
    try:
        prediction = model.predict(singleton)[0]
        app.log.debug(prediction)
        return Response(status_code=200,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'success',
                              'estimated_time': prediction})
    except Exception as e:
        return Response(status_code=400,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'failure',
                              'error message': str(e)})
