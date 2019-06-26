from chalice import Chalice, Response
import json

with open('./model.json', 'r') as f:
    model = json.load(f)

# import pandas as pd
# import joblib
# from ml import TimeTransformer
# clf = joblib.load(path_to_model)

# cols = ['complaint_type', 'latitude', 'longitude', 'created_date']
# obj = pd.DataFrame(columns=cols, index=[0])

app = Chalice(app_name='311prediction')



@app.route('/predict/{complaint_type}', methods=['GET'])
def predict(complaint_type:str) -> Response:
    
    if complaint_type in model:
        return Response(status_code=200,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'success',
                              'complaint_type': complaint_type,
                              'estimated_time': model[complaint_type]})
    else:
        return Response(status_code=400,
                        headers={'Content-Type': 'application/json'},
                        body={'status': 'failure',
                              'complaint_type': complaint_type,
                              'estimated_time': model['overal_median']})



# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
