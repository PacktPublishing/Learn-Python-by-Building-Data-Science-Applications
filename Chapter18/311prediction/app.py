from chalice import Chalice, Response

# import pandas as pd
# import joblib
# from ml import TimeTransformer
# clf = joblib.load(path_to_model)

# cols = ['complaint_type', 'latitude', 'longitude', 'created_date']
# obj = pd.DataFrame(columns=cols, index=[0])

app = Chalice(app_name='311prediction')



@app.route('/predict/{complaint_type}', methods=['GET'])
def predict(complaint_type) -> Response:
    return Response(status_code=200,
                    headers={'Content-Type': 'application/json'},
                    body={'status': 'success',
                          'complaint_type': complaint_type,
                          'estimated_time': 38})
    # try:
    #     request_body = app.current_request.json_body
    #     assert all(col in request_body for col in cols)

    #     for col in cols:
    #         obj.loc[0, col] = request_body.get(col)
        
    #     predicted = clf.predict(obj)
        
    #     return Response(status_code=200,
    #                     headers={'Content-Type': 'application/json'},
    #                     body={'status': 'success',
    #                           'estimated_time': predicted[0]})
    # except Exception as e:
    #     return Response(status_code=400,
    #                     headers={'Content-Type': 'application/json'},
    #                     body={'status': 'failure',
    #                           'message': e.msg}
    #                     ),

        
                                  


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
