import boto3
import os
import pickle
import json
import pandas as pd

s3 = boto3.client('s3')

MODEL_BUCKET = os.environ['MODEL_BUCKET']

obj = s3.get_object(Bucket=MODEL_BUCKET, Key='model/bundle.pkl')
bundle = pickle.loads(obj['Body'].read())
model = bundle['model']
feature_columns = bundle['feature_columns']


def encode_features(df):
    categorical_cols = df.select_dtypes(include='object').columns.tolist()
    if categorical_cols:
        df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
    return df


def lambda_handler(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event.get('body', event)

        input_df = pd.DataFrame([body])

        input_df = encode_features(input_df)

        input_df = input_df.reindex(columns=feature_columns, fill_value=0)

        prediction = model.predict(input_df)[0]

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'predicted_product_wg_ton': float(prediction)})
        }

    except Exception as e:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }