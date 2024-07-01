import json
from utils_shortener import get_long_url


def lambda_handler(event, context):
    short_url = event['pathParameters']['short_url']
    long_url = get_long_url(short_url)
    if long_url:
        return {
            'statusCode': 301,
            'headers': {'Location': long_url}
        }
    else:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'URL not found'})
        }
