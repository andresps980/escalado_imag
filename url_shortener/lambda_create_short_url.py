import json
from utils_shortener import create_short_url


def lambda_handler(event, context):
    long_url = event['queryStringParameters']['long_url']
    base_url = "https://yourdomain.com"  # Replace with your actual domain
    short_url = create_short_url(long_url, base_url)
    return {
        'statusCode': 200,
        'body': json.dumps({'short_url': short_url})
    }
