import json
from utils_shortener import create_short_url


def lambda_handler(event, context):
    long_url = event['queryStringParameters']['long_url']
    # TODO Dominio actual en codigo de AWS desplegado:
    # https://g0jzs4d0ci.execute-api.eu-west-1.amazonaws.com
    # https://g0jzs4d0ci.execute-api.eu-west-1.amazonaws.com/development
    # TODO Buscar una forma de que el codigo en prod lo corga de variable de entorno del API
    base_url = "https://yourdomain.com"  # Replace with your actual domain
    short_url = create_short_url(long_url, base_url)
    return {
        'statusCode': 200,
        'body': json.dumps({'short_url': short_url})
    }
