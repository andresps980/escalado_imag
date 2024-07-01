import boto3
import string
import random

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UrlShortener')


def generate_short_url():
    """Generate a random short URL string."""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(6))


def create_short_url(long_url, base_url):
    """Create a short URL for the given long URL and save it to DynamoDB."""
    short_url = generate_short_url()

    # Ensure the short_url is unique
    while 'Item' in table.get_item(Key={'short_url': short_url}):
        short_url = generate_short_url()

    # Save the URL mapping to DynamoDB
    table.put_item(Item={'short_url': short_url, 'long_url': long_url})

    full_short_url = f"{base_url}/{short_url}"
    return full_short_url


def get_long_url(short_url):
    """Retrieve the long URL for the given short URL from DynamoDB."""
    response = table.get_item(Key={'short_url': short_url})

    if 'Item' in response:
        return response['Item']['long_url']
    else:
        return None
