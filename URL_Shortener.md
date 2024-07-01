
To create a URL shortener service that provides a shortened URL and serves it to the end user, you can integrate the whole process using AWS Lambda, API Gateway, and DynamoDB. The idea is to have two Lambda functions: one to create and return the short URL, and another to redirect users to the long URL when they visit the short URL.

### Step-by-Step Implementation

1. **Setup AWS DynamoDB**:
   - Create a DynamoDB table named `UrlShortener` with a primary key `short_url` (String).

2. **Install AWS SDK**:
   - Install the `boto3` library if you haven't already:
     ```bash
     pip install boto3
     ```

### Python Code

#### Create Short URL Function

This function generates a short URL, saves the mapping to DynamoDB, and returns the full shortened URL.

```python
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
```

#### Retrieve and Redirect URL Function

This function retrieves the original long URL from DynamoDB given the short URL.

```python
def get_long_url(short_url):
    """Retrieve the long URL for the given short URL from DynamoDB."""
    response = table.get_item(Key={'short_url': short_url})

    if 'Item' in response:
        return response['Item']['long_url']
    else:
        return None
```

### API Gateway and Lambda Integration

1. **Create Lambda Functions**:
   - Create two Lambda functions: one for creating short URLs (`CreateShortUrlLambda`) and another for redirecting to long URLs (`RedirectUrlLambda`).

2. **Configure IAM Roles**:
   - Ensure your Lambda functions have permissions to access DynamoDB. Attach the `AmazonDynamoDBFullAccess` policy to the Lambda execution role for simplicity (in production, use the principle of least privilege).

3. **Set Up API Gateway**:
   - Create an API Gateway to trigger these Lambda functions via HTTP requests.

### Lambda Handlers

#### Create Short URL Lambda Handler

```python
import json

def lambda_handler(event, context):
    long_url = event['queryStringParameters']['long_url']
    base_url = "https://yourdomain.com"  # Replace with your actual domain
    short_url = create_short_url(long_url, base_url)
    return {
        'statusCode': 200,
        'body': json.dumps({'short_url': short_url})
    }
```

#### Redirect URL Lambda Handler

```python
import json

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
```

### Setting Up API Gateway

1. **Create API**:
   - Create a new API in API Gateway.

2. **Create Resources and Methods**:
   - For the `CreateShortUrlLambda` function:
     - Create a resource (e.g., `/create`) and a GET method.
     - Set the integration type to Lambda Function and select `CreateShortUrlLambda`.

   - For the `RedirectUrlLambda` function:
     - Create a resource with a path parameter (e.g., `/{short_url}`).
     - Set the integration type to Lambda Function and select `RedirectUrlLambda`.

3. **Deploy API**:
   - Deploy the API to a stage (e.g., `prod`).

### Testing the Service

- **Create Short URL**:
  - Make a GET request to the API Gateway endpoint for the `CreateShortUrlLambda` function with the long URL as a query parameter:
    ```
    GET https://your-api-id.execute-api.region.amazonaws.com/prod/create?long_url=https://www.example.com
    ```
  - The response will contain the short URL.

- **Redirect to Long URL**:
  - Access the short URL in a browser. The `RedirectUrlLambda` function will handle the request and redirect to the original long URL.

This setup provides a full URL shortening service that creates and serves shortened URLs using AWS Lambda, API Gateway, and DynamoDB.


