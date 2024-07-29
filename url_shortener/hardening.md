Securing a call to an open API is crucial to protect sensitive data and ensure that only authorized users and applications have access. Here are some best practices to secure API calls:

### 1. **Use HTTPS**
- Always use HTTPS to encrypt data in transit. This prevents eavesdropping and man-in-the-middle attacks.

### 2. **API Key and Secrets**
- **API Keys**: Provide unique identifiers for each client accessing the API. While API keys offer a basic level of security, they should be used in combination with other security measures.
- **Client Secrets**: Use client secrets in addition to API keys for server-to-server communication.

### 3. **OAuth 2.0**
- Implement OAuth 2.0 for secure token-based authentication. OAuth 2.0 allows applications to obtain limited access to user accounts on an HTTP service.

### 4. **Rate Limiting and Throttling**
- Limit the number of API requests from a single client to prevent abuse and ensure fair use of resources. Implement throttling to control the rate at which requests are processed.

### 5. **IP Whitelisting**
- Restrict access to the API from known IP addresses. This adds an additional layer of security by ensuring that only requests from trusted IP addresses are allowed.

### 6. **Input Validation**
- Validate all inputs to the API to prevent injection attacks, such as SQL injection or cross-site scripting (XSS).

### 7. **Logging and Monitoring**
- Implement logging and monitoring to detect unusual activities and potential security breaches. Monitor API usage patterns to identify and respond to potential threats quickly.

### 8. **CORS (Cross-Origin Resource Sharing)**
- Configure CORS to control which domains can access your API. This helps prevent unauthorized access from malicious websites.

### 9. **Authentication and Authorization**
- Use strong authentication and authorization mechanisms to ensure that only authenticated and authorized users can access certain API endpoints. Role-based access control (RBAC) is often used for this purpose.

### 10. **Secure Endpoints**
- Secure sensitive API endpoints by requiring additional authentication steps or by limiting access based on user roles and permissions.

### 11. **Data Encryption**
- Encrypt sensitive data both at rest and in transit to protect it from unauthorized access.

### 12. **API Gateway**
- Use an API gateway to enforce security policies, such as rate limiting, throttling, and authentication, and to provide a unified interface for managing API calls.

### 13. **Regular Security Audits and Penetration Testing**
- Conduct regular security audits and penetration testing to identify and fix vulnerabilities in your API.

### 14. **Use JSON Web Tokens (JWT)**
- Use JWT for secure token-based authentication. Ensure that tokens are signed and optionally encrypted.

### Implementation Example: Securing an API Call Using OAuth 2.0

Below is an example of how you might secure an API call using OAuth 2.0 in a Node.js environment:

#### Step 1: Obtain an Access Token

```javascript
const axios = require('axios');

async function getAccessToken() {
  const response = await axios.post('https://auth.example.com/oauth/token', {
    client_id: 'YOUR_CLIENT_ID',
    client_secret: 'YOUR_CLIENT_SECRET',
    grant_type: 'client_credentials'
  });

  return response.data.access_token;
}
```

#### Step 2: Make an API Call with the Access Token

```javascript
async function callSecureApi() {
  const accessToken = await getAccessToken();

  const response = await axios.get('https://api.example.com/secure-endpoint', {
    headers: {
      Authorization: `Bearer ${accessToken}`
    }
  });

  console.log(response.data);
}

callSecureApi();
```

In this example, the access token is obtained from the OAuth 2.0 authorization server and then used to make a secure API call.

By implementing these best practices, you can significantly enhance the security of your API calls and protect your application from various security threats.