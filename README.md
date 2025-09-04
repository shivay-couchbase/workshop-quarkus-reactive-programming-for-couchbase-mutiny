#  Error Handling and Retry Strategies

## Overview

This exercise demonstrates advanced error handling patterns and retry strategies in reactive programming with Quarkus and Couchbase. You'll learn how to build resilient applications that can gracefully handle failures and automatically recover from transient errors.

## Learning Objectives

By the end of this exercise, you will understand:

- **Resilient Data Access**: How to implement fallback mechanisms for database operations
- **Retry Strategies**: Different approaches to retry failed operations
- **Error Recovery**: How to gracefully handle and recover from various types of failures
- **Circuit Breaker Patterns**: Implementing timeout and retry logic
- **Exception Handling**: Proper error categorization and response formatting

## Prerequisites

- Quarkus application running with Couchbase connection
- Basic understanding of reactive programming with Mutiny
- Familiarity with Couchbase operations
- Couchbase Image - Docker / Podman
```
docker run -d --name couchbase-server -p 8091-8096:8091-8096 -p 11207:11207 -p 11210:11210 -p 18091-18096:18091-18096 couchbase/server:latest

```

## Error Handling Methods

### 1. Resilient User Retrieval (`getUserResilient`)

**Endpoint**: `GET /couchbase/users/{userId}/resilient`

This method demonstrates a comprehensive error handling strategy with multiple fallback mechanisms:

#### Features:
- **Primary Operation**: Attempts to get user from primary replica
- **Fallback to Replica**: If primary fails, automatically tries replica
- **Retry Logic**: Retries up to 5 times on timeout errors
- **Graceful Degradation**: Returns structured error responses
- **Timeout Handling**: Specific handling for timeout scenarios

#### Error Handling Flow:
1. Try to get user from primary replica
2. If `DocumentNotFoundException` occurs, fallback to any replica
3. If `TimeoutException` occurs, retry up to 5 times
4. If all retries fail, return timeout error response
5. For any other errors, return generic error response

### 2. Service Reliability Testing

#### Bad Service Test (`badServiceTest`)
**Endpoint**: `GET /couchbase/service/bad`

Demonstrates an unreliable service that fails 90% of the time.

#### Good Service Test (`goodServiceTest`)
**Endpoint**: `GET /couchbase/service/good`

Shows how to make the unreliable service more resilient with retry logic.

## How to Run and Test

### 1. Start the Application

```bash
./mvnw quarkus:dev
```

Wait for the application to start and Couchbase to be ready.

### 2. Test Resilient User Retrieval

#### Test with Existing User
```bash
# First, create a user to test with
curl -X POST http://localhost:8080/couchbase/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Test User", "email": "test@example.com", "role": "developer"}'

# Test resilient retrieval
curl -s http://localhost:8080/couchbase/users/user-1756983274324/resilient | jq
```

**Expected Success Response:**
```json
{
  "success": true,
  "user": {
    "name": "Test User",
    "email": "test@example.com",
    "role": "developer",
    "id": "user-1234567890",
    "createdAt": 1234567890123,
    "updatedAt": 1234567890123
  },
  "cas": 1234567890123456789,
  "timestamp": 1234567890123
}
```

#### Test with Non-existent User
```bash
curl -s http://localhost:8080/couchbase/users/non-existent-user/resilient | jq
```

**Expected Error Response:**
```json
{
  "success": false,
  "error": "Internal server error",
  "errorCode": "INTERNAL_ERROR",
  "userId": "non-existent-user",
  "timestamp": 1234567890123
}
```

### 3. Test Service Reliability

#### Test Unreliable Service (Bad Service)
```bash
# Run multiple times to see failures
for i in {1..10}; do
  echo "Attempt $i:"
  curl -s http://localhost:8080/couchbase/service/bad
  echo ""
  sleep 1
done
```

**Expected Behavior:**
- Most requests will fail with "Unhappy Service" exception
- Occasionally (10% chance) you'll see "it worked"

#### Test Reliable Service (Good Service)
```bash
# Run multiple times to see retry behavior
for i in {1..10}; do
  echo "Attempt $i:"
  curl -s http://localhost:8080/couchbase/service/good
  echo ""
  sleep 1
done
```

**Expected Behavior:**
- Service will retry on failures
- Eventually should succeed (though may take multiple attempts)
- Check application logs to see retry attempts

### 4. Monitor Application Logs

Watch the application logs to see error handling in action:

```bash
# In another terminal, watch the logs
tail -f target/quarkus.log
```

Look for:
- Retry attempts
- Error logging
- Fallback operations
- Timeout handling

## Advanced Testing Scenarios

### 1. Simulate Network Issues

To test timeout handling, you can temporarily modify the Couchbase connection settings to simulate network issues:

```properties
# In application.properties
quarkus.couchbase.connection-timeout=1s
quarkus.couchbase.kv-timeout=1s
```

### 2. Test with High Load

```bash
# Test resilient endpoint under load
for i in {1..50}; do
  curl -s http://localhost:8080/couchbase/users/user-1234567890/resilient &
done
wait
```

### 3. Test Error Recovery

```bash
# Test both good and bad services simultaneously
for i in {1..20}; do
  echo "Bad service attempt $i:"
  curl -s http://localhost:8080/couchbase/service/bad &
  echo "Good service attempt $i:"
  curl -s http://localhost:8080/couchbase/service/good &
  echo ""
  sleep 0.5
done
```

## Key Concepts Demonstrated

### 1. Reactive Error Handling Patterns

```java
// Fallback pattern
.onFailure(DocumentNotFoundException.class)
.recoverWithItem(fallbackOperation)

// Retry pattern
.onFailure(TimeoutException.class)
.retry().atMost(5)

// Error transformation
.onFailure().recoverWithItem(err -> 
    JsonObject.create()
        .put("success", false)
        .put("error", err.getMessage())
)
```

### 2. Exception Classification

- **`DocumentNotFoundException`**: Expected business logic error
- **`TimeoutException`**: Transient network/performance issue
- **`UnhappyServiceException`**: Custom application error
- **Generic exceptions**: Unexpected system errors

### 3. Retry Strategies

- **Fixed Retry**: Retry a fixed number of times
- **Conditional Retry**: Retry based on specific conditions
- **Exponential Backoff**: (Can be implemented with delays)

### 4. Circuit Breaker Pattern

The resilient user retrieval implements a simple circuit breaker:
1. Try primary operation
2. Fallback to secondary operation
3. Retry on transient failures
4. Fail gracefully with structured error response

## Best Practices

### 1. Error Response Structure

Always return consistent error response structures:

```json
{
  "success": false,
  "error": "Human-readable error message",
  "errorCode": "MACHINE_READABLE_CODE",
  "timestamp": 1234567890123,
  "additionalContext": "Any relevant context"
}
```

### 2. Logging Strategy

- Log errors with appropriate levels
- Include correlation IDs for tracing
- Don't log sensitive information
- Use structured logging

### 3. Retry Considerations

- **Idempotency**: Ensure operations can be safely retried
- **Backoff**: Consider exponential backoff for retries
- **Circuit Breaker**: Implement circuit breaker for external services
- **Monitoring**: Monitor retry rates and failure patterns

### 4. Timeout Configuration

Configure appropriate timeouts for different operations:

```properties
# Couchbase timeouts
quarkus.couchbase.connection-timeout=10s
quarkus.couchbase.kv-timeout=2.5s
quarkus.couchbase.query-timeout=75s

# HTTP timeouts
quarkus.http.read-timeout=30s
quarkus.http.write-timeout=30s
```

## Troubleshooting

### Common Issues

1. **Application won't start**
   - Check Couchbase connection
   - Verify all dependencies are available

2. **Tests always fail**
   - Ensure Couchbase is running
   - Check network connectivity
   - Verify user data exists

3. **Retry not working**
   - Check exception types match
   - Verify retry conditions
   - Monitor application logs

### Debug Commands

```bash
# Check application health
curl -s http://localhost:8080/q/health | jq

# Check Couchbase connection
curl -s http://localhost:8080/couchbase/health

# View application metrics
curl -s http://localhost:8080/q/metrics | grep -i error
```

## Next Steps

After completing this exercise, consider:

1. **Implementing Circuit Breaker**: Add circuit breaker pattern for external services
2. **Adding Metrics**: Implement retry and error metrics
3. **Custom Retry Policies**: Create more sophisticated retry strategies
4. **Bulk Operations**: Apply error handling to batch operations
5. **Monitoring Integration**: Add monitoring and alerting for error patterns

## Additional Resources

- [Quarkus Reactive Programming Guide](https://quarkus.io/guides/reactive)
- [SmallRye Mutiny Documentation](https://smallrye.io/smallrye-mutiny/)
- [Couchbase Java SDK Error Handling](https://docs.couchbase.com/java-sdk/current/howtos/error-handling.html)
- [Reactive Streams Specification](https://www.reactive-streams.org/)
