# Adding Reactive Programming with Mutiny - CRUD operations 

## Overview
In this exercise, you'll learn how to connect to a Couchbase cluster using reactive programming patterns with Quarkus and Mutiny. You'll understand how to use the Couchbase Async SDK with `Uni.createFrom().completionStage()` to build non-blocking, scalable database applications.

## Learning Objectives
- Understand reactive programming concepts in the context of database operations
- Learn how to use Mutiny with the Couchbase Async SDK
- Implement reactive endpoints that return Mutiny types (Uni and Multi)
- Handle database connectivity and health checks reactively
- Practice converting synchronous operations to reactive ones using `Uni.createFrom().completionStage()`

## Prerequisites
- Exercise 1 completed (Quarkus project setup with Couchbase support)
- Basic understanding of reactive programming concepts
- Couchbase cluster running and accessible

## What You'll Build
A reactive REST API that demonstrates:
- Health check endpoint for cluster connectivity using async ping
- Reactive document upsert operations with proper error handling
- Cluster information retrieval using async diagnostics
- Document CRUD operations with reactive streams
- Batch operations and N1QL queries using async APIs

## Step-by-Step Implementation

### Step 1: Understanding the Current Code
The existing `GreetingResource.java` has been updated to use reactive patterns with Mutiny. Key changes:

1. **Async SDK**: Using `cluster.async()`, `bucket.async()`, and `collection.async()`
2. **Mutiny Integration**: All methods return `Uni<T>` or `Multi<T>` using `Uni.createFrom().completionStage()`
3. **Enhanced Endpoints**: Added health check, cluster info, and reactive CRUD endpoints

### Step 2: Key Reactive Concepts Demonstrated

#### Using Async SDK with CompletionStage
```java
@Inject
Cluster cluster;

public Uni<String> healthCheck() {
    return Uni.createFrom().completionStage(cluster.async().ping())
            .onItem().transform(result -> "Couchbase cluster is healthy!")
            .onFailure().recoverWithItem(t -> "Cluster health check failed: " + t.getMessage());
}
```

This pattern:
- Uses the existing `Cluster` instance
- Calls `.async()` to get async wrappers that return `CompletionStage`
- Uses `Uni.createFrom().completionStage()` to convert to Mutiny `Uni`
- Maintains the same Couchbase Java SDK functionality
- Adds reactive stream capabilities with Mutiny

#### Async Document Operations
```java
public Uni<String> reactiveUpsert(String documentJson) {
    AsyncBucket bucket = cluster.bucket("default").async();
    AsyncCollection collection = bucket.defaultCollection();
    String key = "reactive-doc-" + System.currentTimeMillis();
    
    JsonObject content = JsonObject.fromJson(documentJson)
            .put("createdAt", System.currentTimeMillis())
            .put("documentId", key);

    return Uni.createFrom().completionStage(collection.upsert(key, content))
            .onItem().transform(result -> "Document upserted successfully - Key: " + key + ", Mutation token: " + result.mutationToken() + ", CAS: " + result.cas())
            .onFailure().recoverWithItem(error -> "Document upsert failed: " + error.getMessage());
}
```

### Step 3: Testing the Reactive Endpoints

#### 1. Health Check
```bash
curl http://localhost:8080/couchbase/health
```
Expected response: `"Couchbase cluster is healthy!"`

#### 2. Simple Upsert
```bash
curl http://localhost:8080/couchbase/simpleUpsert
```
Expected response: Document upsert confirmation with mutation token

#### 3. Reactive Upsert
```bash
curl -X POST http://localhost:8080/couchbase/reactiveUpsert \
  -H "Content-Type: application/json" \
  -d '{"author": "John Doe", "title": "Reactive Programming Guide", "content": "Learn reactive patterns"}'
```
Expected response: JSON document with added metadata and mutation token

#### 4. Cluster Information
```bash
curl http://localhost:8080/couchbase/clusterInfo
```
Expected response: JSON with cluster details (name, node count, version, timestamp)

#### 5. Document Retrieval
```bash
curl http://localhost:8080/couchbase/document/{key}
```
Expected response: JSON document or 404 if not found

#### 6. Document Deletion
```bash
curl -X DELETE http://localhost:8080/couchbase/document/{key}
```
Expected response: 204 No Content on success

#### 7. Batch Operations
```bash
curl -X POST http://localhost:8080/couchbase/batchUpsert \
  -H "Content-Type: application/json" \
  -d '[{"name": "Doc 1"}, {"name": "Doc 2"}]'
```
Expected response: Array of documents with mutation tokens

#### 8. N1QL Query
```bash
curl -X POST http://localhost:8080/couchbase/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM `default` LIMIT 5", "parameters": {}}'
```
Expected response: Query results as JSON array

### Step 4: Understanding Reactive Benefits

#### Non-Blocking Operations
- Traditional approach: Thread blocks waiting for database response
- Reactive approach: Thread is freed up while waiting for database response
- Better resource utilization and scalability

#### Backpressure Handling
- Reactive streams can handle varying load gracefully
- Clients can control the rate of data consumption
- Built-in flow control mechanisms

#### Error Handling
- Errors are propagated through the reactive stream
- Graceful degradation and recovery
- Consistent error handling patterns

### Step 5: Advanced Reactive Patterns

#### Combining Multiple Operations
```java
public Uni<String> demonstrateReactivePatterns() {
    AsyncBucket bucket = cluster.bucket("default").async();
    AsyncCollection collection = bucket.defaultCollection();
    
    return Uni.createFrom().completionStage(cluster.async().diagnostics())
            .onItem().transformToUni(info -> {
                JsonObject demoDoc = JsonObject.create()
                        .put("type", "demo")
                        .put("clusterId", info.id())
                        .put("createdAt", System.currentTimeMillis());
                
                return Uni.createFrom().completionStage(collection.upsert("demo-doc", demoDoc))
                        .onItem().transform(result -> "Reactive pattern demonstration completed - Mutation token: " + result.mutationToken());
            })
            .onFailure().recoverWithItem(t -> "Demo operation failed: " + t.getMessage());
}
```

#### Batch Processing with Uni.combine().all()
```java
public Uni<String> batchUpsert(String documentsJson) {
    try {
        JsonArray documentsArray = JsonArray.fromJson(documentsJson);
        List<JsonObject> documents = new ArrayList<>();
        
        for (int i = 0; i < documentsArray.size(); i++) {
            documents.add(documentsArray.getObject(i));
        }
        
        AsyncBucket bucket = cluster.bucket("default").async();
        AsyncCollection collection = bucket.defaultCollection();
        
        List<Uni<JsonObject>> operations = new ArrayList<>();
        
        for (int i = 0; i < documents.size(); i++) {
            JsonObject doc = documents.get(i);
            String key = "batch-" + System.currentTimeMillis() + "-" + i;
            doc.put("batchId", i);
            doc.put("createdAt", System.currentTimeMillis());
            
            Uni<JsonObject> operation = Uni.createFrom().completionStage(collection.upsert(key, doc))
                    .onItem().transform(result -> {
                        doc.put("mutationToken", result.mutationToken().toString());
                        doc.put("cas", result.cas());
                        doc.put("key", key);
                        return doc;
                    })
                    .onFailure().recoverWithItem(t -> JsonObject.create()
                        .put("error", "Upsert failed: " + t.getMessage())
                        .put("key", key));
            
            operations.add(operation);
        }
        
        return Uni.join().all(operations).andCollectFailures()
            .onItem().transform(objects -> {
                return "Batch upsert completed successfully - Total documents: " + documents.size() + ", Successful upserts: " + objects.size();
            })
            .onFailure().recoverWithItem(error -> "Batch upsert failed: " + error.getMessage());
        
    } catch (Exception e) {
        return Uni.createFrom().item("Failed to parse input: " + e.getMessage());
    }
}
```

## Key Differences from Reactive SDK Approach

### API Changes
- **Reactive SDK**: `cluster.reactive()`, `bucket.reactive()`, `collection.reactive()`
- **Async SDK**: `cluster.async()`, `bucket.async()`, `collection.async()`

### Return Types
- **Reactive SDK**: Returns `Mono<T>` and `Flux<T>` (Project Reactor types)
- **Async SDK**: Returns `CompletionStage<T>` (Java standard async types)

### Conversion Pattern
- **Old Approach**: `FromMono.INSTANCE.from(reactiveCluster.ping())`
- **New Approach**: `Uni.createFrom().completionStage(cluster.async().ping())`

### Error Handling
- **onErrorResume()** → **onFailure().recoverWithItem()**
- **onErrorReturn()** → **onFailure().recoverWithItem()**

### Combining Operations
- **Mono.zip()** → **Uni.join().all().andCollectFailures()**
- **Flux.collectList()** → **Multi.collect().asList()**

### Retry Mechanisms
- **retry(n)** → **onFailure().retry().atMost(n)**

## Common Issues and Solutions

### Issue 1: Connection Timeout
**Problem**: `ConnectionTimeoutException` when cluster is unreachable
**Solution**: Ensure Couchbase cluster is running and connection string is correct

### Issue 2: Authentication Failure
**Problem**: `AuthenticationException` when credentials are wrong
**Solution**: Verify username/password in `application.properties`

### Issue 3: Bucket Not Found
**Problem**: `BucketNotFoundException` when accessing non-existent bucket
**Solution**: Create the bucket in Couchbase or update the configuration

### Issue 4: Async SDK Issues
**Problem**: `NoSuchMethodError` when calling `.async()` or using `Uni.createFrom().completionStage()`
**Solution**: Ensure Couchbase Java SDK dependency is properly added to pom.xml and use the correct async API

## Best Practices

1. **Always Use Async SDK**: Call `.async()` on cluster, bucket, and collection instances
2. **Use CompletionStage Pattern**: Use `Uni.createFrom().completionStage()` to convert async operations to Mutiny
3. **Handle Errors Gracefully**: Use `onFailure().recoverWithItem()` for fallback behavior
4. **Log Operations**: Include logging for debugging and monitoring
5. **Validate Input**: Check document structure before database operations
6. **Use Meaningful Keys**: Generate unique, descriptive document keys
7. **Combine Operations**: Use `transformToUni()` and `Uni.join().all()` for complex workflows
8. **Implement Retry Logic**: Use `onFailure().retry().atMost(n)` for transient failures

## Next Steps
After completing this exercise, you'll be ready for:
- Exercise 3: Implementing advanced CRUD operations and data modeling
- Exercise 4: Performing complex reactive N1QL queries
- Exercise 5: Advanced error handling and retry strategies

## Additional Resources
- [Couchbase Java SDK Documentation](https://docs.couchbase.com/java-sdk/current/)
- [Couchbase Async API Documentation](https://docs.couchbase.com/java-sdk/current/concept-docs/async-programming.html)
- [Mutiny Documentation](https://smallrye.io/smallrye-mutiny/)
- [Quarkus Reactive Guide](https://quarkus.io/guides/reactive)

## Challenge Exercise
Try implementing a new endpoint that:
- Retrieves multiple documents by key pattern using async operations
- Combines results from multiple reactive operations using `Uni.join().all()`
- Implements timeout handling with `ifNoItem().after(Duration.ofSeconds(5))`
- Uses `onFailure().retry().atMost(3)` for transient failures

## Summary
In this exercise, you've learned:
- How to use the Couchbase Async SDK with `Uni.createFrom().completionStage()`
- The benefits of reactive programming for database operations
- How to implement reactive REST endpoints with Quarkus and Mutiny
- Best practices for reactive database connectivity using async APIs

The reactive approach provides better scalability, resource utilization, and error handling compared to traditional synchronous database access patterns, while maintaining the familiar Couchbase Java SDK API. The use of `Uni.createFrom().completionStage()` provides better integration with Quarkus and more intuitive error handling patterns compared to Project Reactor.


