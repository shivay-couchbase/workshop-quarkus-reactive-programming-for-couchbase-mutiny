# Exercise 3: Implementing CRUD Operations as Reactive Endpoints (Mutiny)

## **Learning Objectives**

By the end of this exercise, you will be able to:
- Implement complete CRUD operations using reactive programming patterns with `FromMono.INSTANCE.from()`
- Use Mutiny types (`Uni` and `Multi`) for database operations
- Handle different data types and document structures
- Implement proper error handling for CRUD operations
- Use reactive operators for data transformation and validation
- Convert between Project Reactor types and Mutiny types

##  **Implementation Steps**

### **Step 1: Create a Document Model**

First, let's create a proper document model for our CRUD operations:

```java
public class UserDocument {
    private String id;
    private String name;
    private String email;
    private String role;
    private long createdAt;
    private long updatedAt;
    
    // Constructors, getters, setters
}
```

### **Step 2: Implement Create Operations**

```java
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.TEXT_PLAIN)
@Path("users")
public Uni<JsonObject> createUser(String userJson) {
    ReactiveBucket bucket = cluster.bucket("default").reactive();
    ReactiveCollection collection = bucket.defaultCollection();
    String userId = "user-" + System.currentTimeMillis();
    
    Uni<JsonObject> uniUser = Uni.createFrom().item(() -> {
        JsonObject user = JsonObject.fromJson(userJson);
        // Add metadata
        user.put("id", userId);
        user.put("createdAt", System.currentTimeMillis());
        user.put("updatedAt", System.currentTimeMillis());
        return user;
    }).onFailure().invoke(err -> LOG.error("There was an error creating User from JSON: " + err.getMessage()));
    
    Uni<MutationResult> result = uniUser.flatMap(u -> FromMono.INSTANCE.from(collection.insert(userId, u)));
    
    return result.map(mr -> JsonObject.create()
            .put("success", true)
            .put("message", "User created successfully")
            .put("userId", userId)
            .put("cas", mr.cas())
            .put("timestamp", System.currentTimeMillis()))
        .onFailure().recoverWithItem(
        error -> JsonObject.create()
                .put("success", false)
                .put("error", "User creation failed: " + error.getMessage())
        );
}
```

### **Step 3: Implement Read Operations**

```java
@GET
@Produces(MediaType.APPLICATION_JSON)
@Path("users/{userId}")
public Uni<Map<String, Object>> getUser(@PathParam("userId") String userId) {
    ReactiveBucket bucket = cluster.bucket("default").reactive();
    ReactiveCollection collection = bucket.defaultCollection();
    Uni<GetResult> uniResult = FromMono.INSTANCE.from(collection.get(userId));
    
    return uniResult.map( result -> {
                JsonObject user = result.contentAsObject();
                // Convert Couchbase JsonObject to Map for proper serialization
                Map<String, Object> userMap = new HashMap<>();
                for (String key : user.getNames()) {
                    userMap.put(key, user.get(key));
                }
                Map<String, Object> response = Map.of(
                        "success", true,
                        "user", userMap,
                        "cas", result.cas(),
                        "timestamp", System.currentTimeMillis()
                );
                return response;
            }
    ).onFailure().recoverWithItem(error -> {
                Map<String, Object> response = Map.of(
                        "success", false,
                        "error", "User retrieval failed: " + error.getMessage(),
                        "timestamp", System.currentTimeMillis());
                return response;
            }
    );
}

@GET
@Produces(MediaType.TEXT_PLAIN)
@Path("users/all")
public Uni<List<JsonObject>> getAllUsers() {
    ReactiveBucket bucket = cluster.bucket("default").reactive();
    String query = "SELECT META().id, * FROM `_default` WHERE META().id LIKE 'user-%' LIMIT 50";
    Mono<List<JsonObject>> objects = bucket.defaultScope()
            .query(query)
            .flatMap(r -> r.rowsAsObject().collectList());
    Uni<List<JsonObject>> results = FromMono.INSTANCE.from(objects);
    results.onFailure().invoke(err -> LOG.error(err));
    return results;
}
```

### **Step 4: Implement Update Operations**

```java
@PUT
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("users/{userId}")
public Uni<JsonObject> updateUser(@PathParam("userId") String userId, String updateJson) {
    ReactiveBucket bucket = cluster.bucket("default").reactive();
    ReactiveCollection collection = bucket.defaultCollection();
    
    Uni<JsonObject> uniUpdates = Uni.createFrom().item(() -> {
        // Add metadata
        return JsonObject.fromJson(updateJson)
                .put("updatedAt", System.currentTimeMillis());
    }).onFailure().invoke(err -> LOG.error("error while parsing JSON: " + err.getMessage()));
    
    Uni<GetResult> uniUser = FromMono.INSTANCE.from(collection.get(userId));
    
    Uni<MutationResult> uniMutRes = Uni.combine().all().unis(uniUser, uniUpdates)
            .asTuple().flatMap(tuple -> {
                GetResult existingUser = tuple.getItem1();
                JsonObject currentUser = existingUser.contentAsObject();
                // Merge updates with existing user data
                for (String key : tuple.getItem2().getNames()) {
                    if (!key.equals("id") && !key.equals("createdAt")) {
                        currentUser.put(key, tuple.getItem2().get(key));
                    }
                }
                return FromMono.INSTANCE.from(collection.replace(userId, currentUser, ReplaceOptions.replaceOptions().cas(existingUser.cas())));
            });
    
    return uniMutRes.map(r -> JsonObject.create()
                                        .put("success", true)
                                        .put("message", "User updated successfully")
                                        .put("userId", userId)
                                        .put("cas", r.cas())
                                        .put("timestamp", System.currentTimeMillis()))
            .onFailure().recoverWithItem(err -> JsonObject.create()
                    .put("success", false)
                    .put("error", "User update failed: " + err.getMessage()));
}
```

### **Step 5: Implement Delete Operations**

```java
@DELETE
@Produces(MediaType.APPLICATION_JSON)
@Path("users/{userId}")
public Uni<JsonObject> deleteUser(@PathParam("userId") String userId) {
    ReactiveBucket bucket = cluster.bucket("default").reactive();
    ReactiveCollection collection = bucket.defaultCollection();
    Uni<MutationResult> result = FromMono.INSTANCE.from(collection.remove(userId));
    
    return result
            .map(r -> JsonObject.create()
                    .put("success", true)
                    .put("message", "User deleted successfully")
                    .put("userId", userId)
                    .put("mutationToken", r.mutationToken().toString())
                    .put("timestamp", System.currentTimeMillis()))
            .onFailure().recoverWithItem(error -> JsonObject.create()
                    .put("success", false)
                    .put("error", "User deletion failed: " + error.getMessage())
                    .put("key", userId)
                    .put("timestamp", System.currentTimeMillis()));
}
```

### **Step 6: Implement Advanced CRUD Operations**

```java
@POST
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("users/batch")
public Uni<JsonObject> batchCreateUsers(String usersJson) {
    Multi objects = Uni.createFrom().item(() ->
                    JsonArray.fromJson(usersJson))
            .onFailure(InvalidArgumentException.class).invoke(err -> LOG.error("Given argument is invalid JSON"))
            .onItem().transformToMulti(array -> Multi.createFrom().iterable(array.toList()));
    
    Multi<JsonObject> multiUsers = objects.map( obj -> {
                JsonObject user = (JsonObject) obj;
                String userId = "user-" + System.currentTimeMillis() ;
                user.put("id", userId);
                user.put("createdAt", System.currentTimeMillis());
                user.put("updatedAt", System.currentTimeMillis());
                return user;
            }
    ).onFailure(ClassCastException.class).invoke(error -> LOG.error("Unsafe cast"));
    
    ReactiveBucket bucket = cluster.bucket("default").reactive();
    ReactiveCollection collection = bucket.defaultCollection();
    Multi<MutationResult> multiInsertResults = multiUsers.flatMap(u -> FromMono.INSTANCE.from(collection.insert(u.getString("id"), u)).toMulti());
    Multi<JsonObject> multiJOS = multiInsertResults.map( mr -> JsonObject.create()
                    .put("success", true)
                    .put("cas", mr.cas()))
            .onFailure().recoverWithItem(err -> JsonObject.create()
                    .put("success", false)
                    .put("error", err.getMessage()));
    
    return multiJOS.collect().asList().map(list -> JsonArray.from(list))
            .map(resultsArray -> JsonObject.create()
                    .put("success", true)
                    .put("results", resultsArray)
                    .put("totalProcessed", resultsArray.size())
                    .put("timestamp", System.currentTimeMillis()))
            .onFailure().recoverWithItem(e -> JsonObject.create()
                    .put("success", false)
                    .put("error", "Invalid JSON format: " + e.getMessage()));
}
```

## **Testing the CRUD Operations**

### **Test Data**

Create a test user:
```bash
curl -X POST http://localhost:8080/couchbase/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john.doe@example.com",
    "role": "developer"
  }'
```

### **Test All Operations**

1. **Create User**:
```bash
curl -X POST http://localhost:8080/couchbase/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Jane Smith", "email": "jane.smith@example.com", "role": "admin"}'
```

2. **Get User**:
```bash
curl http://localhost:8080/couchbase/users/user-1756793442664
```

3. **Update User**:
```bash
curl -X PUT http://localhost:8080/couchbase/users/user-1756794823112-1 \
  -H "Content-Type: application/json" \
  -d '{"role": "senior-developer", "department": "Engineering"}'
```

4. **Get All Users**:
```bash
curl http://localhost:8080/couchbase/users/all
```

5. **Delete User**:
```bash
curl -X DELETE http://localhost:8080/couchbase/users/user-1756794779179
```

6. **Batch Create**:
```bash
curl -X POST http://localhost:8080/couchbase/users/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "Ryan Gosling", "email": "ryan@example.com", "role": "designer"},
    {"name": "Bib Roaster", "email": "bib@example.com", "role": "tester"}
  ]'
```

## 🔍 **Key Learning Points**

### **Reactive Patterns Used**
1. **Error Handling**: `onFailure().recoverWithItem()` for graceful error recovery
2. **Data Transformation**: `map()` for converting responses
3. **Composition**: `flatMap()` for chaining operations
4. **Batch Processing**: `Multi.collect().asList()` for collecting multiple results
5. **Type Conversion**: `FromMono.INSTANCE.from()` for converting Project Reactor to Mutiny
6. **Combining Operations**: `Uni.combine().all().unis().asTuple()` for parallel operations
7. **Error Logging**: `onFailure().invoke()` for logging errors without stopping the stream

### **Couchbase Operations**
1. **Insert**: For creating new documents
2. **Get**: For retrieving documents
3. **Replace**: For updating existing documents
4. **Remove**: For deleting documents
5. **Query**: For complex data retrieval

### **Best Practices**
1. **Input Validation**: Always validate JSON input with proper error handling
2. **Error Logging**: Use `onFailure().invoke()` for logging errors with context
3. **Consistent Response Format**: Use standardized response structure with success/error flags
4. **Metadata Management**: Track creation and update timestamps
5. **CAS Handling**: Use CAS for optimistic concurrency control in updates
6. **Type Conversion**: Use `FromMono.INSTANCE.from()` for converting Reactor types to Mutiny
7. **Stream Processing**: Use `Multi` for batch operations and `Uni` for single operations

## 🎯 **Next Steps**

After completing this exercise, you should:
- Understand how to implement reactive CRUD operations
- Be comfortable with Mutiny types and operators
- Know how to handle errors in reactive streams
- Be ready for Exercise 4: Performing reactive N1QL queries

## 📝 **Exercise Completion Checklist**

- [ ] Implement Create operation (POST /users)
- [ ] Implement Read operation (GET /users/{userId})
- [ ] Implement Read All operation (GET /users)
- [ ] Implement Update operation (PUT /users/{userId})
- [ ] Implement Delete operation (DELETE /users/{userId})
- [ ] Implement Batch Create operation (POST /users/batch)
- [ ] Test all operations with sample data
- [ ] Verify error handling works correctly
- [ ] Understand reactive patterns used

## 🔄 **Key Implementation Patterns**

### **FromMono.INSTANCE.from() Pattern**
This is the core pattern used throughout the implementation:
```java
// Convert Project Reactor Mono to Mutiny Uni
Uni<MutationResult> result = FromMono.INSTANCE.from(collection.insert(userId, user));
```

### **Uni.combine().all().unis().asTuple() Pattern**
For combining multiple operations:
```java
Uni<MutationResult> uniMutRes = Uni.combine().all().unis(uniUser, uniUpdates)
    .asTuple().flatMap(tuple -> {
        // Process both results
    });
```

### **Multi Processing Pattern**
For batch operations:
```java
Multi<JsonObject> multiUsers = objects.map(obj -> {
    // Transform each item
}).flatMap(u -> FromMono.INSTANCE.from(collection.insert(u.getString("id"), u)).toMulti());
`
