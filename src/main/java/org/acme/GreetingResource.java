package org.acme;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveBucket;
import com.couchbase.client.java.AsyncCollection;
import com.couchbase.client.java.AsyncBucket;   
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.kv.ReplaceOptions;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.converters.uni.UniReactorConverters;
import io.smallrye.mutiny.converters.uni.FromMono;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.ArrayList;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.time.Duration;
 
@ApplicationScoped
@Path("couchbase")
public class GreetingResource {

    private static final Logger LOG = Logger.getLogger(GreetingResource.class);
 
@Inject
Cluster cluster;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("health")
    public Uni<String> healthCheck() {
        return Uni.createFrom().completionStage(cluster.async().ping())
                .onItem().transform(result -> "Couchbase cluster is healthy!")
                .onFailure().recoverWithItem(t -> "Cluster health check failed: " + t.getMessage());
    }
 
@GET
@Produces(MediaType.TEXT_PLAIN)
@Path("simpleUpsert")
    public Uni<String> simpleUpsert() {
        AsyncBucket bucket = cluster.bucket("default").async();
        AsyncCollection collection = bucket.defaultCollection();
        
        String key = "simple-doc-" + System.currentTimeMillis();
        JsonObject content = JsonObject.create()
                .put("author", "mike2")
                .put("title", "My Blog Post 2r4")
                .put("timestamp", System.currentTimeMillis())
                .put("documentId", key);

        return Uni.createFrom().completionStage(collection.upsert(key, content))
                .onItem().transform(result -> "Document upserted successfully! Key: " + key + ", Mutation token: " + result.mutationToken())
                .onFailure().recoverWithItem(t -> "Document upsert failed: " + t.getMessage());
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("reactiveUpsert")
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

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("list-documents")
    public Uni<String> listDocuments() {
        AsyncBucket bucket = cluster.bucket("default").async();
        String query = "SELECT META().id, * FROM `default` WHERE META().id LIKE '%doc-%' LIMIT 10";
        
        return Uni.createFrom().completionStage(cluster.async().query(query))
                .onItem().transform(result -> {
                    StringBuilder response = new StringBuilder();
                    response.append("{\n  \"documents\": [\n");
                    
                    List<JsonObject> rows = result.rowsAsObject();
                    for (int i = 0; i < rows.size(); i++) {
                        JsonObject row = rows.get(i);
                        response.append("    {\n");
                        response.append("      \"id\": \"").append(row.getString("id")).append("\",\n");
                        response.append("      \"content\": ").append(row.getObject("default").toString()).append("\n");
                        if (i < rows.size() - 1) {
                            response.append("    },\n");
                        } else {
                            response.append("    }\n");
                        }
                    }
                    response.append("  ],\n");
                    response.append("  \"total\": ").append(rows.size()).append("\n");
                    response.append("}\n");
                    
                    return response.toString();
                })
                .onFailure().recoverWithItem(error -> "{\"error\": \"Query failed: " + error.getMessage() + "\"}");
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("test-json")
    public Uni<String> testJsonResponse() {
        return Uni.createFrom().item("JSON response test successful - " + System.currentTimeMillis());
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("clusterInfo")
    public Uni<String> getClusterInfo() {
        return Uni.createFrom().completionStage(cluster.async().diagnostics())
                .onItem().transform(info -> "Cluster diagnostics retrieved successfully")
                .onFailure().recoverWithItem(t -> "Failed to get cluster info: " + t.getMessage());
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("document/{key}")
    public Uni<String> getDocument(@PathParam("key") String key) {
        AsyncBucket bucket = cluster.bucket("default").async();
        AsyncCollection collection = bucket.defaultCollection();
        
        return Uni.createFrom().completionStage(collection.get(key))
                .onItem().transform(result -> {
                try {
                    JsonObject content = result.contentAsObject();
                    return "Document found - Key: " + key + ", CAS: " + result.cas() + ", Content: " + content.toString();
                } catch (Exception e) {
                    LOG.error("Error processing document content", e);
                    return "Error processing document: " + e.getMessage();
                }
            })
                .onFailure().recoverWithItem(t -> "Document retrieval failed: " + t.getMessage());
    }

    @DELETE
    @Path("document/{key}")
    public Uni<JsonObject> deleteDocument(@PathParam("key") String key) {
        AsyncBucket bucket = cluster.bucket("default").async();
        AsyncCollection collection = bucket.defaultCollection();
        
        return Uni.createFrom().completionStage(collection.remove(key))
                .map(mr -> JsonObject.create()
                        .put("message", "Document deleted successfully")
                        .put("key", key)
                        .put("mutationToken", mr.mutationToken().toString())
                        .put("timestamp", System.currentTimeMillis()))
                .onFailure().recoverWithItem(error ->
                        JsonObject.create()
                        .put("error", "Document deletion failed: " + error.getMessage())
                        .put("key", key)
                        .put("timestamp", System.currentTimeMillis())
                );
    }
 
// @ApplicationScoped
// @Path("couchbase")
// public class GreetingResource {

//     private static final Logger LOG = Logger.getLogger(GreetingResource.class);
 
// @Inject
// Cluster cluster;

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("test-reactive")
//     public Uni<String> testReactive() {
//         return Uni.createFrom().item("Reactive endpoint works!");
//     }

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("health-sync")
//     public String healthCheckSync() {
//         try {
//             // Test basic synchronous connection
//             cluster.ping();
//             return "Couchbase cluster is healthy (sync)!";
//         } catch (Exception e) {
//             LOG.error("Sync health check failed", e);
//             return "Couchbase cluster connection failed: " + e.getMessage();
//         }
//     }

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("health")
//         public Uni<String> healthCheck() {
//         return Uni.createFrom().completionStage(cluster.async().ping())
//                 .onItem().transform(result -> "Couchbase cluster is healthy!")
//                 .onFailure().recoverWithItem(t -> "Cluster health check failed: " + t.getMessage());
//     }
 
// @GET
// @Produces(MediaType.TEXT_PLAIN)
// @Path("simpleUpsert")
//     public Uni<String> simpleUpsert() {
//         AsyncBucket bucket = cluster.bucket("default").async();
//         AsyncCollection collection = bucket.defaultCollection();
 
//     JsonObject content = JsonObject.create()
//             .put("author", "mike2")
//             .put("title", "My Blog Post 2r4")
//             .put("timestamp", System.currentTimeMillis());

//         return Uni.createFrom().completionStage(collection.upsert("document-key", content))
//                 .onItem().transform(result -> "Document upserted successfully! Mutation token: " + result.mutationToken())
//                 .onFailure().recoverWithItem(t -> "Document upsert failed: " + t.getMessage());
//     }

//     @POST
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("reactiveUpsert")
//     public Uni<String> reactiveUpsert(String documentJson) {
//         try {
//             JsonObject document = JsonObject.fromJson(documentJson);
//             AsyncBucket bucket = cluster.bucket("default").async();
//             AsyncCollection collection = bucket.defaultCollection();
            
//             String key = "doc-" + System.currentTimeMillis();
            
//             // Add metadata
//             document.put("createdAt", System.currentTimeMillis());
//             document.put("documentId", key);
            
//             return Uni.createFrom().completionStage(collection.upsert(key, document))
//                     .onItem().transform(result -> "Document upserted successfully - Key: " + key + ", Mutation token: " + result.mutationToken() + ", CAS: " + result.cas())
//                     .onFailure().recoverWithItem(t -> "Document upsert failed: " + t.getMessage());
//         } catch (Exception e) {
//             LOG.error("Failed to parse reactive upsert input", e);
//             return Uni.createFrom().item("Failed to parse input: " + e.getMessage());
//         }
//     }

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("test-json")
//     public Uni<String> testJsonResponse() {
//         return Uni.createFrom().item("JSON response test successful - " + System.currentTimeMillis());
//     }

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("clusterInfo")
//     public Uni<String> getClusterInfo() {
//         return Uni.createFrom().completionStage(cluster.async().diagnostics())
//                 .onItem().transform(info -> "Cluster diagnostics retrieved successfully")
//                 .onFailure().recoverWithItem(t -> "Failed to get cluster info: " + t.getMessage());
//     }

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("document/{key}")
//     public Uni<String> getDocument(@PathParam("key") String key) {
//                 AsyncBucket bucket = cluster.bucket("default").async();
//         AsyncCollection collection = bucket.defaultCollection();
        
//         return Uni.createFrom().completionStage(collection.get(key))
//                 .onItem().transform(result -> {
//                 try {
//                     JsonObject content = result.contentAsObject();
//                     return "Document found - Key: " + key + ", CAS: " + result.cas() + ", Content: " + content.toString();
//                 } catch (Exception e) {
//                     LOG.error("Error processing document content", e);
//                     return "Error processing document: " + e.getMessage();
//                 }
//             })
//                 .onFailure().recoverWithItem(t -> "Document retrieval failed: " + t.getMessage());
//     }

//     @DELETE
//     @Path("document/{key}")
//     public Uni<JsonObject> deleteDocument(@PathParam("key") String key) {
//                 AsyncBucket bucket = cluster.bucket("default").async();
//         AsyncCollection collection = bucket.defaultCollection();
        
//         return Uni.createFrom().completionStage(collection.remove(key))
//                 .onItem().transform(result -> JsonObject.create()
//                 .put("message", "Document deleted successfully")
//                 .put("key", key)
//                 .put("mutationToken", result.mutationToken().toString())
//                 .put("timestamp", System.currentTimeMillis()))
//                 .onFailure().recoverWithItem(t -> JsonObject.create()
//                     .put("error", "Document deletion failed: " + t.getMessage())
//                     .put("key", key)
//                     .put("timestamp", System.currentTimeMillis()));
//     }

//     @POST
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("batchUpsert")
//     public Uni<String> batchUpsert(String documentsJson) {
//         try {
//             // Parse the JSON array from the string input
//             JsonArray documentsArray = JsonArray.fromJson(documentsJson);
//             List<JsonObject> documents = new ArrayList<>();
            
//             for (int i = 0; i < documentsArray.size(); i++) {
//                 documents.add(documentsArray.getObject(i));
//             }
            
//                         AsyncBucket bucket = cluster.bucket("default").async();
//             AsyncCollection collection = bucket.defaultCollection();
            
//             List<Uni<JsonObject>> operations = new ArrayList<>();
            
//             for (int i = 0; i < documents.size(); i++) {
//                 JsonObject doc = documents.get(i);
//                 String key = "batch-" + System.currentTimeMillis() + "-" + i;
//                 doc.put("batchId", i);
//                 doc.put("createdAt", System.currentTimeMillis());
                
//                 Uni<JsonObject> operation = Uni.createFrom().completionStage(collection.upsert(key, doc))
//                         .onItem().transform(result -> {
//                         doc.put("mutationToken", result.mutationToken().toString());
//                         doc.put("cas", result.cas());
//                         doc.put("key", key);
//                         return doc;
//                         })
//                         .onFailure().recoverWithItem(t -> JsonObject.create()
//                             .put("error", "Upsert failed: " + t.getMessage())
//                             .put("key", key));
                
//                 operations.add(operation);
//             }
            
//             return Uni.join().all(operations).andCollectFailures()
//                 .onItem().transform(objects -> {
//                     return "Batch upsert completed successfully - Total documents: " + documents.size() + ", Successful upserts: " + objects.size();
//                 })
//                 .onFailure().recoverWithItem(error -> "Batch upsert failed: " + error.getMessage());
            
//         } catch (Exception e) {
//             LOG.error("Failed to parse batch upsert input", e);
//             return Uni.createFrom().item("Failed to parse input: " + e.getMessage());
//         }
//     }

//     @POST
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("query")
//     public Uni<String> executeQuery(String queryRequestJson) {
//         try {
//             JsonObject queryRequest = JsonObject.fromJson(queryRequestJson);
//             String query = queryRequest.getString("query");
//             JsonObject parameters = queryRequest.getObject("parameters");
            
//             if (parameters == null) {
//                 parameters = JsonObject.create();
//             }
            
//             return Uni.createFrom().completionStage(cluster.async().query(query))
//                     .onItem().transform(result -> {
//                         try {
//                             List<JsonObject> rows = new ArrayList<>();
//                             result.rowsAsObject().forEach(rows::add);
//                             return "Query executed successfully - Query: " + query + ", Row count: " + rows.size();
//                         } catch (Exception e) {
//                             LOG.error("Error processing query result", e);
//                             return "Query executed but failed to process results: " + e.getMessage();
//                         }
//                     })
//                     .onFailure().recoverWithItem(t -> "Query execution failed: " + t.getMessage());
//         } catch (Exception e) {
//             LOG.error("Failed to parse query input", e);
//             return Uni.createFrom().item("Failed to parse input: " + e.getMessage());
//         }
//     }

//     @GET
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("document/{key}/exists")
//     public Uni<String> checkDocumentExists(@PathParam("key") String key) {
//         AsyncBucket bucket = cluster.bucket("default").async();
//         AsyncCollection collection = bucket.defaultCollection();
        
//         return Uni.createFrom().completionStage(collection.exists(key))
//                 .onItem().transform(exists -> "Document exists check - Key: " + key + ", Exists: " + exists.exists())
//                 .onFailure().recoverWithItem(t -> "Document existence check failed: " + t.getMessage());
//     }

//     @POST
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.TEXT_PLAIN)
//     @Path("demo")
//     public Uni<String> demonstrateReactivePatterns(String demoRequestJson) {
//         try {
//             // This demonstrates combining multiple reactive operations
//             AsyncBucket bucket = cluster.bucket("default").async();
//             AsyncCollection collection = bucket.defaultCollection();
            
//             return Uni.createFrom().completionStage(cluster.async().diagnostics())
//                     .onItem().transformToUni(info -> {
//                     JsonObject demoDoc = JsonObject.create()
//                         .put("type", "demo")
//                         .put("clusterId", info.id())
//                         .put("createdAt", System.currentTimeMillis());
                    
//                         return Uni.createFrom().completionStage(collection.upsert("demo-doc", demoDoc))
//                                 .onItem().transform(result -> "Reactive pattern demonstration completed - Mutation token: " + result.mutationToken());
//                     })
//                     .onFailure().recoverWithItem(t -> "Demo operation failed: " + t.getMessage());
//         } catch (Exception e) {
//             LOG.error("Failed to process demo request", e);
//             return Uni.createFrom().item("Demo operation failed: " + e.getMessage());
//         }
//     }

//     // ===== EXERCISE 3: CRUD Operations as Reactive Endpoints =====
    
//     @POST
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users")
//     public Uni<JsonObject> createUser(String userJson) {
//         try {
//             JsonObject user = JsonObject.fromJson(userJson);
//             String userId = "user-" + System.currentTimeMillis();
            
//             // Add metadata
//             user.put("id", userId);
//             user.put("createdAt", System.currentTimeMillis());
//             user.put("updatedAt", System.currentTimeMillis());
            
//                         AsyncBucket bucket = cluster.bucket("default").async();
//             AsyncCollection collection = bucket.defaultCollection();
            
//             return Uni.createFrom().completionStage(collection.insert(userId, user))
//                     .onItem().transform(result -> JsonObject.create()
//                     .put("success", true)
//                     .put("message", "User created successfully")
//                     .put("userId", userId)
//                     .put("cas", result.cas())
//                     .put("timestamp", System.currentTimeMillis()))
//                     .onFailure().recoverWithItem(t -> JsonObject.create()
//                         .put("success", false)
//                         .put("error", "User creation failed: " + t.getMessage()));
//         } catch (Exception e) {
//             return Uni.createFrom().item(JsonObject.create()
//                 .put("success", false)
//                 .put("error", "Invalid JSON format: " + e.getMessage()));
//         }
//     }
    
//     @GET
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/{userId}")
//     public Uni<Map<String, Object>> getUser(@PathParam("userId") String userId) {
//                 AsyncBucket bucket = cluster.bucket("default").async();
//         AsyncCollection collection = bucket.defaultCollection();
        
//         return Uni.createFrom().completionStage(collection.get(userId))
//                 .onItem().transform(result -> {
//                 JsonObject user = result.contentAsObject();
//                     // Convert Couchbase JsonObject to Map for proper serialization
//                     Map<String, Object> userMap = new HashMap<>();
//                     for (String key : user.getNames()) {
//                         userMap.put(key, user.get(key));
//                     }
                    
//                     Map<String, Object> response = new HashMap<>();
//                     response.put("success", true);
//                     response.put("user", userMap);
//                     response.put("cas", result.cas());
//                     response.put("timestamp", System.currentTimeMillis());
//                     return response;
//                 })
//                 .onFailure().recoverWithItem(t -> {
//                     Map<String, Object> errorResponse = new HashMap<>();
//                     errorResponse.put("success", false);
//                     errorResponse.put("error", "User retrieval failed: " + t.getMessage());
//                     errorResponse.put("timestamp", System.currentTimeMillis());
//                     return errorResponse;
//                 });
//     }
    
//     // @GET
//     // @Produces(MediaType.APPLICATION_JSON)
//     // @Path("users/all")
//     // public Uni<Map<String, Object>> getAllUsers() {
//     //     ReactiveBucket bucket = cluster.bucket("default").reactive();
//     //     ReactiveCollection collection = bucket.defaultCollection();
        
//     //     // Test with a simple Map instead of Couchbase JsonObject
//     //     Map<String, Object> response = Map.of(
//     //         "success", true,
//     //         "message", "Endpoint is working - N1QL query temporarily disabled",
//     //         "note", "There are 7 users in the database (verified via direct Couchbase query)",
//     //         "timestamp", System.currentTimeMillis(),
//     //         "count", 0
//     //     );
        
//     //     return Uni.createFrom().item(response);
//     // }

// @GET
// @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/all")
// public Uni<Map<String, Object>> getAllUsers() {
//         String query = "SELECT META().id, * FROM `default` WHERE META().id LIKE 'user-%' LIMIT 50";
    
//         return Uni.createFrom().completionStage(cluster.async().query(query))
//                 .onItem().transform(result -> {
//                     try {
//                         Map<String, Object> response = new HashMap<>();
                        
//                         // Extract actual user data from the query results
//                         List<Map<String, Object>> actualUsers = new ArrayList<>();
//                         result.rowsAsObject().forEach(row -> {
//                             Map<String, Object> userMap = new HashMap<>();
//                             for (String key : row.getNames()) {
//                                 userMap.put(key, row.get(key));
//                             }
//                             actualUsers.add(userMap);
//                         });
                        
//                         response.put("users", actualUsers);
//                         response.put("count", actualUsers.size());
//                         response.put("query", query);
//                         response.put("debug", "Query executed successfully");
                        
//                         return response;
//                     } catch (Exception e) {
//                         System.err.println("Query processing error: " + e.getMessage());
//                         Map<String, Object> errorResponse = new HashMap<>();
//                         errorResponse.put("error", "Query processing failed: " + e.getMessage());
//                         errorResponse.put("query", query);
//                         return errorResponse;
//                     }
//                 })
//                 .onFailure().recoverWithItem(t -> {
//                     System.err.println("Query error: " + t.getMessage());
//                     Map<String, Object> errorResponse = new HashMap<>();
//                     errorResponse.put("error", "Query failed: " + t.getMessage());
//                     errorResponse.put("query", query);
//                     return errorResponse;
//                 });
// }

// // @GET
// // @Produces(MediaType.APPLICATION_JSON)
// // @Path("users/all")
// // public Uni<List<JsonObject>> getAllUsers() {
// //     AsyncBucket bucket = cluster.bucket("default").async();
// //     AsyncCollection collection = bucket.defaultCollection();
// //     String query = "SELECT META().id, * FROM `default` WHERE META().id LIKE 'user-%' LIMIT 50";
// //     var asyncQuery = cluster.async()
// //             .query(query);
// //     var queryToUni = Uni.createFrom().completionStage(asyncQuery);
// //     Uni<List<JsonObject>> objects = queryToUni.onItem().transform(qr -> qr.rowsAsObject());
// //     return objects;
// // }

//     // @GET
//     // @Produces(MediaType.APPLICATION_JSON)
//     // @Path("users/all")
//     // public Multi<JsonObject> getAllUsers() {
//     //     ReactiveBucket bucket = cluster.bucket("default").reactive();
//     //     ReactiveCollection collection = bucket.defaultCollection();
        
//     //     // Use a simple N1QL query with proper converter approach
//     //     String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%' LIMIT 50";
        
//     //     // Convert the Reactor Mono to Mutiny Multi using the proper converter
//     //     return Multi.createFrom().publisher(
//     //         cluster.reactive().query(query, QueryOptions.queryOptions().timeout(Duration.ofSeconds(10)))
//     //             .flatMapMany(result -> result.rowsAsObject())
//     //             .toStream()
//     //             .collect(java.util.stream.Collectors.toList())
//     //             .stream()
//     //     );
//     // }
    
//     @PUT
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/{userId}")
//     public Uni<JsonObject> updateUser(@PathParam("userId") String userId, String updateJson) {
//         try {
//             JsonObject updates = JsonObject.fromJson(updateJson);
//             updates.put("updatedAt", System.currentTimeMillis());
            
//                         AsyncBucket bucket = cluster.bucket("default").async();
//             AsyncCollection collection = bucket.defaultCollection();
            
//             return Uni.createFrom().completionStage(collection.get(userId))
//                     .onItem().transformToUni(existingUser -> {
//                     JsonObject currentUser = existingUser.contentAsObject();
                    
//                     // Merge updates with existing user data
//                     for (String key : updates.getNames()) {
//                         if (!key.equals("id") && !key.equals("createdAt")) {
//                             currentUser.put(key, updates.get(key));
//                         }
//                     }
                    
//                         return Uni.createFrom().completionStage(
//                             collection.replace(userId, currentUser, ReplaceOptions.replaceOptions().cas(existingUser.cas()))
//                         ).onItem().transform(result -> JsonObject.create()
//                     .put("success", true)
//                     .put("message", "User updated successfully")
//                     .put("userId", userId)
//                     .put("cas", result.cas())
//                             .put("timestamp", System.currentTimeMillis()));
//                     })
//                     .onFailure().recoverWithItem(t -> JsonObject.create()
//                         .put("success", false)
//                         .put("error", "User update failed: " + t.getMessage()));
//         } catch (Exception e) {
//             return Uni.createFrom().item(JsonObject.create()
//                 .put("success", false)
//                 .put("error", "Invalid JSON format: " + e.getMessage()));
//         }
//     }
    
//     @GET
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/test")
//     public Uni<Map<String, Object>> testEndpoint() {
//         Map<String, Object> response = new HashMap<>();
//         response.put("message", "Test endpoint working");
//         response.put("timestamp", System.currentTimeMillis());
//         response.put("status", "ok");
//         return Uni.createFrom().item(response);
//     }

//     @GET
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/debug")
//     public Uni<Map<String, Object>> debugUsers() {
//         // First, let's test if we can access the cluster
//         try {
//             return Uni.createFrom().completionStage(cluster.async().query("SELECT META().id, * FROM `default` LIMIT 5"))
//                     .onItem().transform(result -> {
//                         try {
//                             Map<String, Object> response = new HashMap<>();
                            
//                             // Extract actual document data
//                             List<Map<String, Object>> actualDocs = new ArrayList<>();
//                             result.rowsAsObject().forEach(row -> {
//                                 Map<String, Object> docMap = new HashMap<>();
//                                 for (String key : row.getNames()) {
//                                     docMap.put(key, row.get(key));
//                                 }
//                                 actualDocs.add(docMap);
//                             });
                            
//                             response.put("sampleDocuments", actualDocs);
//                             response.put("bucket", "default");
//                             response.put("collection", "default");
//                             response.put("status", "connected");
//                             return response;
//                         } catch (Exception e) {
//                             System.err.println("Couchbase query processing error: " + e.getMessage());
//                             Map<String, Object> errorResponse = new HashMap<>();
//                             errorResponse.put("error", "Failed to process query results: " + e.getMessage());
//                             errorResponse.put("status", "disconnected");
//                             return errorResponse;
//                         }
//                     })
//                     .onFailure().recoverWithItem(t -> {
//                         System.err.println("Couchbase query error: " + t.getMessage());
//                         Map<String, Object> errorResponse = new HashMap<>();
//                         errorResponse.put("error", "Failed to connect to Couchbase: " + t.getMessage());
//                         errorResponse.put("status", "disconnected");
//                         return errorResponse;
//                     });
//         } catch (Exception e) {
//             Map<String, Object> errorResponse = new HashMap<>();
//             errorResponse.put("error", "Failed to connect to Couchbase: " + e.getMessage());
//             errorResponse.put("status", "disconnected");
//             return Uni.createFrom().item(errorResponse);
//         }
//     }
    
//     @DELETE
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/{userId}")
//     public Uni<JsonObject> deleteUser(@PathParam("userId") String userId) {
//                 AsyncBucket bucket = cluster.bucket("default").async();
//         AsyncCollection collection = bucket.defaultCollection();
        
//         return Uni.createFrom().completionStage(collection.remove(userId))
//                 .onItem().transform(result -> JsonObject.create()
//                 .put("success", true)
//                 .put("message", "User deleted successfully")
//                 .put("userId", userId)
//                 .put("mutationToken", result.mutationToken().toString())
//                 .put("timestamp", System.currentTimeMillis()))
//                 .onFailure().recoverWithItem(t -> JsonObject.create()
//                     .put("success", false)
//                     .put("error", "User deletion failed: " + t.getMessage())
//                     .put("key", userId)
//                     .put("timestamp", System.currentTimeMillis()));
//     }
    
//     @POST
//     @Consumes(MediaType.APPLICATION_JSON)
//     @Produces(MediaType.APPLICATION_JSON)
//     @Path("users/batch")
//     public Uni<JsonObject> batchCreateUsers(String usersJson) {
//         try {
//             JsonArray usersArray = JsonArray.fromJson(usersJson);
//             List<Uni<JsonObject>> operations = new ArrayList<>();
            
//             for (int i = 0; i < usersArray.size(); i++) {
//                 JsonObject user = usersArray.getObject(i);
//                 String userId = "user-" + System.currentTimeMillis() + "-" + i;
                
//                 user.put("id", userId);
//                 user.put("createdAt", System.currentTimeMillis());
//                 user.put("updatedAt", System.currentTimeMillis());
                
//                                 AsyncBucket bucket = cluster.bucket("default").async();
//                 AsyncCollection collection = bucket.defaultCollection();
                
//                 Uni<JsonObject> operation = Uni.createFrom().completionStage(collection.insert(userId, user))
//                         .onItem().transform(result -> JsonObject.create()
//                         .put("userId", userId)
//                         .put("success", true)
//                         .put("cas", result.cas()))
//                         .onFailure().recoverWithItem(t -> JsonObject.create()
//                         .put("userId", userId)
//                         .put("success", false)
//                             .put("error", t.getMessage()));
                
//                 operations.add(operation);
//             }
            
//             return Uni.join().all(operations).andCollectFailures()
//                 .onItem().transform(objects -> {
//                 List<JsonObject> results = new ArrayList<>();
//                 for (Object obj : objects) {
//                     results.add((JsonObject) obj);
//                 }
                
//                 // Create a proper JsonArray from the results list
//                 JsonArray resultsArray = JsonArray.create();
//                 for (JsonObject result : results) {
//                     resultsArray.add(result);
//                 }
                
//                 return JsonObject.create()
//                     .put("success", true)
//                     .put("results", resultsArray)
//                     .put("totalProcessed", results.size())
//                     .put("timestamp", System.currentTimeMillis());
//             });
//         } catch (Exception e) {
//             return Uni.createFrom().item(JsonObject.create()
//                 .put("success", false)
//                 .put("error", "Invalid JSON format: " + e.getMessage()));
//         }
//     }


// ----------------------------------------
    // // ===== EXERCISE 5: Error Handling and Retry Strategies =====
    
    // @GET
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/{userId}/resilient")
    // public Uni<JsonObject> getUserResilient(@PathParam("userId") String userId) {
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
        
    //     return Uni.createFrom().emitter(emitter -> {
    //         collection.get(userId)
    //             .subscribe(
    //                 result -> {
    //                     JsonObject user = result.contentAsObject();
    //                     emitter.complete(JsonObject.create()
    //                         .put("success", true)
    //                         .put("user", user)
    //                         .put("cas", result.cas())
    //                         .put("timestamp", System.currentTimeMillis()));
    //                 },
    //                 error -> {
    //                     if (error.getMessage() != null && error.getMessage().contains("not found")) {
    //                         LOG.warn("User not found: " + userId);
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", false)
    //                             .put("error", "User not found")
    //                             .put("errorCode", "USER_NOT_FOUND")
    //                             .put("userId", userId)
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     } else if (error instanceof java.util.concurrent.TimeoutException) {
    //                         LOG.error("Timeout getting user: " + userId, error);
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", false)
    //                             .put("error", "Operation timed out")
    //                             .put("errorCode", "TIMEOUT")
    //                             .put("userId", userId)
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     } else {
    //                         LOG.error("Unexpected error getting user: " + userId, error);
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", false)
    //                             .put("error", "Internal server error")
    //                             .put("errorCode", "INTERNAL_ERROR")
    //                             .put("userId", userId)
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     }
    //                 }
    //             );
    //     });
    // }
    
    // @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/retry")
    // public Uni<JsonObject> createUserWithRetry(String userJson) {
    //     try {
    //         JsonObject user = JsonObject.fromJson(userJson);
    //         String userId = "user-" + System.currentTimeMillis();
            
    //         user.put("id", userId);
    //         user.put("createdAt", System.currentTimeMillis());
    //         user.put("updatedAt", System.currentTimeMillis());
            
    //         ReactiveBucket bucket = cluster.bucket("default").reactive();
    //         ReactiveCollection collection = bucket.defaultCollection();
            
    //         return Uni.createFrom().emitter(emitter -> {
    //             collection.insert(userId, user)
    //                 .subscribe(
    //                     result -> emitter.complete(JsonObject.create()
    //                         .put("success", true)
    //                         .put("message", "User created successfully")
    //                         .put("userId", userId)
    //                         .put("cas", result.cas())
    //                         .put("timestamp", System.currentTimeMillis())),
    //                     error -> emitter.complete(JsonObject.create()
    //                         .put("success", false)
    //                         .put("error", "User creation failed after retries: " + error.getMessage())
    //                         .put("errorCode", "RETRY_EXHAUSTED")
    //                         .put("userId", userId)
    //                         .put("timestamp", System.currentTimeMillis()))
    //                 );
    //         });
    //     } catch (Exception e) {
    //         return Uni.createFrom().item(JsonObject.create()
    //             .put("success", false)
    //             .put("error", "Invalid JSON format: " + e.getMessage())
    //             .put("errorCode", "INVALID_INPUT")
    //             .put("timestamp", System.currentTimeMillis()));
    //     }
    // }
    
    // @PUT
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/{userId}/retry")
    // public Uni<JsonObject> updateUserWithRetry(@PathParam("userId") String userId, String updateJson) {
    //     try {
    //         JsonObject updates = JsonObject.fromJson(updateJson);
    //         updates.put("updatedAt", System.currentTimeMillis());
            
    //         ReactiveBucket bucket = cluster.bucket("default").reactive();
    //         ReactiveCollection collection = bucket.defaultCollection();
            
    //         return Uni.createFrom().emitter(emitter -> {
    //             collection.get(userId)
    //                 .subscribe(
    //                     existingUser -> {
    //                         JsonObject currentUser = existingUser.contentAsObject();
                            
    //                         for (String key : updates.getNames()) {
    //                             if (!key.equals("id") && !key.equals("createdAt")) {
    //                                 currentUser.put(key, updates.get(key));
    //                             }
    //                         }
                            
    //                         collection.replace(userId, currentUser, ReplaceOptions.replaceOptions().cas(existingUser.cas()))
    //                             .subscribe(
    //                                 result -> emitter.complete(JsonObject.create()
    //                                     .put("success", true)
    //                                     .put("message", "User updated successfully")
    //                                     .put("userId", userId)
    //                                     .put("cas", result.cas())
    //                                     .put("timestamp", System.currentTimeMillis())),
    //                                 error -> emitter.complete(JsonObject.create()
    //                                     .put("success", false)
    //                                     .put("error", "User update failed after retries: " + error.getMessage())
    //                                     .put("errorCode", "RETRY_EXHAUSTED")
    //                                     .put("userId", userId)
    //                                     .put("timestamp", System.currentTimeMillis()))
    //                             );
    //                     },
    //                     error -> emitter.complete(JsonObject.create()
    //                         .put("success", false)
    //                         .put("error", "User update failed after retries: " + error.getMessage())
    //                         .put("errorCode", "RETRY_EXHAUSTED")
    //                         .put("userId", userId)
    //                         .put("timestamp", System.currentTimeMillis()))
    //                 );
    //         });
    //     } catch (Exception e) {
    //         return Uni.createFrom().item(JsonObject.create()
    //             .put("success", false)
    //             .put("error", "Invalid JSON format: " + e.getMessage())
    //             .put("errorCode", "INVALID_INPUT")
    //             .put("timestamp", System.currentTimeMillis()));
    //     }
    // }
    
    // @GET
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/circuit-breaker")
    // public Uni<JsonObject> getAllUsersWithCircuitBreaker() {
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
        
    //     String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%'";
        
    //     return Uni.createFrom().emitter(emitter -> {
    //         cluster.reactive().query(query)
    //             .subscribe(
    //                 result -> {
    //                     try {
    //                         List<JsonObject> users = new ArrayList<>();
    //                         result.rowsAsObject().toStream().forEach(users::add);
                            
    //                         // Create a proper JsonArray from the users list
    //                         JsonArray usersArray = JsonArray.create();
    //                         for (JsonObject user : users) {
    //                             usersArray.add(user);
    //                         }
                            
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", true)
    //                             .put("users", usersArray)
    //                             .put("count", users.size())
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     } catch (Exception e) {
    //                         LOG.error("Error processing query result", e);
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", false)
    //                             .put("error", "Error processing query results: " + e.getMessage())
    //                             .put("errorCode", "PROCESSING_ERROR")
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     }
    //                 },
    //                 error -> emitter.complete(JsonObject.create()
    //                     .put("success", false)
    //                     .put("error", "Service temporarily unavailable due to high error rate")
    //                     .put("errorCode", "CIRCUIT_BREAKER_OPEN")
    //                     .put("timestamp", System.currentTimeMillis())
    //                     .put("fallback", true))
    //             );
    //     });
    // }
    
    // @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/bulk-resilient")
    // public Uni<JsonObject> bulkCreateUsersResilient(String usersJson) {
    //     try {
    //         JsonArray usersArray = JsonArray.fromJson(usersJson);
    //         List<Uni<JsonObject>> operations = new ArrayList<>();
            
    //         for (int i = 0; i < usersArray.size(); i++) {
    //             JsonObject user = usersArray.getObject(i);
    //             String userId = "user-" + System.currentTimeMillis() + "-" + i;
                
    //             user.put("id", userId);
    //             user.put("createdAt", System.currentTimeMillis());
    //             user.put("updatedAt", System.currentTimeMillis());
                
    //             ReactiveBucket bucket = cluster.bucket("default").reactive();
    //             ReactiveCollection collection = bucket.defaultCollection();
                
    //             Uni<JsonObject> operation = Uni.createFrom().emitter(emitter -> {
    //                 collection.insert(userId, user)
    //                     .subscribe(
    //                         result -> emitter.complete(JsonObject.create()
    //                             .put("userId", userId)
    //                             .put("success", true)
    //                             .put("cas", result.cas())),
    //                         error -> emitter.complete(JsonObject.create()
    //                             .put("userId", userId)
    //                             .put("success", false)
    //                             .put("error", error.getMessage())
    //                             .put("errorCode", "INSERT_FAILED"))
    //                     );
    //             });
                
    //             operations.add(operation);
    //         }
            
    //         return Uni.join().all(operations).andCollectFailures()
    //             .onItem().transform(objects -> {
    //                 List<JsonObject> results = new ArrayList<>();
    //                 int successCount = 0;
    //                 int failureCount = 0;
                    
    //                 for (Object obj : objects) {
    //                     JsonObject result = (JsonObject) obj;
    //                     results.add(result);
    //                     if (result.getBoolean("success")) {
    //                         successCount++;
    //                     } else {
    //                         failureCount++;
    //                     }
    //                 }
                    
    //                 // Create a proper JsonArray from the results list
    //                 JsonArray resultsArray = JsonArray.create();
    //                 for (JsonObject result : results) {
    //                     resultsArray.add(result);
    //                 }
                    
    //                 return JsonObject.create()
    //                     .put("success", failureCount == 0)
    //                     .put("results", resultsArray)
    //                     .put("totalProcessed", results.size())
    //                     .put("successCount", successCount)
    //                     .put("failureCount", failureCount)
    //                     .put("timestamp", System.currentTimeMillis());
    //             });
    //     } catch (Exception e) {
    //         return Uni.createFrom().item(JsonObject.create()
    //             .put("success", false)
    //             .put("error", "Invalid JSON format: " + e.getMessage())
    //             .put("errorCode", "INVALID_INPUT")
    //             .put("timestamp", System.currentTimeMillis()));
    //     }
    // }
    
    // @GET
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("health/advanced")
    // public Uni<JsonObject> advancedHealthCheck() {
    //     ReactiveCluster reactiveCluster = cluster.reactive();
        
    //     return Uni.createFrom().emitter(emitter -> {
    //         reactiveCluster.diagnostics()
    //             .subscribe(
    //                 info -> emitter.complete(JsonObject.create()
    //                     .put("status", "healthy")
    //                     .put("clusterId", info.id())
    //                     .put("version", info.version())
    //                     .put("sdk", info.sdk())
    //                     .put("state", info.state())
    //                     .put("endpointCount", info.endpoints().size())
    //                     .put("timestamp", System.currentTimeMillis())),
    //                 error -> {
    //                     LOG.error("Health check failed", error);
    //                     emitter.complete(JsonObject.create()
    //                         .put("status", "unhealthy")
    //                         .put("error", "Health check failed: " + error.getMessage())
    //                         .put("errorCode", "HEALTH_CHECK_FAILED")
    //                         .put("timestamp", System.currentTimeMillis()));
    //                 }
    //             );
    //     });
    // }

    // // ===== EXERCISE 6: Reactive Streams and Transactions =====
    
    // @GET
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/stream")
    // public Multi<JsonObject> streamUsers() {
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
        
    //     String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%'";
        
    //     return Multi.createFrom().emitter(emitter -> {
    //         cluster.reactive().query(query)
    //             .flatMapMany(result -> result.rowsAsObject())
    //             .subscribe(
    //                 user -> emitter.emit(JsonObject.create()
    //                     .put("user", user)
    //                     .put("timestamp", System.currentTimeMillis())),
    //                 error -> {
    //                     LOG.error("Error streaming users", error);
    //                     emitter.fail(error);
    //                 },
    //                 () -> emitter.complete()
    //             );
    //     });
    // }
    
    // @GET
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/stream-batched")
    // public Multi<JsonObject> streamUsersBatched() {
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
        
    //     String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%'";
        
    //     return Multi.createFrom().emitter(emitter -> {
    //         cluster.reactive().query(query)
    //             .flatMapMany(result -> result.rowsAsObject())
    //             .buffer(5) // Process in batches of 5
    //             .subscribe(
    //                 batch -> {
    //                     JsonObject batchResult = JsonObject.create();
    //                     batchResult.put("batchSize", batch.size());
                        
    //                     // Create a proper JsonArray from the batch list
    //                     JsonArray usersArray = JsonArray.create();
    //                     for (JsonObject user : batch) {
    //                         usersArray.add(user);
    //                     }
    //                     batchResult.put("users", usersArray);
    //                     batchResult.put("timestamp", System.currentTimeMillis());
    //                     emitter.emit(batchResult);
    //                 },
    //                 error -> {
    //                     LOG.error("Error processing batched users", error);
    //                     emitter.fail(error);
    //                 },
    //                 () -> emitter.complete()
    //             );
    //     });
    // }
    
    // @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/pipeline")
    // public Uni<JsonObject> processUserPipeline(String pipelineRequest) {
    //     try {
    //         JsonObject request = JsonObject.fromJson(pipelineRequest);
    //         String operation = request.getString("operation");
            
    //         ReactiveBucket bucket = cluster.bucket("default").reactive();
    //         ReactiveCollection collection = bucket.defaultCollection();
            
    //         String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%'";
            
    //         return Uni.createFrom().emitter(emitter -> {
    //             cluster.reactive().query(query)
    //                 .subscribe(
    //                     result -> {
    //                         try {
    //                             List<JsonObject> users = new ArrayList<>();
    //                             result.rowsAsObject().toStream().forEach(users::add);
                                
    //                             // Apply filters based on operation
    //                             List<JsonObject> filteredUsers = users.stream()
    //                                 .filter(user -> {
    //                                     switch (operation) {
    //                                         case "active":
    //                                             return user.containsKey("status") && "active".equals(user.getString("status"));
    //                                         case "admin":
    //                                             return user.containsKey("role") && "admin".equals(user.getString("role"));
    //                                         case "recent":
    //                                             long createdAt = user.getLong("createdAt");
    //                                             return System.currentTimeMillis() - createdAt < 24 * 60 * 60 * 1000; // Last 24 hours
    //                                         default:
    //                                             return true;
    //                                     }
    //                                 })
    //                                 .map(user -> {
    //                                     // Transform user data
    //                                     JsonObject transformed = JsonObject.create();
    //                                     transformed.put("id", user.getString("id"));
    //                                     transformed.put("name", user.getString("name"));
    //                                     transformed.put("email", user.getString("email"));
    //                                     transformed.put("role", user.getString("role"));
    //                                     transformed.put("processedAt", System.currentTimeMillis());
    //                                     return transformed;
    //                                 })
    //                                 .collect(java.util.stream.Collectors.toList());
                                
    //                             // Create a proper JsonArray from the processedUsers list
    //                             JsonArray usersArray = JsonArray.create();
    //                             for (JsonObject user : filteredUsers) {
    //                                 usersArray.add(user);
    //                             }
                                
    //                             emitter.complete(JsonObject.create()
    //                                 .put("success", true)
    //                                 .put("operation", operation)
    //                                 .put("users", usersArray)
    //                                 .put("count", filteredUsers.size())
    //                                 .put("timestamp", System.currentTimeMillis()));
    //                         } catch (Exception e) {
    //                             LOG.error("Error processing query result", e);
    //                             emitter.complete(JsonObject.create()
    //                                 .put("success", false)
    //                                 .put("error", "Error processing query results: " + e.getMessage())
    //                                 .put("errorCode", "PROCESSING_ERROR")
    //                                 .put("timestamp", System.currentTimeMillis()));
    //                         }
    //                     },
    //                     error -> emitter.complete(JsonObject.create()
    //                         .put("success", false)
    //                         .put("error", "Pipeline processing failed: " + error.getMessage())
    //                         .put("errorCode", "PIPELINE_FAILED")
    //                         .put("timestamp", System.currentTimeMillis()))
    //                 );
    //         });
    //     } catch (Exception e) {
    //         return Uni.createFrom().item(JsonObject.create()
    //             .put("success", false)
    //             .put("error", "Pipeline processing failed: " + e.getMessage())
    //             .put("errorCode", "PIPELINE_FAILED")
    //             .put("timestamp", System.currentTimeMillis()));
    //     }
    // }
    
    // @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/transaction")
    // public Uni<JsonObject> createUserWithTransaction(String transactionRequest) {
    //     try {
    //         JsonObject request = JsonObject.fromJson(transactionRequest);
    //         JsonArray users = request.getArray("users");
    //         JsonObject metadata = request.getObject("metadata");
            
    //         ReactiveBucket bucket = cluster.bucket("default").reactive();
    //         ReactiveCollection collection = bucket.defaultCollection();
            
    //         // Create a list of operations to perform in transaction
    //         List<Uni<JsonObject>> operations = new ArrayList<>();
            
    //         for (int i = 0; i < users.size(); i++) {
    //             JsonObject user = users.getObject(i);
    //             String userId = "user-" + System.currentTimeMillis() + "-" + i;
                
    //             user.put("id", userId);
    //             user.put("createdAt", System.currentTimeMillis());
    //             user.put("updatedAt", System.currentTimeMillis());
    //             user.put("transactionId", metadata.getString("transactionId"));
                
    //             Uni<JsonObject> operation = Uni.createFrom().emitter(emitter -> {
    //                 collection.insert(userId, user)
    //                     .subscribe(
    //                         result -> emitter.complete(JsonObject.create()
    //                             .put("userId", userId)
    //                             .put("success", true)
    //                             .put("cas", result.cas())),
    //                         error -> emitter.complete(JsonObject.create()
    //                             .put("userId", userId)
    //                             .put("success", false)
    //                             .put("error", error.getMessage()))
    //                     );
    //             });
                
    //             operations.add(operation);
    //         }
            
    //         // Execute all operations and collect results
    //         return Uni.join().all(operations).andCollectFailures()
    //             .onItem().transform(objects -> {
    //                 List<JsonObject> results = new ArrayList<>();
    //                 int successCount = 0;
    //                 int failureCount = 0;
                    
    //                 for (Object obj : objects) {
    //                     JsonObject result = (JsonObject) obj;
    //                     results.add(result);
    //                     if (result.getBoolean("success")) {
    //                         successCount++;
    //                     } else {
    //                         failureCount++;
    //                     }
    //                 }
                    
    //                 // Check if all operations succeeded
    //                 boolean allSucceeded = failureCount == 0;
                    
    //                 // Create a proper JsonArray from the results list
    //                 JsonArray resultsArray = JsonArray.create();
    //                 for (JsonObject result : results) {
    //                     resultsArray.add(result);
    //                 }
                    
    //                 return JsonObject.create()
    //                     .put("success", allSucceeded)
    //                     .put("transactionId", metadata.getString("transactionId"))
    //                     .put("results", resultsArray)
    //                     .put("timestamp", System.currentTimeMillis());
    //             });
    //     } catch (Exception e) {
    //         return Uni.createFrom().item(JsonObject.create()
    //             .put("success", false)
    //             .put("error", "Transaction failed: " + e.getMessage())
    //             .put("errorCode", "TRANSACTION_FAILED")
    //             .put("timestamp", System.currentTimeMillis()));
    //     }
    // }
    
    // @GET
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/aggregate")
    // public Uni<JsonObject> aggregateUsers() {
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
        
    //     String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%'";
        
    //     return Uni.createFrom().emitter(emitter -> {
    //         cluster.reactive().query(query)
    //             .subscribe(
    //                 result -> {
    //                     try {
    //                         List<JsonObject> users = new ArrayList<>();
    //                         result.rowsAsObject().toStream().forEach(users::add);
                            
    //                         // Group users by role
    //                         Map<String, List<JsonObject>> roleGroups = users.stream()
    //                             .collect(java.util.stream.Collectors.groupingBy(user -> user.getString("role")));
                            
    //                         JsonObject aggregation = JsonObject.create();
    //                         JsonObject roleStats = JsonObject.create();
                            
    //                         for (Map.Entry<String, List<JsonObject>> entry : roleGroups.entrySet()) {
    //                             String role = entry.getKey();
    //                             List<JsonObject> userList = entry.getValue();
                                
    //                             JsonObject roleInfo = JsonObject.create();
    //                             roleInfo.put("count", userList.size());
                                
    //                             // Create a proper JsonArray from the users collection
    //                             JsonArray usersArray = JsonArray.create();
    //                             for (JsonObject user : userList) {
    //                                 usersArray.add(user);
    //                             }
    //                             roleInfo.put("users", usersArray);
                                
    //                             // Calculate average creation time
    //                             long totalTime = userList.stream()
    //                                 .mapToLong(user -> user.getLong("createdAt"))
    //                                 .sum();
    //                             long avgTime = userList.isEmpty() ? 0 : totalTime / userList.size();
    //                             roleInfo.put("averageCreationTime", avgTime);
                                
    //                             roleStats.put(role, roleInfo);
    //                         }
                            
    //                         aggregation.put("totalUsers", roleGroups.values().stream().mapToInt(List::size).sum());
    //                         aggregation.put("roleDistribution", roleStats);
    //                         aggregation.put("timestamp", System.currentTimeMillis());
                            
    //                         emitter.complete(aggregation);
    //                     } catch (Exception e) {
    //                         LOG.error("Error processing query result", e);
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", false)
    //                             .put("error", "Error processing query results: " + e.getMessage())
    //                             .put("errorCode", "PROCESSING_ERROR")
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     }
    //                 },
    //                 error -> {
    //                     LOG.error("Aggregation failed", error);
    //                     emitter.complete(JsonObject.create()
    //                         .put("success", false)
    //                         .put("error", "Aggregation failed: " + error.getMessage())
    //                         .put("errorCode", "AGGREGATION_FAILED")
    //                         .put("timestamp", System.currentTimeMillis()));
    //                 }
    //             );
    //     });
    // }
    
    // @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/validate")
    // public Uni<JsonObject> validateUsers() {
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
        
    //     String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%'";
        
    //     return Uni.createFrom().emitter(emitter -> {
    //         cluster.reactive().query(query)
    //             .subscribe(
    //                 result -> {
    //                     try {
    //                         List<JsonObject> users = new ArrayList<>();
    //                         result.rowsAsObject().toStream().forEach(users::add);
                            
    //                         List<JsonObject> validations = new ArrayList<>();
                            
    //                         for (JsonObject user : users) {
    //                             JsonObject validation = JsonObject.create();
    //                             validation.put("userId", user.getString("id"));
    //                             validation.put("valid", true);
    //                             validation.put("errors", new ArrayList<String>());
                                
    //                             // Validate required fields
    //                             if (!user.containsKey("name") || user.getString("name").trim().isEmpty()) {
    //                                 validation.put("valid", false);
    //                                 validation.getArray("errors").add("Name is required");
    //                             }
                                
    //                             if (!user.containsKey("email") || !isValidEmail(user.getString("email"))) {
    //                                 validation.put("valid", false);
    //                                 validation.getArray("errors").add("Invalid email format");
    //                             }
                                
    //                             if (!user.containsKey("role") || !isValidRole(user.getString("role"))) {
    //                                 validation.put("valid", false);
    //                                 validation.getArray("errors").add("Invalid role");
    //                             }
                                
    //                             validations.add(validation);
    //                         }
                            
    //                         int validCount = 0;
    //                         int invalidCount = 0;
    //                         List<JsonObject> invalidUsers = new ArrayList<>();
                            
    //                         for (JsonObject validation : validations) {
    //                             if (validation.getBoolean("valid")) {
    //                                 validCount++;
    //                             } else {
    //                                 invalidCount++;
    //                                 invalidUsers.add(validation);
    //                             }
    //                         }
                            
    //                         // Create a proper JsonArray from the invalidUsers list
    //                         JsonArray invalidUsersArray = JsonArray.create();
    //                         for (JsonObject invalidUser : invalidUsers) {
    //                             invalidUsersArray.add(invalidUser);
    //                         }
                            
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", true)
    //                             .put("totalUsers", validations.size())
    //                             .put("validCount", validCount)
    //                             .put("invalidCount", invalidCount)
    //                             .put("invalidUsers", invalidUsersArray)
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     } catch (Exception e) {
    //                         LOG.error("Error processing query result", e);
    //                         emitter.complete(JsonObject.create()
    //                             .put("success", false)
    //                             .put("error", "Error processing query results: " + e.getMessage())
    //                             .put("errorCode", "PROCESSING_ERROR")
    //                             .put("timestamp", System.currentTimeMillis()));
    //                     }
    //                 },
    //                 error -> emitter.complete(JsonObject.create()
    //                     .put("success", false)
    //                     .put("error", "Validation failed: " + error.getMessage())
    //                     .put("errorCode", "VALIDATION_FAILED")
    //                     .put("timestamp", System.currentTimeMillis()))
    //             );
    //     });
    // }
    
    // @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/migrate")
    // public Uni<JsonObject> migrateUsers(String migrationRequest) {
    //     try {
    //         JsonObject request = JsonObject.fromJson(migrationRequest);
    //         String sourceRole = request.getString("sourceRole");
    //         String targetRole = request.getString("targetRole");
    //         String reason = request.getString("reason");
            
    //         ReactiveBucket bucket = cluster.bucket("default").reactive();
    //         ReactiveCollection collection = bucket.defaultCollection();
            
    //         String query = "SELECT * FROM `default` WHERE META().id LIKE 'user-%' AND `role` = $role";
    //         JsonObject parameters = JsonObject.create().put("role", sourceRole);
            
    //         return Uni.createFrom().emitter(emitter -> {
    //             cluster.reactive().query(query, QueryOptions.queryOptions().parameters(parameters))
    //                 .subscribe(
    //                     queryResult -> {
    //                         try {
    //                             List<JsonObject> users = new ArrayList<>();
    //                             queryResult.rowsAsObject().toStream().forEach(users::add);
                                
    //                             List<JsonObject> results = new ArrayList<>();
                                
    //                             for (JsonObject user : users) {
    //                                 String userId = user.getString("id");
                                    
    //                                 // Update user role
    //                                 user.put("role", targetRole);
    //                                 user.put("previousRole", sourceRole);
    //                                 user.put("migratedAt", System.currentTimeMillis());
    //                                 user.put("migrationReason", reason);
    //                                 user.put("updatedAt", System.currentTimeMillis());
                                    
    //                                 try {
    //                                     // For now, we'll simulate the replace operation
    //                                     // In a real implementation, you'd need to handle the async replace
    //                                     results.add(JsonObject.create()
    //                                         .put("userId", userId)
    //                                         .put("success", true)
    //                                         .put("oldRole", sourceRole)
    //                                         .put("newRole", targetRole)
    //                                         .put("cas", System.currentTimeMillis()));
    //                                 } catch (Exception e) {
    //                                     results.add(JsonObject.create()
    //                                         .put("userId", userId)
    //                                         .put("success", false)
    //                                         .put("error", e.getMessage()));
    //                                 }
    //                             }
                                
    //                             int successCount = 0;
    //                             int failureCount = 0;
                                
    //                             for (JsonObject result : results) {
    //                                 if (result.getBoolean("success")) {
    //                                     successCount++;
    //                                 } else {
    //                                     failureCount++;
    //                                 }
    //                             }
                                
    //                             // Create a proper JsonArray from the results list
    //                             JsonArray resultsArray = JsonArray.create();
    //                             for (JsonObject result : results) {
    //                                 resultsArray.add(result);
    //                             }
                                
    //                             emitter.complete(JsonObject.create()
    //                                 .put("success", failureCount == 0)
    //                                 .put("migration", JsonObject.create()
    //                                     .put("sourceRole", sourceRole)
    //                                     .put("targetRole", targetRole)
    //                                     .put("reason", reason))
    //                                 .put("results", resultsArray)
    //                                 .put("totalProcessed", results.size())
    //                                 .put("successCount", successCount)
    //                                 .put("failureCount", failureCount)
    //                                 .put("timestamp", System.currentTimeMillis()));
    //                         } catch (Exception e) {
    //                             LOG.error("Error processing query result", e);
    //                             emitter.complete(JsonObject.create()
    //                                 .put("success", false)
    //                                 .put("error", "Error processing query results: " + e.getMessage())
    //                                 .put("errorCode", "PROCESSING_ERROR")
    //                                 .put("timestamp", System.currentTimeMillis()));
    //                         }
    //                     },
    //                     error -> emitter.complete(JsonObject.create()
    //                         .put("success", false)
    //                         .put("error", "Migration failed: " + error.getMessage())
    //                         .put("errorCode", "MIGRATION_FAILED")
    //                         .put("timestamp", System.currentTimeMillis()))
    //                 );
    //         });
    //     } catch (Exception e) {
    //         return Uni.createFrom().item(JsonObject.create()
    //             .put("success", false)
    //             .put("error", "Migration failed: " + e.getMessage())
    //             .put("errorCode", "MIGRATION_FAILED")
    //             .put("timestamp", System.currentTimeMillis()));
    //     }
    // }
    
    // Helper methods for validation
    private boolean isValidEmail(String email) {
        return email != null && email.contains("@") && email.contains(".");
    }
    
    private boolean isValidRole(String role) {
        return role != null && Arrays.asList("admin", "user", "moderator", "developer").contains(role);
    }
}
