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
import io.smallrye.mutiny.converters.multi.FromFlux;
import java.util.concurrent.CompletionStage;
import reactor.core.publisher.Mono;
import java.util.concurrent.TimeoutException;
import java.lang.IllegalArgumentException;

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
        ReactiveCluster reactiveCluster = cluster.reactive();

        Uni<PingResult> uni = FromMono.INSTANCE.from(reactiveCluster.ping());
        return uni.onItem().transform(pr -> "Couchbase cluster is healthy!" + pr.exportToJson())
                .onFailure().recoverWithItem(t -> "Cluster health check failed: " + t.getMessage());
    }
 
@GET
@Produces(MediaType.TEXT_PLAIN)
@Path("simpleUpsert")
    public Uni<String> simpleUpsert() {
        ReactiveBucket bucket = cluster.bucket("default").reactive();
        ReactiveCollection collection = bucket.defaultCollection();
        Uni<JsonObject> u = Uni.createFrom().item(() ->
                JsonObject.create()
            .put("author", "mike2")
            .put("title", "My Blog Post 2r4")
                        .put("timestamp", System.currentTimeMillis()));
        Uni<MutationResult> u1 = u.flatMap(jo -> FromMono.INSTANCE.from(collection.upsert("document-key",jo)));
        return u1.onItem().transform(result -> "Document upserted successfully! Mutation token: " + result.mutationToken())
                 .onFailure().recoverWithItem(error -> "Document upsert failed: " + error.getMessage());
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("reactiveUpsert")
    public Uni<String> reactiveUpsert(String documentJson) {

            ReactiveBucket bucket = cluster.bucket("default").reactive();
            ReactiveCollection collection = bucket.defaultCollection();
            String key = "doc-" + System.currentTimeMillis();
            
        Uni<JsonObject> u = Uni.createFrom().item(() -> {
            // Add metadata
            return JsonObject.fromJson(documentJson)
                    .put("createdAt", System.currentTimeMillis())
                    .put("documentId", key);

        }).onFailure().invoke(err -> LOG.error("Error occurred while parsing JSON string", err));
        Uni<MutationResult> u1 = u.flatMap(jo -> FromMono.INSTANCE.from(collection.upsert("document-key",jo)));
        return u1.onItem().transform(result -> "Document upserted successfully - Key: " + key + ", Mutation token: " + result.mutationToken() + ", CAS: " + result.cas())
                .onFailure().recoverWithItem(error -> "Document upsert failed: " + error.getMessage());
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
        AsyncCluster asyncCluster = cluster.async();
        return Uni.createFrom().completionStage(asyncCluster.diagnostics())
                .onItem().transform(result -> "Cluster diagnostics retrieved successfully: " + result.exportToJson())
                .onFailure().recoverWithItem(error -> "Failed to get cluster info: " + error.getMessage());
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("document/{key}")
    public Uni<String> getDocument(@PathParam("key") String key) {
        ReactiveBucket bucket = cluster.bucket("default").reactive();
        ReactiveCollection collection = bucket.defaultCollection();

        Uni<GetResult> resultUni = FromMono.INSTANCE.from(collection.get(key)).onFailure(DocumentNotFoundException.class).recoverWithItem("The Document was not found");
        return resultUni.onItem().transform(result -> "Document found - Key: " + key + ", CAS: " + result.cas() + ", Content: " + result.contentAsObject())
                .onFailure().invoke(err -> LOG.error("And unkown error occured: " + err.getMessage()));
    }

    @DELETE
    @Path("document/{key}")
    public Uni<JsonObject> deleteDocument(@PathParam("key") String key) {
        ReactiveBucket bucket = cluster.bucket("default").reactive();
        ReactiveCollection collection = bucket.defaultCollection();

        Uni<MutationResult> resultUni = FromMono.INSTANCE.from(collection.remove(key));
        return resultUni.map(mr -> JsonObject.create()
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


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("query")
    public Multi<JsonObject> executeQuery(String queryRequestJson) {
        ReactiveCluster reactiveCluster = cluster.reactive();
       Uni<ReactiveQueryResult> results = Uni.createFrom().item(() -> {
            JsonObject queryRequest = JsonObject.fromJson(queryRequestJson);
            String query = queryRequest.getString("query");
            JsonObject parameters = queryRequest.getObject("parameters");
            if (parameters == null) {
                parameters = JsonObject.create();
            }
            return reactiveCluster.query(query, QueryOptions.queryOptions().parameters(parameters));
        }).flatMap( mono -> FromMono.INSTANCE.from(mono));
      Multi docs = results.onItem().transformToMulti(reactiveQueryResult -> FromFlux.INSTANCE.from(reactiveQueryResult.rowsAsObject()));
      return docs;
    }
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("document/{key}/exists")
    public Uni<String> checkDocumentExists(@PathParam("key") String key) {
        ReactiveBucket bucket = cluster.bucket("default").reactive();
        ReactiveCollection collection = bucket.defaultCollection();
        Uni<ExistsResult> exists = FromMono.INSTANCE.from(collection.exists(key));
        return exists.map(r -> "Document exists check - Key: " + key + ", Exists: " + r.exists())
                .onFailure().recoverWithItem(error ->"Document existence check failed: " + error.getMessage()
                );
    }
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("demo")
    public Uni<String> demonstrateReactivePatterns(String demoRequestJson) {
        try {
            // This demonstrates combining multiple reactive operations
            ReactiveCluster reactiveCluster = cluster.reactive();
            ReactiveBucket bucket = cluster.bucket("default").reactive();
            ReactiveCollection collection = bucket.defaultCollection();
            
            return Uni.createFrom().emitter(emitter -> {
                reactiveCluster.diagnostics()
                    .subscribe(
                        info -> {
                    JsonObject demoDoc = JsonObject.create()
                        .put("type", "demo")
                        .put("clusterId", info.id())
                        .put("createdAt", System.currentTimeMillis());
                    
                            collection.upsert("demo-doc", demoDoc)
                                .subscribe(
                                    result -> emitter.complete("Reactive pattern demonstration completed - Mutation token: " + result.mutationToken()),
                                    error -> emitter.complete("Demo operation failed: " + error.getMessage())
                                );
                        },
                        error -> emitter.complete("Demo operation failed: " + error.getMessage())
                    );
                });
        } catch (Exception e) {
            LOG.error("Failed to process demo request", e);
            return Uni.createFrom().item("Demo operation failed: " + e.getMessage());
        }
    }
    // ===== EXERCISE 3: CRUD Operations as Reactive Endpoints =====
    
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
        Uni<MutationResult> result = uniUser.flatMap(u -> FromMono.INSTANCE.from(collection.insert(userId, u)));;
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
        Uni<GetResult> uniUser = FromMono.INSTANCE.from(
                 collection.get(userId)
        );
       Uni<MutationResult> uniMutRes = Uni.combine().all().unis(uniUser, uniUpdates )
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
    
    //  @POST
    // @Consumes(MediaType.APPLICATION_JSON)
    // @Produces(MediaType.APPLICATION_JSON)
    // @Path("users/batch")
    // public Uni<JsonObject> batchCreateUsers(String usersJson) {
    //     Multi objects = Uni.createFrom().item(() ->
    //                     JsonArray.fromJson(usersJson))
    //             .onFailure(InvalidArgumentException.class).invoke(err -> LOG.error("Given argument is invalid JSON"))
    //             .onItem().transformToMulti(array -> Multi.createFrom().iterable(array.toList()));
    //     Multi<JsonObject> multiUsers = objects.map( obj -> {
    //                 JsonObject user = (JsonObject) obj;
    //                 String userId = "user-" + System.currentTimeMillis() ;
    //                 user.put("id", userId);
    //                 user.put("createdAt", System.currentTimeMillis());
    //                 user.put("updatedAt", System.currentTimeMillis());
    //                 return user;
    //             }
    //     ).onFailure(ClassCastException.class).invoke(error -> LOG.error("Unsafe cast"));
    //     ReactiveBucket bucket = cluster.bucket("default").reactive();
    //     ReactiveCollection collection = bucket.defaultCollection();
    //     Multi<MutationResult> multiInsertResults = multiUsers.flatMap(u -> FromMono.INSTANCE.from(collection.insert(u.getString("id"), u)).toMulti());
    //     Multi<JsonObject> multiJOS = multiInsertResults.map( mr -> JsonObject.create()
    //                     .put("success", true)
    //                     .put("cas", mr.cas()))
    //             .onFailure().recoverWithItem(err -> JsonObject.create()
    //                     .put("success", false)
    //                     .put("error", err.getMessage()));
    //     return multiJOS.collect().asList().map(list -> JsonArray.from(list))
    //             .map(resultsArray -> JsonObject.create()
    //                     .put("success", true)
    //                     .put("results", resultsArray)
    //                     .put("totalProcessed", resultsArray.size())
    //                     .put("timestamp", System.currentTimeMillis()))
    //             .onFailure().recoverWithItem(e -> JsonObject.create()
    //                     .put("success", false)
    //                     .put("error", "Invalid JSON format: " + e.getMessage()));
    // }

    // ===== EXERCISE 5: Error Handling and Retry Strategies =====
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{userId}/resilient")
    public Uni<JsonObject> getUserResilient(@PathParam("userId") String userId) {
        ReactiveBucket bucket = cluster.bucket("default").reactive();
        ReactiveCollection collection = bucket.defaultCollection();
        Uni<GetResult> uniReplicaUser = FromMono.INSTANCE.from(collection.getAnyReplica(userId));
        Uni<GetResult> uniUser = FromMono.INSTANCE.from(collection.get(userId))
                .onFailure(DocumentNotFoundException.class).recoverWithItem(uniReplicaUser);
        return uniUser.map(gr -> JsonObject.create()
                    .put("success", true)
                .put("user", gr.contentAsObject())
                .put("cas", gr.cas())
                .put("timestamp", System.currentTimeMillis()))
                .onFailure(TimeoutException.class).retry().atMost(5)
                .onFailure(TimeoutException.class).recoverWithItem(err -> JsonObject.create()
                        .put("success", false)
                        .put("error", "Operation timed out")
                        .put("errorCode", "TIMEOUT")
                        .put("userId", userId)
                        .put("timestamp", System.currentTimeMillis()))
                .onFailure().recoverWithItem(err -> JsonObject.create()
                        .put("success", false)
                        .put("error", "Internal server error")
                        .put("errorCode", "INTERNAL_ERROR")
                        .put("userId", userId)
                        .put("timestamp", System.currentTimeMillis()));
    }
    
@GET
@Produces(MediaType.TEXT_PLAIN)
@Path("service/bad")
public Uni<String> badServiceTest() {
    return reallyBadService();
}
@GET
@Produces(MediaType.TEXT_PLAIN)
@Path("service/good")
public Uni<String> goodServiceTest() {
    return saferBadService();
}

private Uni<String> saferBadService(){
    return reallyBadService()
            .onFailure(UnhappyServiceException.class).invoke(err -> LOG.error("Something went wrong: " + err.getMessage()))
            .onFailure(UnhappyServiceException.class).retry()
            .until(failure -> {
                return (failure instanceof UnhappyServiceException);
            });
}

private Uni<String> reallyBadService(){
    return Uni.createFrom().item(() -> {
        if (Math.random() < 0.1) {
            return "it worked";
        } else {
            throw new UnhappyServiceException("Unhappy Service");
        }
    });
}

private class UnhappyServiceException extends RuntimeException {
    public UnhappyServiceException(String message) {
        super(message);
    }
}
    
    // Helper methods for validation
    private boolean isValidEmail(String email) {
        return email != null && email.contains("@") && email.contains(".");
    }
    
    private boolean isValidRole(String role) {
        return role != null && Arrays.asList("admin", "user", "moderator", "developer").contains(role);
    }
}
