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
    
    // Helper methods for validation
    private boolean isValidEmail(String email) {
        return email != null && email.contains("@") && email.contains(".");
    }
    
    private boolean isValidRole(String role) {
        return role != null && Arrays.asList("admin", "user", "moderator", "developer").contains(role);
    }
}
