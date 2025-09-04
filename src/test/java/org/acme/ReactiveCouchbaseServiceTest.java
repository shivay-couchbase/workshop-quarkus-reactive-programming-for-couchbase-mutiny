package org.acme;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ReactiveCouchbaseServiceTest {

    @Inject
    Cluster cluster;

    private JsonObject testDocument;

    @BeforeEach
    void setUp() {
        testDocument = JsonObject.create()
            .put("author", "Test Author")
            .put("title", "Test Document")
            .put("content", "This is a test document for reactive testing");
    }

    @Test
    void testClusterConnection() {
        // Test that we can connect to the cluster
        assertNotNull(cluster);
        
        // Test that the cluster is accessible
        var bucket = cluster.bucket("default");
        assertNotNull(bucket);
        
        var collection = bucket.defaultCollection();
        assertNotNull(collection);
    }

    @Test
    void testReactiveClusterAccess() {
        // Test that we can get the reactive version of the cluster
        var reactiveCluster = cluster.reactive();
        assertNotNull(reactiveCluster);
        
        var reactiveBucket = cluster.bucket("default").reactive();
        assertNotNull(reactiveBucket);
        
        var reactiveCollection = reactiveBucket.defaultCollection();
        assertNotNull(reactiveCollection);
    }

    @Test
    void testDocumentCreation() {
        var reactiveBucket = cluster.bucket("default").reactive();
        var reactiveCollection = reactiveBucket.defaultCollection();
        
        String key = "test-key-" + System.currentTimeMillis();
        
        Mono<com.couchbase.client.java.kv.MutationResult> upsertResult = 
            reactiveCollection.upsert(key, testDocument);
        
        assertNotNull(upsertResult);
        
        // Test the reactive operation
        var result = upsertResult.block();

        assertNotNull(result.mutationToken());
        assertNotNull(result.cas());

    }

    @Test
    void testDocumentRetrieval() {
        var reactiveBucket = cluster.bucket("default").reactive();
        var reactiveCollection = reactiveBucket.defaultCollection();
        
        String key = "test-retrieve-" + System.currentTimeMillis();
        
        // First upsert a document
        var retrievedDoc = reactiveCollection.upsert(key, testDocument)
            .flatMap(result -> reactiveCollection.get(key)).block();

        assertEquals("Test Author", retrievedDoc.contentAsObject().getString("author"));
        assertEquals("Test Document", retrievedDoc.contentAsObject().getString("title"));
;
    }

    @Test

    void testDocumentDeletion() {
        var reactiveBucket = cluster.bucket("default").reactive();
        var reactiveCollection = reactiveBucket.defaultCollection();
        
        String key = "test-delete-" + System.currentTimeMillis();
        
        // Upsert, then delete, then verify deletion
        assertThrows(DocumentNotFoundException.class, () ->
        reactiveCollection.upsert(key, testDocument)
            .flatMap(r -> reactiveCollection.remove(key))
            .then(reactiveCollection.get(key))

            .block(), "Should throw DocNotFound");
    }

    @Test
    void testDocumentExists() {
        var reactiveBucket = cluster.bucket("default").reactive();
        var reactiveCollection = reactiveBucket.defaultCollection();
        
        String key = "test-exists-" + System.currentTimeMillis();
        
        // Test non-existent document
        Boolean exists = reactiveCollection.exists(key)
            .map(result -> result.exists())
            .block();
        assertFalse(exists);
        
        // Test existing document
        var existsResult = reactiveCollection.upsert(key, testDocument)
            .flatMap(result -> reactiveCollection.exists(key))
            .map(result -> result.exists())
            .block();
        assertTrue(existsResult);
    }

    @Test
    void testClusterDiagnostics() {
        var reactiveCluster = cluster.reactive();
        
        Mono<com.couchbase.client.core.diagnostics.DiagnosticsResult> diagnostics = 
            reactiveCluster.diagnostics();
        
        assertNotNull(diagnostics);
        
        var result = diagnostics.block();

        assertNotNull(result.id());
        assertNotNull(result.endpoints());
        assertNotNull(result.state());


    }

    @Test
    void testBatchOperations() {
        var reactiveBucket = cluster.bucket("default").reactive();
        var reactiveCollection = reactiveBucket.defaultCollection();
        
        JsonObject doc1 = JsonObject.create().put("id", 1).put("name", "Document 1");
        JsonObject doc2 = JsonObject.create().put("id", 2).put("name", "Document 2");
        
        String key1 = "batch-1-" + System.currentTimeMillis();
        String key2 = "batch-2-" + System.currentTimeMillis();
        
        // Test multiple operations
        Mono<com.couchbase.client.java.kv.MutationResult> op1 = 
            reactiveCollection.upsert(key1, doc1);
        Mono<com.couchbase.client.java.kv.MutationResult> op2 = 
            reactiveCollection.upsert(key2, doc2);
        
        // Combine operations
        var tuple = Mono.zip(op1, op2)
            .block();
        assertNotNull(tuple.getT1().mutationToken());
        assertNotNull(tuple.getT2().mutationToken());

    }

    @Test
    void testErrorHandling() {
        var reactiveBucket = cluster.bucket("default").reactive();
        var reactiveCollection = reactiveBucket.defaultCollection();
        
        // Test with invalid key (empty string) - this should fail
        Mono<com.couchbase.client.java.kv.MutationResult> invalidUpsert = 
            reactiveCollection.upsert("", testDocument);
        
        assertThrows(Exception.class, () -> {
            invalidUpsert.block();
        });
    }
}
