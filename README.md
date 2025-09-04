# Workshop - Reactive Programming for Databases with Couchbase, Quarkus & Mutiny


## Docker Pull / Podman pull

```
docker run -d --name couchbase-server -p 8091-8096:8091-8096 -p 11207:11207 -p 11210:11210 -p 18091-18096:18091-18096 couchbase/server:latest
```

Install Quarkus wiht Couchbase
```
https://code.quarkus.io/?e=io.quarkiverse.couchbase%3Aquarkus-couchbase
```

# Get Started with Couchbase Quarkus SDK
1. Create a new application
We recommend creating a Quarkus app with the Couchbase Extension via code.quarkus.io. The link will automatically add the Couchbase and REST Quarkus extensions and generate a new sample application.

If you already have an application on hand, add Couchbase as a dependency:

## 1.1. Add the Dependency

Maven
```
<dependency>
    <groupId>io.quarkiverse.couchbase</groupId>
    <artifactId>quarkus-couchbase</artifactId>
    <version>1.0.0</version>
</dependency>
Gradle
```
Gradle
```
dependencies {
     implementation 'io.quarkiverse.couchbase:quarkus-couchbase:1.0.0'
}

```

## 2. Configure Your Application
Add your connection string and credentials in application.properties located at src/main/resources/application.properties:
```
quarkus.couchbase.connection-string=couchbase://localhost
quarkus.couchbase.username=Administrator
quarkus.couchbase.password=password
```
The extension automatically starts a TestContainer, which can be disabled if desired with:
```
quarkus.devservices.enabled=false
```
Remember to head to the Couchbase Cluster UI at http://localhost:8091 and create a Bucket named default if you’re using DevServices.

## 3. Injecting the Cluster
The Quarkus Couchbase extension produces a Cluster bean that can be injected using the  @Inject annotation.
```
import jakarta.inject.Inject;
import com.couchbase.client.java.Cluster;
 
@Inject
Cluster cluster;
```
From there, its usage is the same as it would be with the normal Java SDK.


## 4. Example: Creating an HTTP GET Endpoint
Modify the code in src/main/java/org/acme/GreetingResource.java:
```
package org.acme;
 
 
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
 
import com.couchbase.client.java.Cluster;
 
@ApplicationScoped
@Path("couchbase")
public class GreetingResource {
     @Inject
     Cluster cluster;
     
     @GET
     @Produces(MediaType.TEXT_PLAIN)
     @Path("simpleQuery")
     public String simpleQuery() {
          var query = cluster.query("SELECT RAW 'hello world' AS greeting");
          return query.rowsAs(String.class).get(0);
     }
}
```
## 5. Example: Performing KV Operations
Use the same KV API as in the normal Java SDK:
```
package org.acme;
 
 
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
 
@ApplicationScoped
@Path("couchbase")
public class GreetingResource {
 
@Inject
Cluster cluster;
 
@GET
@Produces(MediaType.TEXT_PLAIN)
@Path("simpleUpsert")
public String simpleUpsert() {
    var bucket = cluster.bucket("default");
    var collection = bucket.defaultCollection();
 
    JsonObject content = JsonObject.create()
        .put("author", "mike")
        .put("title", "My Blog Post 1");
 
    MutationResult result = collection.upsert("document-key", content);
 
    return result.mutationToken().toString();
  }
}
```

## Running your application
Run in dev mode with:
```
mvn quarkus:dev
```
And head to the Developer UI at http://localhost:8080/q/dev-ui/welcome.

Or compile to a native executable:
```
mvn clean install -Dnative -Dmaven.test.skip
```
The native-image will be located in the target directory of your module.

## Adding Mutiny Dependency for Quarkus

```
<dependency>
    <groupId>io.smallrye.reactive</groupId>
    <artifactId>mutiny-reactor</artifactId>
</dependency>
```
