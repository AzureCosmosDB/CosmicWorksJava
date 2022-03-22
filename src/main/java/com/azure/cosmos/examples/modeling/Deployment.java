package com.azure.cosmos.examples.modeling;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.examples.changefeed.ChangeFeedConfigurations;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import reactor.core.publisher.Flux;

public class Deployment {

    public void CreateDatabase(CosmosClient cosmosDBClient, int schemaVersion) {
        {
            int schemaVersionStart;
            int schemaVersionEnd;
            if (schemaVersion == 0) {
                schemaVersionStart = schemaVersion;
                schemaVersionEnd = schemaVersion;
            } else {
                schemaVersionStart = 1;
                schemaVersionEnd = 4;
            }

            for (int schemaVersionCounter = schemaVersionStart; schemaVersionCounter <= schemaVersionEnd; schemaVersionCounter++) {
                System.out.println("create started for schema " + schemaVersionCounter);
                CreateDatabaseAndContainers(cosmosDBClient, "database-v" + schemaVersionCounter, schemaVersionCounter);
            }
        }
    }

    public List<List<SchemaDetails>> getSchemaDetails() {

        List<List<SchemaDetails>> DatabaseSchema = new ArrayList<>();

        List<SchemaDetails> DatabaseSchema_1 = new ArrayList<>();
        DatabaseSchema_1.add(new SchemaDetails("customer", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("customerAddress", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("customerPassword", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("product", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("productCategory", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("productTag", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("productTags", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("salesOrder", "/id"));
        DatabaseSchema_1.add(new SchemaDetails("salesOrderDetail", "/id"));

        List<SchemaDetails> DatabaseSchema_2 = new ArrayList<>();
        DatabaseSchema_2.add(new SchemaDetails("customer", "/id"));
        DatabaseSchema_2.add(new SchemaDetails("product", "/categoryId"));
        DatabaseSchema_2.add(new SchemaDetails("productCategory", "/type"));
        DatabaseSchema_2.add(new SchemaDetails("productTag", "/type"));
        DatabaseSchema_2.add(new SchemaDetails("salesOrder", "/customerId"));

        List<SchemaDetails> DatabaseSchema_3 = new ArrayList<>();
        DatabaseSchema_3.add(new SchemaDetails("leases", "/id"));
        DatabaseSchema_3.add(new SchemaDetails("customer", "/id"));
        DatabaseSchema_3.add(new SchemaDetails("product", "/categoryId"));
        DatabaseSchema_3.add(new SchemaDetails("productCategory", "/type"));
        DatabaseSchema_3.add(new SchemaDetails("productTag", "/type"));
        DatabaseSchema_3.add(new SchemaDetails("salesOrder", "/customerId"));

        List<SchemaDetails> DatabaseSchema_4 = new ArrayList<>();
        DatabaseSchema_4.add(new SchemaDetails("customer", "/customerId"));
        DatabaseSchema_4.add(new SchemaDetails("product", "/categoryId"));
        DatabaseSchema_4.add(new SchemaDetails("productMeta", "/type"));
        DatabaseSchema_4.add(new SchemaDetails("salesByCategory", "/categoryId"));

        DatabaseSchema.add(DatabaseSchema_1);
        DatabaseSchema.add(DatabaseSchema_2);
        DatabaseSchema.add(DatabaseSchema_3);
        DatabaseSchema.add(DatabaseSchema_4);

        return DatabaseSchema;

    }

    public void CreateDatabaseAndContainers(CosmosClient cosmosDBClient, String database, int schema) {

        System.out.println("creating database and containers for schema v" + schema);
        System.out.println("DatabaseName:" + database + " key:provided");

        List<List<SchemaDetails>> DatabaseSchema = getSchemaDetails();
        if (schema >= 1 & schema <= 4) {
            ThroughputProperties throughputProperties = ThroughputProperties.createAutoscaledThroughput(4000);
            CosmosDatabaseResponse cosmosDatabaseResponse = cosmosDBClient.createDatabaseIfNotExists(database,
                    throughputProperties);
            CosmosDatabase cosmosDatabase = cosmosDBClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());
            for (SchemaDetails schemaDetails : DatabaseSchema.get(schema - 1)) {
                CosmosContainerProperties autoScaleContainerProperties = new CosmosContainerProperties(
                        schemaDetails.getContainerName(), schemaDetails.getPk());
                CosmosContainerResponse databaseResponse = cosmosDatabase.createContainer(autoScaleContainerProperties,
                        throughputProperties,
                        new CosmosContainerRequestOptions());
                CosmosContainer container = cosmosDatabase.getContainer(databaseResponse.getProperties().getId());
                System.out.println("container: " + cosmosDatabase.getId() + "." + container.getId() + " created!");
            }
        }
    }

    public void DeleteDatabases(CosmosClient cosmosDBClient, int schemaVersion) {
        {
            int schemaVersionStart;
            int schemaVersionEnd;
            if (schemaVersion == 0) {
                schemaVersionStart = schemaVersion;
                schemaVersionEnd = schemaVersion;
            } else {
                schemaVersionStart = 1;
                schemaVersionEnd = 4;
            }
            try (Scanner in = new Scanner(System.in)) {
                System.out.println("Are you sure you want to delete all the databases? y/n");
                String input = in.nextLine();
                if (input.equals("y")) {
                    for (int schemaVersionCounter = schemaVersionStart; schemaVersionCounter <= schemaVersionEnd; schemaVersionCounter++) {
                        System.out.println("delete started for schema " + schemaVersionCounter);
                        DeleteDatabasesAndContainers(cosmosDBClient, "database-v" + schemaVersionCounter, schemaVersionCounter);
                    }
                    System.out.println("Databases deleted, exiting program.");
                    System.exit(0);
                }
                else {
                    System.out.println("Ok, delete aborted");
                }                       
            }

        }
    }
    public void DeleteDatabasesAndContainers(CosmosClient cosmosDBClient, String database, int schema) {
        System.out.println("creating database and containers for schema v" + schema);
        System.out.println("DatabaseName:" + database + " key:provided");
        cosmosDBClient.getDatabase(database).delete(new CosmosDatabaseRequestOptions());
    }    

    public void LoadDatabase() {
        {
            final ExecutorService es = Executors.newCachedThreadPool();
            int[] schemaVersions = {1,2,3,4};
            for (int v : schemaVersions) {
                final Runnable task = () -> LoadContainersFromFolder(v, "cosmic-works-v" + v,
                        "database-v" + v);
                es.execute(task);
            }
            es.shutdown();

            try {
                final boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
                if (finished) {
                    System.out.println("finished loading all data!!");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static CosmosAsyncClient getCosmosClient() {

        return new CosmosClientBuilder()
                .endpoint(ChangeFeedConfigurations.HOST)
                .key(ChangeFeedConfigurations.MASTER_KEY)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    public void LoadContainersFromFolder(int schemaVersion, String SourceDatabaseName,
                                         String TargetDatabaseName) {
        List<List<SchemaDetails>> DatabaseSchema = getSchemaDetails();    
        CosmosAsyncClient clientAsync = getCosmosClient();
        CosmosAsyncDatabase database = clientAsync.getDatabase(TargetDatabaseName);
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        String folder = s + "/src/main/java/com/azure/cosmos/examples/data" + "/" + SourceDatabaseName + "/";
        folder = folder.replace("\\", "/");
        System.out.println("folder: " + folder);
        System.out.println("Preparing to load containers and data for " + TargetDatabaseName + "....");
        File path = new java.io.File(folder);
        File[] listOfFiles = path.listFiles();
        final ExecutorService es = Executors.newCachedThreadPool();

        assert listOfFiles != null;
        for (File file : listOfFiles) {
            final Runnable task = () -> {
                System.out.println("new container thread...");
                if (file.isFile()) {
                    String pk = "";
                    System.out.println("loading data for container: " + file.getName()+" in database "+TargetDatabaseName);
                    System.out.println("schemaVersion: "+schemaVersion);
                    List<SchemaDetails> schemaDetails = DatabaseSchema.get(schemaVersion -1);
                    for (SchemaDetails schema : schemaDetails) {
                        if (file.getName().equals(schema.getContainerName())) {
                            pk = schema.getPk().substring(1);
                            try {
                                Thread.sleep(0);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    Scanner sc;
                    try {
                        sc = new Scanner(file);
                        sc.useDelimiter("\\Z");
                        String JsonArrayString = sc.next();
                        JSONArray jsonArray = new JSONArray(JsonArrayString);
                        List<JsonNode> docList = new ArrayList<>();
                        for (Object doc : jsonArray) {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode actualObj = mapper.readTree(doc.toString());
                            docList.add(actualObj);
                            Thread.sleep(0);
                        }
                        Flux<JsonNode> docsToInsert = Flux.fromIterable(docList);
                        CosmosAsyncContainer productCategoryContainer = database.getContainer(file.getName());
                        bulkCreateGeneric(docsToInsert, productCategoryContainer, pk);
                        sc.close();
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("finished loading data for container: " + file.getName());
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            es.execute(task);
        }
        es.shutdown();

        try {
            final boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
            if (finished) {
                System.out.println("finished loading all data for database: " + TargetDatabaseName);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void bulkCreateGeneric(Flux<JsonNode> items, CosmosAsyncContainer cosmosDBClient, String pk) {
        Flux<CosmosItemOperation> cosmosItemOperations = items
                .map(item -> CosmosBulkOperations.getCreateItemOperation(item,
                        new PartitionKey(item.get(pk).asText())));
        cosmosDBClient.executeBulkOperations(cosmosItemOperations).blockLast();
    }

    public static class SchemaDetails {
        public SchemaDetails(String containerName, String Pk) {
            setContainerName(containerName);
            setPk(Pk);
        }

        public void setContainerName(String containerName) {
            this.ContainerName = containerName;
        }

        public void setPk(String pk) {
            this.Pk = pk;
        }

        public String getContainerName() {
            return ContainerName;
        }

        public String getPk() {
            return Pk;
        }

        public String ContainerName;
        public String Pk;
    }

}
