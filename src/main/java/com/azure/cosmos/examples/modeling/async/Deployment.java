// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.modeling.async;

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
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.examples.changefeed.ChangeFeedConfigurations;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Deployment {

    private static Logger logger = LoggerFactory.getLogger(Deployment.class);
    private CosmosAsyncDatabase database;

    public void createDatabase(CosmosAsyncClient cosmosDBClient, int schemaVersion) {
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
                logger.info("create started for schema " + schemaVersionCounter);
                createDatabaseAndContainers(cosmosDBClient, "database-v" + schemaVersionCounter, schemaVersionCounter);
            }
        }
    }

    public List<List<SchemaDetails>> getSchemaDetails() {

        List<List<SchemaDetails>> databaseSchema = new ArrayList<>();

        List<SchemaDetails> databaseSchema_1 = new ArrayList<>();
        databaseSchema_1.add(new SchemaDetails("customer", "/id"));
        databaseSchema_1.add(new SchemaDetails("customerAddress", "/id"));
        databaseSchema_1.add(new SchemaDetails("customerPassword", "/id"));
        databaseSchema_1.add(new SchemaDetails("product", "/id"));
        databaseSchema_1.add(new SchemaDetails("productCategory", "/id"));
        databaseSchema_1.add(new SchemaDetails("productTag", "/id"));
        databaseSchema_1.add(new SchemaDetails("productTags", "/id"));
        databaseSchema_1.add(new SchemaDetails("salesOrder", "/id"));
        databaseSchema_1.add(new SchemaDetails("salesOrderDetail", "/id"));

        List<SchemaDetails> databaseSchema_2 = new ArrayList<>();
        databaseSchema_2.add(new SchemaDetails("customer", "/id"));
        databaseSchema_2.add(new SchemaDetails("product", "/categoryId"));
        databaseSchema_2.add(new SchemaDetails("productCategory", "/type"));
        databaseSchema_2.add(new SchemaDetails("productTag", "/type"));
        databaseSchema_2.add(new SchemaDetails("salesOrder", "/customerId"));

        List<SchemaDetails> databaseSchema_3 = new ArrayList<>();
        databaseSchema_3.add(new SchemaDetails("leases", "/id"));
        databaseSchema_3.add(new SchemaDetails("customer", "/id"));
        databaseSchema_3.add(new SchemaDetails("product", "/categoryId"));
        databaseSchema_3.add(new SchemaDetails("productCategory", "/type"));
        databaseSchema_3.add(new SchemaDetails("productTag", "/type"));
        databaseSchema_3.add(new SchemaDetails("salesOrder", "/customerId"));

        List<SchemaDetails> databaseSchema_4 = new ArrayList<>();
        databaseSchema_4.add(new SchemaDetails("customer", "/customerId"));
        databaseSchema_4.add(new SchemaDetails("product", "/categoryId"));
        databaseSchema_4.add(new SchemaDetails("productMeta", "/type"));
        databaseSchema_4.add(new SchemaDetails("salesByCategory", "/categoryId"));

        databaseSchema.add(databaseSchema_1);
        databaseSchema.add(databaseSchema_2);
        databaseSchema.add(databaseSchema_3);
        databaseSchema.add(databaseSchema_4);

        return databaseSchema;

    }

    public void createDatabaseAndContainers(CosmosAsyncClient cosmosDBClient, String databaseName, int schema) {

        logger.info("creating database and containers for schema v" + schema);
        logger.info("DatabaseName:" + database + " key:provided");

        List<List<SchemaDetails>> DatabaseSchema = getSchemaDetails();
        if (schema >= 1 & schema <= 4) {
            ThroughputProperties throughputProperties = ThroughputProperties.createAutoscaledThroughput(4000);

            Mono<CosmosDatabaseResponse> databaseIfNotExists = cosmosDBClient.createDatabaseIfNotExists(databaseName);
            databaseIfNotExists.flatMap(databaseResponse -> {
                database = cosmosDBClient.getDatabase(databaseResponse.getProperties().getId());
                logger.info("Checking database " + database.getId() + " completed!\n");
                return Mono.empty();
            }).block();

            for (SchemaDetails schemaDetails : DatabaseSchema.get(schema - 1)) {
                CosmosContainerProperties autoScaleContainerProperties = new CosmosContainerProperties(
                        schemaDetails.getContainerName(), schemaDetails.getPk());
                Mono<CosmosContainerResponse> containerIfNotExists = database.createContainerIfNotExists(autoScaleContainerProperties, throughputProperties);

                //  Create autoscale container with 4000 RU/s
                CosmosContainerResponse cosmosContainerResponse = containerIfNotExists.block();
                CosmosAsyncContainer container = database.getContainer(cosmosContainerResponse.getProperties().getId());
                logger.info("container: " + cosmosContainerResponse.getProperties().getId() + "." + container.getId() + " created!");
            }
        }
    }

    public void deleteDatabases(CosmosAsyncClient cosmosDBClient, int schemaVersion) {
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
                logger.info("Are you sure you want to delete all the databases? y/n");
                String input = in.nextLine();
                if (input.equals("y")) {
                    for (int schemaVersionCounter = schemaVersionStart; schemaVersionCounter <= schemaVersionEnd; schemaVersionCounter++) {
                        logger.info("delete started for schema " + schemaVersionCounter);
                        deleteDatabasesAndContainers(cosmosDBClient, "database-v" + schemaVersionCounter, schemaVersionCounter);
                    }
                    logger.info("Databases deleted, exiting program.");
                    System.exit(0);
                }
                else {
                    logger.info("Ok, delete aborted");
                }
            }

        }
    }
    public void deleteDatabasesAndContainers(CosmosAsyncClient cosmosDBClient, String database, int schema) {
        logger.info("creating database and containers for schema v" + schema);
        logger.info("DatabaseName:" + database + " key:provided");
        cosmosDBClient.getDatabase(database).delete(new CosmosDatabaseRequestOptions()).block();
    }

    public void loadDatabase() {
        {
            final ExecutorService es = Executors.newCachedThreadPool();
            int[] schemaVersions = {1,2,3,4};
            for (int v : schemaVersions) {
                final Runnable task = () -> loadContainersFromFolder(v, "cosmic-works-v" + v,
                        "database-v" + v);
                es.execute(task);
            }
            es.shutdown();

            try {
                final boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
                if (finished) {
                    logger.info("finished loading all data!!");
                }
            } catch (InterruptedException e) {
                logger.info("Exception: " + e);
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

    public void loadContainersFromFolder(int schemaVersion, String sourceDatabaseName,
                                         String targetDatabaseName) {
        List<List<SchemaDetails>> DatabaseSchema = getSchemaDetails();
        CosmosAsyncClient clientAsync = getCosmosClient();
        CosmosAsyncDatabase database = clientAsync.getDatabase(targetDatabaseName);
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        String folder = s + "/src/main/java/com/azure/cosmos/examples/data" + "/" + sourceDatabaseName + "/";
        folder = folder.replace("\\", "/");
        logger.info("folder: " + folder);
        logger.info("Preparing to load containers and data for " + targetDatabaseName + "....");
        File path = new java.io.File(folder);
        File[] listOfFiles = path.listFiles();
        final ExecutorService es = Executors.newCachedThreadPool();

        assert listOfFiles != null;
        for (File file : listOfFiles) {
            final Runnable task = () -> {
                logger.info("new container thread...");
                if (file.isFile()) {
                    String pk = "";
                    logger.info("loading data for container: " + file.getName()+" in database "+targetDatabaseName);
                    logger.info("schemaVersion: "+schemaVersion);
                    List<SchemaDetails> schemaDetails = DatabaseSchema.get(schemaVersion -1);
                    for (SchemaDetails schema : schemaDetails) {
                        if (file.getName().equals(schema.getContainerName())) {
                            pk = schema.getPk().substring(1);
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
                        }
                        Flux<JsonNode> docsToInsert = Flux.fromIterable(docList);
                        CosmosAsyncContainer productCategoryContainer = database.getContainer(file.getName());
                        bulkCreateGeneric(docsToInsert, productCategoryContainer, pk);
                        sc.close();
                    } catch (IOException e) {
                        logger.info("Exception: " + e);
                    }
                    logger.info("finished loading data for container: " + file.getName());
                }
            };
            es.execute(task);
        }
        es.shutdown();

        try {
            final boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);
            if (finished) {
                logger.info("finished loading all data for database: " + targetDatabaseName);
            }
        } catch (InterruptedException e) {
            logger.info("Exception: " + e);
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
