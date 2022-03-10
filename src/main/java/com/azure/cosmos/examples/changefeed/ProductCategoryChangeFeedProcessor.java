// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.examples.changefeed;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.CustomPOJO;
import com.azure.cosmos.examples.models.Models.Product;
import com.azure.cosmos.examples.models.Models.ProductCategory;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import java.util.ArrayList;
import java.util.List;

/**
 * Sample for Change Feed Processor of Product Category in CosmicWorksJava.
 */
public class ProductCategoryChangeFeedProcessor {

    public static int WAIT_FOR_WORK = 60000;
    public static final String DATABASE_NAME = "database-v3";
    public static final String COLLECTION_NAME = "productCategory";
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(ProductCategoryChangeFeedProcessor.class);
    private static ChangeFeedProcessor changeFeedProcessorInstance;
    static CosmosAsyncContainer productContainer;

    public static void main(String[] args) {
        logger.info("BEGIN Sample");

        try {

            logger.info("-->CREATE DocumentClient");
            CosmosAsyncClient client = getCosmosClient();
            CosmosAsyncDatabase database = client.getDatabase("database-v3");
            CosmosAsyncContainer productCategoryContainer = database.getContainer("productCategory");
            productContainer = database.getContainer("product");
            CosmosAsyncContainer leaseContainer = database.getContainer("leases");
            clearScreen();
            logger.info("-->START Change Feed Processor on worker (handles changes asynchronously)");
            changeFeedProcessorInstance = getChangeFeedProcessor("Java_CosmicWorks_Host_1", productCategoryContainer,
                    leaseContainer);
            changeFeedProcessorInstance.start()
                    .subscribeOn(Schedulers.elastic())
                    .doOnSuccess(aVoid -> {
                        // pass
                    })
                    .subscribe();
            Thread.sleep(50);

        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("END Sample");
    }

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer,
            CosmosAsyncContainer leaseContainer) {
        return new ChangeFeedProcessorBuilder()
                .hostName(hostName)
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges((List<JsonNode> docs) -> {
                    logger.info("--->setHandleChanges() START");
                    for (JsonNode document : docs) {
                        try {
                            logger.info(
                                    "---->DOCUMENT UPDATE RECEIVED: " + OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                                            .writeValueAsString(document));
                            CustomPOJO pojo_doc = OBJECT_MAPPER.treeToValue(document, CustomPOJO.class);
                            logger.info("----=>id: " + pojo_doc.getId());

                            ProductCategory doc = OBJECT_MAPPER.treeToValue(document, ProductCategory.class);
                            String categoryId = doc.getId();
                            String categoryName = doc.getName();
                            System.out.println("categoryId: " + categoryId);
                            System.out.println("new categoryName: " + categoryName);
                            UpdateProductCategoryName(categoryId, categoryName);

                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }
                    logger.info("--->handleChanges() END");

                })
                .buildChangeFeedProcessor();
    }

    public static CosmosAsyncClient getCosmosClient() {

        return new CosmosClientBuilder()
                .endpoint(ChangeFeedConfigurations.HOST)
                .key(ChangeFeedConfigurations.MASTER_KEY)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName) {
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        return client.getDatabase(databaseResponse.getProperties().getId());
    }

    public static void bulkReplaceItems(Flux<Product> products) {
        Flux<CosmosItemOperation> cosmosItemOperations = products
                .map(product -> CosmosBulkOperations.getReplaceItemOperation(product.getId(), product,
                        new PartitionKey(product.getCategoryId())));
        productContainer.executeBulkOperations(cosmosItemOperations).blockLast();
    }

    public static void UpdateProductCategoryName(String categoryId, String categoryName) {
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        CosmosPagedFlux<Product> productByCategoryPagedFluxResponse = productContainer.queryItems(
                "SELECT * FROM c WHERE c.categoryId = '" + categoryId + "'", queryOptions, Product.class);

        try {
            List<Product> productList = new ArrayList<Product>();
            productByCategoryPagedFluxResponse.byPage(preferredPageSize).flatMap(fluxResponse -> {

                for (Product doc : fluxResponse.getResults()) {
                    doc.setCategoryName(categoryName);
                    ObjectMapper jsondoc = new ObjectMapper();
                    try {
                        productList.add(doc);
                        System.out.println("Product doc that will be updated: "
                                + jsondoc.writerWithDefaultPrettyPrinter().writeValueAsString(doc));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
                Flux<Product> productsToReplace = Flux.fromIterable(productList);
                bulkReplaceItems(productsToReplace);
                return Flux.empty();
            }).blockLast();

        } catch (Exception err) {
            if (err instanceof CosmosException) {
                // Client-specific errors
                CosmosException cerr = (CosmosException) err;
                cerr.printStackTrace();
                logger.error(String.format("Read Item failed with %s\n", cerr));
            } else {
                // General errors
                err.printStackTrace();
            }
        }
    }

    public static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }
}
