// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.modeling.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.CosmosBatchOperationResult;
import com.azure.cosmos.models.CosmosBatchResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.azure.cosmos.examples.models.Models.CustomerV2;
import com.azure.cosmos.examples.models.Models.CustomerV4;
import com.azure.cosmos.examples.models.Models.Product;
import com.azure.cosmos.examples.models.Models.ProductCategory;
import com.azure.cosmos.examples.models.Models.SalesOrder;
import com.azure.cosmos.examples.models.Models.SalesOrderDetails;
import com.azure.cosmos.implementation.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class ModelingDemos implements AutoCloseable {

    private final CosmosClient client;
    private CosmosDatabase database;
    private CosmosContainer container;
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();

    protected static Logger logger = LoggerFactory.getLogger(ModelingDemos.class);

    public void close() {
        client.close();
    }

    public static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    /**
     * Run CosmicWorksJava demo app
     */
    // <Main>
    public static void main(String[] args) {
        ModelingDemos p = new ModelingDemos();

        try (Scanner in = new Scanner(System.in)) {

            boolean exit = false;
            while (!exit) {
                Thread.sleep(1000);
                clearScreen();
                System.out.println("Cosmos DB Modeling and Partitioning Demos");
                System.out.println("[a]   Query for single customer");
                System.out.println("[b]   Point read for single customer");
                System.out.println("[c]   List all product categories");
                System.out.println("[d]   Query products by category id");
                System.out.println("[e]   Update product category name");
                System.out.println("[f]   Query orders by customer id");
                System.out.println("[g]   Query for customer and all orders");
                System.out.println("[h]   Create new order and update order total");
                System.out.println("[i]   Delete order and update order total");
                System.out.println("[j]   Query top 10 customers");
                System.out.println("---------------------------------------------");
                System.out.println("[k]   Create databases and containers");
                System.out.println("[l]   Upload data to containers");
                System.out.println("[m]   Delete databases and containers");
                System.out.println("---------------------------------------------");
                System.out.println("[x]   Exit");
                String input = in.nextLine();
                if (input.equals("a")) {
                    clearScreen();
                    System.out.println("Calling query for single customer");
                    p.queryCustomer();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("b")) {
                    clearScreen();
                    System.out.println("Point read for single customer");
                    p.getCustomer();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("c")) {
                    clearScreen();
                    System.out.println("List all product categories");
                    p.listAllProductCategories();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("d")) {
                    clearScreen();
                    System.out.println("Query products by category id");
                    p.queryProductsByCategoryId();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("e")) {
                    clearScreen();
                    System.out.println("Update product category name");
                    p.queryProductsForCategory();
                    p.updateProductCategory();
                    p.pressAnyKeyToContinue("Category updated.\nPress any key to continue...");
                    p.queryProductsForCategory();
                    p.pressAnyKeyToContinue("Press any key to revert categories...");
                    p.revertProductCategory();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("f")) {
                    clearScreen();
                    System.out.println("Query orders by customer id");
                    p.querySalesOrdersByCustomerId();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("g")) {
                    clearScreen();
                    System.out.println("Query for customer and all orders");
                    p.queryCustomerAndSalesOrdersByCustomerId();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("h")) {
                    clearScreen();
                    System.out.println("Create new order and update order total");
                    p.createNewOrderAndUpdateCustomerOrderTotal();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("i")) {
                    clearScreen();
                    System.out.println("Delete order and update order total");
                    p.deleteOrder();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("j")) {
                    clearScreen();
                    System.out.println("Query top 10 customers");
                    p.getTop10Customers();
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("k")) {
                    clearScreen();
                    System.out.println("Create databases and containers");
                    new Deployment().createDatabase(p.client, 1);
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("l")) {
                    clearScreen();
                    System.out.println("Upload data to containers");
                    final long startTime = System.currentTimeMillis();
                    new Deployment().loadDatabase();
                    final long endTime = System.currentTimeMillis();
                    final long durationMillis = (endTime - startTime);
                    String duration = millisecondsToTime(durationMillis);
                    clearScreen();
                    System.out.println("Finished loading all data!!");
                    System.out.println("Upload took: " + duration);
                    p.pressAnyKeyToContinue("Press any key to continue...");
                }
                if (input.equals("m")) {
                    clearScreen();
                    System.out.println("Delete databases and containers");
                    new Deployment().deleteDatabases(p.client, 1);

                }
                if (input.equals("x")) {
                    exit = true;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    ModelingDemos() {
        ArrayList<String> preferredRegions = new ArrayList<>();
        preferredRegions.add("West US");
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .preferredRegions(preferredRegions)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildClient();
    }
    // </Main>

    public void queryCustomer() {
        database = client.getDatabase("database-v2");
        container = database.getContainer("customer");
        int preferredPageSize = 10;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);

        String customerId = "FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447";
        CosmosPagedIterable<Product> customerPagedIterable = container.queryItems(
                "SELECT * FROM c WHERE c.id = \"" + customerId + "\"", queryOptions, Product.class);

        customerPagedIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            logger.info("Item Ids " + cosmosItemPropertiesFeedResponse
                    .getResults()
                    .stream()
                    .map(Product::getId)
                    .collect(Collectors.toList()));
        });

    }

    public void getCustomer() {
        try {
            database = client.getDatabase("database-v2");
            container = database.getContainer("customer");
            String customerId = "FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447";
            CosmosItemResponse<CustomerV2> item = container.readItem(customerId, new PartitionKey(customerId),
                    CustomerV2.class);
            double requestCharge = item.getRequestCharge();
            Duration requestLatency = item.getDuration();
            logger.info(String.format(
                    "Point Read for a single customer\n. Item successfully read with id %s with a charge of %.2f and within duration %s",
                    item.getItem().getId(), requestCharge, requestLatency));
        } catch (CosmosException e) {
            e.printStackTrace();
            logger.info(String.format("Read Item failed with %s", e));
        }

    }

    public void listAllProductCategories() {
        database = client.getDatabase("database-v2");
        container = database.getContainer("productCategory");
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);

        CosmosPagedIterable<ProductCategory> productTypesIterable = container.queryItems(
                "SELECT * FROM c WHERE c.type = 'category'", queryOptions, ProductCategory.class);

        productTypesIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            logger.info("Product types " + cosmosItemPropertiesFeedResponse
                    .getResults()
                    .stream()
                    .map(ProductCategory::getName)
                    .collect(Collectors.toList()));
        });
    }

    public void queryProductsByCategoryId() {
        database = client.getDatabase("database-v3");
        container = database.getContainer("product");
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);

        String categoryId = "AB952F9F-5ABA-4251-BC2D-AFF8DF412A4A";

        CosmosPagedIterable<JsonNode> productByCategoryIterable = container.queryItems(
                "SELECT * FROM c WHERE c.categoryId = '" + categoryId + "'", queryOptions, JsonNode.class);

        productByCategoryIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            for (JsonNode product : cosmosItemPropertiesFeedResponse.getResults()) {
                ObjectMapper doc = new ObjectMapper();
                try {
                    System.out.println(doc.writerWithDefaultPrettyPrinter().writeValueAsString(product));
                } catch (JsonProcessingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

    }

    public void queryProductsForCategory() {
        database = client.getDatabase("database-v3");
        container = database.getContainer("product");
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);

        String sql = "SELECT COUNT(1) AS ProductCount, c.categoryName " +
                "FROM c WHERE c.categoryId = '86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4' " +
                "GROUP BY c.categoryName";
        CosmosPagedIterable<JsonNode> productByCategoryIterable = container.queryItems(
                sql, queryOptions, JsonNode.class);

        productByCategoryIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            for (JsonNode product : cosmosItemPropertiesFeedResponse.getResults()) {
                try {
                    System.out.println("Product count: " + product.get("ProductCount").asText() + "\nCategory name: "
                            + product.get("categoryName").asText());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    private void updateProductCategory() {
        database = client.getDatabase("database-v3");
        container = database.getContainer("productCategory");

        String categoryId = "86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4";

        System.out.println("Update the name and replace 'and' with '&'");
        ProductCategory updatedProductCategory = new ProductCategory();
        updatedProductCategory.setId(categoryId);
        updatedProductCategory.setType("category");
        updatedProductCategory.setName("Accessories, Tires & Tubes");

        CosmosItemResponse<ProductCategory> productCategoryResponse = container.replaceItem(updatedProductCategory,
                updatedProductCategory.getId(), new PartitionKey(updatedProductCategory.getType()),
                new CosmosItemRequestOptions());

        logger.info("Request charge of replace operation: {} RU", productCategoryResponse.getRequestCharge());

        logger.info("Done.");
    }

    private void revertProductCategory() {
        database = client.getDatabase("database-v3");
        container = database.getContainer("productCategory");

        String categoryId = "86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4";

        System.out.println("Change category name back to original");
        ProductCategory updatedProductCategory = new ProductCategory();
        updatedProductCategory.setId(categoryId);
        updatedProductCategory.setType("category");
        updatedProductCategory.setName("Accessories, Tires and Tubes");

        CosmosItemResponse<ProductCategory> productCategoryResponse = container.replaceItem(updatedProductCategory,
                updatedProductCategory.getId(), new PartitionKey(updatedProductCategory.getType()),
                new CosmosItemRequestOptions());

        logger.info("Request charge of replace operation: {} RU", productCategoryResponse.getRequestCharge());
        pressAnyKeyToContinue("Category reverted.\nPress any key to continue...");
        logger.info("Done.");
    }

    private void querySalesOrdersByCustomerId() {
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        database = client.getDatabase("database-v4");
        container = database.getContainer("customer");

        String customerId = "FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447";

        String sql = "SELECT * from c WHERE c.type = 'salesOrder' and c.customerId = '" + customerId + "'";

        CosmosPagedIterable<SalesOrder> customerSalesOrderIterable = container.queryItems(
                sql, queryOptions, SalesOrder.class);

        customerSalesOrderIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            System.out.println("Print out orders for this customer\n");
            for (SalesOrder product : cosmosItemPropertiesFeedResponse.getResults()) {
                ObjectMapper doc = new ObjectMapper();
                try {
                    System.out.println(doc.writerWithDefaultPrettyPrinter().writeValueAsString(product));
                } catch (JsonProcessingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    private void queryCustomerAndSalesOrdersByCustomerId() {
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        database = client.getDatabase("database-v4");
        container = database.getContainer("customer");

        String customerId = "FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447";

        String sql = "SELECT * from c WHERE c.customerId = '" + customerId + "'";

        CosmosPagedIterable<JsonNode> customerSalesOrderIterable = container.queryItems(
                sql, queryOptions, JsonNode.class);
        customerSalesOrderIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    cosmosItemPropertiesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + cosmosItemPropertiesFeedResponse.getRequestCharge());

            List<SalesOrder> orders = new ArrayList<>();
            CustomerV4 customer = new CustomerV4();
            for (JsonNode record : cosmosItemPropertiesFeedResponse.getResults()) {
                try {
                    if (record.get("type").asText().equals("customer")) {
                        customer = OBJECT_MAPPER.treeToValue(record, CustomerV4.class);
                    }
                    if (record.get("type").asText().equals("salesOrder")) {
                        orders.add(OBJECT_MAPPER.treeToValue(record, SalesOrder.class));
                    }
                } catch (JsonProcessingException | IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Printing out customer record and all their orders\n");
            ObjectMapper json = new ObjectMapper();
            try {
                System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(customer));
                for (SalesOrder order : orders) {
                    System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(order));
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        });
    }

    private void createNewOrderAndUpdateCustomerOrderTotal() {
        database = client.getDatabase("database-v4");
        container = database.getContainer("customer");

        // Get the customer
        String customerId = "FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447";
        CosmosItemResponse<CustomerV4> item = container.readItem(customerId, new PartitionKey(customerId),
                CustomerV4.class);
        CustomerV4 customer = item.getItem();

        // Increment the salesOrderTotal property
        customer.salesOrderCount++;

        // Create a new order
        String orderId = "5350ce31-ea50-4df9-9a48-faff97675ac5"; // Normally would use Guid.NewGuid().ToString()

        SalesOrder salesOrder = new SalesOrder();
        salesOrder.setId(orderId);
        salesOrder.setType("salesOrder");
        salesOrder.setCustomerId(customer.id);
        LocalDateTime myDateObj = LocalDateTime.now();
        System.out.println("Before formatting: " + myDateObj);
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDate = myDateObj.format(myFormatObj);
        System.out.println("After formatting: " + formattedDate);
        salesOrder.setOrderDate(formattedDate);
        salesOrder.setShipDate("");
        List<SalesOrderDetails> salesOrders = new ArrayList<>();
        SalesOrderDetails order1 = new SalesOrderDetails();
        {
            order1.setSku("FR-M94B-38");
            order1.setName("HL Mountain Frame - Black, 38");
            order1.setPrice(1349.6);
            order1.setQuantity(1);
        }
        SalesOrderDetails order2 = new SalesOrderDetails();
        {
            order1.setSku("SO-R809-M");
            order1.setName("Racing Socks, M");
            order1.setPrice(8.99);
            order1.setQuantity(2);
        }
        salesOrders.add(order1);
        salesOrders.add(order2);
        salesOrder.setDetails(salesOrders);

        ObjectMapper doc = new ObjectMapper();
        try {
            System.out.println("Sales order to be updated: "
                    + doc.writerWithDefaultPrettyPrinter().writeValueAsString(salesOrder));
            System.out.println("Customer record to be updated: "
                    + doc.writerWithDefaultPrettyPrinter().writeValueAsString(customer));
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // Submit both as a transactional batch
        CosmosBatch batch = CosmosBatch.createCosmosBatch(new PartitionKey(customerId));
        batch.createItemOperation(salesOrder);
        batch.upsertItemOperation(customer);
        CosmosBatchResponse response = container.executeCosmosBatch(batch);
        CosmosBatchOperationResult result = response.getResults().get(0);

        if (!response.isSuccessStatusCode()) {
            // Handle and log exception
            System.out.println("There was an error, status code: " + result.getStatusCode());
            if (result.getStatusCode() == 409) {
                System.out.println("Looks like the record is already there. Try running delete record first.");
            }
        } else {
            System.out.println("Order created successfully");
        }
    }

    private void deleteOrder() {
        database = client.getDatabase("database-v4");
        container = database.getContainer("customer");

        String customerId = "FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447";
        String orderId = "5350ce31-ea50-4df9-9a48-faff97675ac5";

        CosmosItemResponse<CustomerV4> item = container.readItem(customerId, new PartitionKey(customerId),
                CustomerV4.class);
        CustomerV4 customer = item.getItem();

        // Decrement the salesOrderTotal property
        customer.salesOrderCount--;

        // Submit both as a transactional batch
        CosmosBatch batch = CosmosBatch.createCosmosBatch(new PartitionKey(customerId));
        batch.deleteItemOperation(orderId);
        batch.replaceItemOperation(customerId, customer);
        CosmosBatchResponse response = container.executeCosmosBatch(batch);
        CosmosBatchOperationResult result = response.getResults().get(0);

        if (!response.isSuccessStatusCode()) {
            // Handle and log exception
            System.out.println("There was an error, status code: " + result.getStatusCode());
        } else {
            System.out.println("Order deleted successfully");
        }
    }

    private void getTop10Customers() {
        int preferredPageSize = 100;
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        database = client.getDatabase("database-v4");
        container = database.getContainer("customer");

        // Query to get our top 10 customers
        String sql = "SELECT TOP 10 c.firstName, c.lastName, c.salesOrderCount " +
                "FROM c WHERE c.type = 'customer' " +
                "ORDER BY c.salesOrderCount DESC";

        CosmosPagedIterable<JsonNode> customerIterable = container.queryItems(
                sql, queryOptions, JsonNode.class);
        customerIterable.iterableByPage(preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            System.out.println("Print out top 10 customers and number of orders\n");
            for (JsonNode record : cosmosItemPropertiesFeedResponse.getResults()) {
                try {
                    System.out.println(
                            "Customer Name: " + record.get("firstName").asText() + " " + record.get("lastName").asText()
                                    + "\t\tOrders: " + record.get("salesOrderCount").asText());
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static String millisecondsToTime(long milliseconds) {
        long minutes = (milliseconds / 1000) / 60;
        long seconds = (milliseconds / 1000) % 60;
        String secondsStr = Long.toString(seconds);
        String secs;
        if (secondsStr.length() >= 2) {
            secs = secondsStr.substring(0, 2);
        } else {
            secs = "0" + secondsStr;
        }
        return minutes + " minutes, " + secs + " seconds.";
    }

    private void pressAnyKeyToContinue(String message) {
        System.out.println(message);
        try {
            // noinspection ResultOfMethodCallIgnored
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void shutdown() {
        client.close();
        logger.info("Done.");
    }
}
