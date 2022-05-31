# Cosmic Works Java

How to migrate a relational data model to Azure Cosmos DB, a distributed, horizontally scalable, NoSQL database.

This repo is used to support a presentation on how to migrate a relational database schema to a NoSQL database like Azure Cosmos DB. It is based on [CosmicWorks](https://github.com/AzureCosmosDB/CosmicWorks), but re-worked using the [Java V4 SDK](https://docs.microsoft.com/azure/cosmos-db/sql/sql-api-sdk-java-v4) for Azure Cosmos DB.  
This repo contains a Powerpoint presentation and Java resources at `/CosmicWorksJava/tree/master/src/main/java/com/azure/cosmos/examples` that represents the demos for this presentation with three folders:

* **modeling**: This contains the main app that shows the evolution of the data models from v1 to v4

* **changefeed**: This contains an app `ProductCategoryChangeFeedProcessor` which uses change feed processor to monitor the product categories container for changes and then propagates those to the products container.

* **models**: This contains all of the POJO classes used in the above two apps.

## Steps to setup

[![Deploy To Azure](https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/1-CONTRIBUTION-GUIDE/images/deploytoazure.svg?sanitize=true)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2Fazurecosmosdb%2Fcosmicworksjava%2Fmain%2Fazuredeploy.json)

1. Clone this repository to your local machine.
1. Click the Deploy to Azure button above. This will provision a new Cosmos DB account in a single region.
1. When the deployment is complete, click on the Outputs tab in the custom deployment blade. Copy the `uri` and `key` values and save locally.
1. Environment variables must be set with these values:

```
ACCOUNT_HOST=your account uri;ACCOUNT_KEY=your account primary key
```

1. To run a sample, specify its Main Class 

```
com.azure.cosmos.examples.modeling.async.ModelingDemos
```

1. On the main menu, press 'k' to create the database and container resources (Note, these are billable resources).
1. On the main menu, press 'l' to load data. (Note, this can take quite some time and may time out when run locally over low bandwidth connections. Best performance is running on a VM in the same region the Cosmos account was provisioned in.)
1. Run the `ProductCategoryChangeFeedProcessor.java` app in a separate JVM, specifying it's Main Class:

```
com.azure.cosmos.examples.changefeed.ProductCategoryChangeFeedProcessor
```

1. Finally, put breakpoints for any of the functions you want to run then press the corresponding menu item key to execute.

> [!IMPORTANT]
> To minimize cost related to this sample it is recommended to run the 'Delete databases and containers' item from the main menu. This will delete the databases and containers and just leave an empty Cosmos account which has no cost. You can then start the sample again and run 'k' and  'l' menu items to rehydrate the account.

## Source data

You can download all of the data for each of the 4 versions of the Cosmos DB databases as it progresses through its evolution from the data folder in this repository.
You can see the contents of these storage containers below.

* [Cosmic Works version 1](https://github.com/AzureCosmosDB/CosmicWorksJava/tree/main/src/main/java/com/azure/cosmos/examples/data/cosmic-works-v1)

* [Cosmic Works version 2](https://github.com/AzureCosmosDB/CosmicWorksJava/tree/main/src/main/java/com/azure/cosmos/examples/data/cosmic-works-v2)

* [Cosmic Works version 3](https://github.com/AzureCosmosDB/CosmicWorksJava/tree/main/src/main/java/com/azure/cosmos/examples/data/cosmic-works-v3)

* [Cosmic Works version 4](https://github.com/AzureCosmosDB/CosmicWorksJava/tree/main/src/main/java/com/azure/cosmos/examples/data/cosmic-works-v4)

You can also [download a bak file](https://github.com/AzureCosmosDB/CosmicWorksJava/tree/main/src/main/java/com/azure/cosmos/examples/data/adventure-works-2017) for the original Adventure Works 2017 database this session and app is built upon.
