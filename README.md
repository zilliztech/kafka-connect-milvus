# Kafka Connect Milvus Connector

This is a Kafka sink connector for Milvus. It allows you to stream vector data from Kafka to Milvus.

This current version supports connection from
[Confluent Cloud](https://www.confluent.io/confluent-cloud/) (hosted Kafka) to Milvus (self-hosted or 
[Zilliz Cloud](https://zilliz.com/cloud).

Zilliz Cloud and Milvus are vector databases where you can ingest, store and search vector data.
A data record in Zilliz Cloud or Milvus contains one vector field and multiple scalar fields such
as string, integer and float. To stream data, the Kafka message schema must match the schema of the Milvus collection, containing one and only one vector field, and zero to many scalar fields. Each field name must be exactly the same on both sides.

# Quick Start

Use this quick start to get up and running with the kafka-connect-milvus connector.

## Step 1: Download the kafka-connect-milvus plugin

Complete the following steps to download the kafka-connect-milvus plugin.

1. download the plugin zip file `zilliz-kafka-connect-milvus-v0.1.0-alpha.zip` from [here](https://github.com/zilliztech/kafka-connect-milvus/releases/tag/v0.1.0).

## Step 2: Configure Confluent Cloud and Zilliz Cloud

Ensure you have Confluent Cloud and Zilliz Cloud setup and properly configured.
1. If you don't already have a topic in Confluent Cloud that matches the schema of the Milvus collection, create a topic (e.g. `topic_0`) in Confluent Cloud.
2. If you don't already have a collection in Zilliz Cloud, create a collection with the same name (e.g. `topic_0`) in zilliz cloud with a vector field (in this example the vector has `dimension=8`).

Note: Make sure they have the same number of fields, where one and only one is a vector field, the names of each field are exactly the same. Only the field defined in milvus collection can be inserted.

<img src="src/main/resources/images/collection_schema.png" width="100%"  alt=""/>

## Step 3: Load the kafka-connect-milvus plugin to a Confluent Cloud Instance
1. Go to the Connectors section in your Confluent Cloud cluster.
2. Click on `Add Plugin`.
3. Upload the `zilliz-kafka-connect-milvus-xxx.zip` file you downloaded in Step 1.

<img src="src/main/resources/images/add_plugin.png" width="50%" />

4. put `com.milvus.io.kafka.MilvusSinkConnector` into Connector class.

## Step 4: Configure the kafka-connect-milvus Connector

1. Go to the Connectors section in your Confluent Cloud cluster.
2. Click on `Get Started`.

Provide the following credentials by `Add Sensitive Property`:

```json
  "public.endpoint": "https://<public.endpoint>:port",
  "token": "*****************************************",
  "collection.name": "topic_0",
  "topics": "topic_0"
```

Note: the token field is either the API token or `<username>:<password>`, depending on the instance type of your collection in Milvus or Zilliz Cloud.
  
## Step 5: Launch the connector

Start the connector to begin streaming data from Kafka to Milvus.

* First, produce a message to the Kafka topic you just created in Confluent Cloud
```json
{
  "id": 0,
  "title": "The Reported Mortality Rate of Coronavirus Is Not Important",
  "title_vector": [0.041732933, 0.013779674, -0.027564144, -0.013061441, 0.009748648, 0.00082446384, -0.00071647146, 0.048612226],
  "link": "https://medium.com/swlh/the-reported-mortality-rate-of-coronavirus-is-not-important-369989c8d912"
}
```

* Then check if the data record has been  inserted into the collection in Zilliz Cloud. Here is what it looks like on Zilliz Cloud if the insertion was successful:

<img src="src/main/resources/images/insearted_entities.png" width="80%" />
 

### Support

If you require any assistance or have questions regarding the Kafka Connect Milvus Connector, please feel free to reach out to our support team: **Email:** [support@zilliz.com](mailto:support@zilliz.com)
