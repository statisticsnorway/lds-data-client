# Linked Data Store (LDS) data module

Proof of Concept module for reading and writing data to underlying distributed datastore.

## Modules

The project is composed of the following modules; 

1. lds-data-client. The main modules, it contains the DataClient class that can be used to write and read
data.
2. lds-data-gcs. Backend implementation for Google Cloud Storage. 
3. lds-data-hadoop. Backend implementation for Hadoop fs.
5. lds-data-service. A REST web service around the lds-data-client.

## Usage

Add the desired modules to your project:  

```
<dependency>
    <groupId>no.ssb.lds.data</groupId>
    <artifactId>lds-data-client</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>

<dependency>
    <groupId>no.ssb.lds.data</groupId>
    <artifactId>lds-data-gcs</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Instanciate the client: 

```java

// Set the parquet settings.
ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();

// Set the parquet settings.
DataClient.Configuration clientConfiguration = new DataClient.Configuration();

BinaryBackend backend = /* ... */

client = DataClient.builder()
        .withParquetProvider(new ParquetProvider(parquetConfiguration))
        .withBinaryBackend(backend)
        .withConfiguration(clientConfiguration)
        .withFormatConverters(/* ... */)
        .build();

```
