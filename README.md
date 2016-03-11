# kafka-connect-document-source
The connector is used to load data extracted from documents (PDF, Word, ..) to Kafka.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration
```ini
name=document-source
connector.class=org.apache.kafka.connect.document.DocumentSourceConnector
tasks.max=1
schema.name=test_schema3
topic=test_topic3
filename.path=/path/to/file/filename.pdf
content.extractor=tika
```

- **name**: name of the connector
- **connector.class**: class of the implementation of the connector
- **tasks.max**: maximum number of tasks to create
- **schema.name**: name to use for the schema
- **topic**: name of the topic to append to
- **filename.path**: path to the file to extract content from
- **content.extractor**: type of content extractor to use; possible values are:
	- 'tika': use the Apache Tika content extractor

# Records

The records added to Kafka have following fields:
- '**name**': the name of the file the content has been extracted from
- '**metadata**': JSON string containing all metadata fields extracted from the file
- '**raw_content**': string containing the raw contents of the file (text only)
- '**content**': string containing the structured content of the file (XHTML)