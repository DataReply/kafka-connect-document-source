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
files=/path/to/file/filename1.pdf,/path/to/file/filename2.docx
content.extractor=tika
output.type="xml"
```

- **name**: name of the connector
- **connector.class**: class of the implementation of the connector
- **tasks.max**: maximum number of tasks to create
- **schema.name**: name to use for the schema
- **topic**: name of the topic to append to
- **files**: comma separated list of paths to the files to extract content from
- **files.prefix**: prefix for the files
- **content.extractor**: type of content extractor to use; possible values are:
	- 'tika': use the Apache Tika content extractor (default)
	- 'oracle': use the Oracle Clean Content content extractor (faster but not recommended as it's less tested and testable)
- **output.type**: output type of extracted content, i.e. in what format the content should be sent to Kafka, can be one of:
	- 'text', to extract only the plain text
	- 'xml', to extract the content in a more structured format (XHTML) as well as the file's metadata
	- 'text_xml' or 'xml_text', to extract both (default)

# Records

The records added to Kafka have following fields:
- '**name**': the name of the file the content has been extracted from
- '**raw_content**': string containing the raw contents of the file (plain text only)
- '**metadata**': JSON string containing all metadata fields extracted from the file (empty if plain text only is extracted)
- '**content**': string containing the structured content of the file (XHTML) (not present if plain text only is extracted)