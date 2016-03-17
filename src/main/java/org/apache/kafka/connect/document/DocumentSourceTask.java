package org.apache.kafka.connect.document;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.document.extraction.ContentExtractor;
import org.apache.kafka.connect.document.extraction.OracleContentExtractor;
import org.apache.kafka.connect.document.extraction.TikaContentExtractor;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * DocumentSourceTask is a Task that extracts and sends document content to Kafka
 *
 * @author Sergio Spinatelli
 */
// TODO: comment code properly
public class DocumentSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(DocumentSourceTask.class);


    private static Schema schema = null;
    private String schemaName;
    private String subSchemaName;
    private String topic;
    private String filename_path;
    private String extractor_cfg;
    private boolean done;
    private ContentExtractor extractor;
    private String output_type;
    private Schema subschema;

    @Override
    public String version() {
        return new DocumentSourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        schemaName = props.get(DocumentSourceConnector.SCHEMA_NAME);
        if (schemaName == null)
            throw new ConnectException("config schema.name null");
        subSchemaName = "sub_" + schemaName;
        topic = props.get(DocumentSourceConnector.TOPIC);
        if (topic == null)
            throw new ConnectException("config topic null");


        filename_path = props.get(DocumentSourceConnector.FILE_PATH);
        if (filename_path == null || filename_path.isEmpty())
            throw new ConnectException("missing filename.path");

        extractor_cfg = props.get(DocumentSourceConnector.CONTENT_EXTRACTOR);
        output_type = props.get(DocumentSourceConnector.OUTPUT_TYPE);

        if (extractor_cfg == ContentExtractor.ORACLE) {
            try {
                extractor = new OracleContentExtractor(filename_path, output_type);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else
            extractor = new TikaContentExtractor(filename_path);

        log.trace("Creating schema");
        subschema = SchemaBuilder
                .struct()
                .name(subSchemaName)
                .field("ContentType", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Created", Schema.OPTIONAL_STRING_SCHEMA)
                .field("LastModified", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Author", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Title", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("raw_content", Schema.OPTIONAL_STRING_SCHEMA)
                .field("content", Schema.OPTIONAL_STRING_SCHEMA)
                .field("metadata", subschema)
                .build();
        done = false;
    }


    /**
     * Poll this DocumentSourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {

        List<SourceRecord> records = new ArrayList<>();

        if (!done) {
            try {
                Struct messageStruct = new Struct(schema);
                messageStruct.put("name", extractor.fileName());
                messageStruct.put("content", output_type.equals("text") ? "" : extractor.xml());
                messageStruct.put("raw_content", output_type.equals("xml") ? "" : extractor.plainText());
                Struct subStruct = extractor_cfg.equals("tika") ? tikaMetadata(extractor.metadata()) : oracleMetadata(extractor.metadata());
                messageStruct.put("metadata", subStruct);
                SourceRecord record = new SourceRecord(Collections.singletonMap("document_content", extractor.fileName()), Collections.singletonMap(extractor.fileName(), 0), topic, messageStruct.schema(), messageStruct);
                records.add(record);
                stop();
            } catch (Exception e) {
                e.printStackTrace();
                stop();
            }
        }
        return records;
    }

    private Struct tikaMetadata(Metadata md) {
        Struct subStruct = new Struct(subschema);
        subStruct.put("ContentType", md.get("Content-Type"));
        subStruct.put("Created", md.get("Creation-Date"));
        subStruct.put("LastModified", md.get("Last-Modified"));
        subStruct.put("Author", md.get("Author"));
        subStruct.put("Title", md.get("dc:title"));
        return subStruct;
    }

    private Struct oracleMetadata(Metadata md) {
        Struct subStruct = new Struct(subschema);
        subStruct.put("ContentType", md.get("format"));
        subStruct.put("Created", md.get("DateCreated"));
        subStruct.put("LastModified", md.get("DateLastSaved"));
        subStruct.put("Author", md.get("Author"));
        subStruct.put("Title", md.get("Title"));
        return subStruct;
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        done = true;
    }

}
