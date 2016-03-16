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
    private String topic;
    private String filename_path;
    private String extractor_cfg;
    private boolean done;
    private ContentExtractor extractor;
    private String output_type;

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
        topic = props.get(DocumentSourceConnector.TOPIC);
        if (topic == null)
            throw new ConnectException("config topic null");


        filename_path = props.get(DocumentSourceConnector.FILE_PATH);
        if (filename_path == null || filename_path.isEmpty())
            throw new ConnectException("missing filename.path");

        extractor_cfg = props.get(DocumentSourceConnector.CONTENT_EXTRACTOR);
        output_type = props.get(DocumentSourceConnector.OUTPUT_TYPE);
        if (extractor_cfg == ContentExtractor.TIKA)
            extractor = new TikaContentExtractor(filename_path);
        else if (extractor_cfg == ContentExtractor.ORACLE) {
            try {
                extractor = new OracleContentExtractor(filename_path, output_type);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("raw_content", Schema.OPTIONAL_STRING_SCHEMA)
                .field("content", Schema.OPTIONAL_STRING_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
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
                messageStruct.put("content", extractor.xml());
                messageStruct.put("raw_content", extractor.plainText());
                messageStruct.put("metadata", extractor.metadata());
                SourceRecord record = new SourceRecord(Collections.singletonMap("document_content", extractor.fileName()), Collections.singletonMap(extractor.fileName(), 0), topic, messageStruct.schema(), messageStruct);
                records.add(record);
                stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return records;
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        done = true;
    }

}
