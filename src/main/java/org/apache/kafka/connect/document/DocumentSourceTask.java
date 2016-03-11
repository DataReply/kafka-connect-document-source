package org.apache.kafka.connect.document;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.*;
import java.util.*;


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
    private String content_extractor;
    private boolean done;

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
        if(schemaName == null)
            throw new ConnectException("config schema.name null");
        topic = props.get(DocumentSourceConnector.TOPIC);
        if(topic == null)
            throw new ConnectException("config topic null");


        filename_path = props.get(DocumentSourceConnector.FILE_PATH);
        if(filename_path == null || filename_path.isEmpty())
            throw new ConnectException("missing filename.path");

        content_extractor = props.get(DocumentSourceConnector.CONTENT_EXTRACTOR);

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
            File file = new File(filename_path);
            // creates the record
            // no need to save offsets
            SourceRecord record = tikaExtractContent(file);
            records.add(record);
            stop();
        }
        return records;
    }


    /**
     * Create a new SourceRecord from a File's contents
     *
     * @return a source records
     */
    private SourceRecord tikaExtractContent(File file) {
        AutoDetectParser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        try {
            InputStream stream = new FileInputStream(file);
            InputStream raw_stream = new FileInputStream(file);

            ContentHandler handler = new ToXMLContentHandler();
            parser.parse(stream, handler, metadata);

            StringWriter writer = new StringWriter();
            JsonMetadata.toJson(metadata, writer);

            ContentHandler raw_handler = new BodyContentHandler();
            parser.parse(raw_stream, raw_handler, metadata);

            // creates the structured message
            Struct messageStruct = new Struct(schema);
            messageStruct.put("name", file.getName());
            messageStruct.put("content", handler.toString().trim());
            messageStruct.put("raw_content", raw_handler.toString().trim());
            messageStruct.put("metadata", writer.toString().trim());

            stream.close();
            raw_stream.close();

            return new SourceRecord(Collections.singletonMap("document_content", file.getName()), Collections.singletonMap(file.getName(), 0), topic, messageStruct.schema(), messageStruct);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // TODO: am in meeting, not sure about this, will think about it later
        return null;
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        done = true;
    }

}
