package org.apache.kafka.connect.document;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * DocumentSourceTask is a Task that extracts and sends document content to Kafka
 *
 * @author Sergio Spinatelli
 */
public class DocumentSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(DocumentSourceTask.class);


    private TimerTask task;
    private static Schema schema = null;
    private String schemaName;
    private String topic;
    private String filename_path;


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

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("content", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
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

        File file = new File(filename_path);
        // creates the record
        // no need to save offsets
        SourceRecord record = extractContent(file);
        records.add(record);
        this.stop();

        return records;
    }


    /**
     * Create a new SourceRecord from a File's contents
     *
     * @return a source records
     */
    private SourceRecord extractContent(File file) {

        byte[] data = null;
        try {
            //transform file to byte[]
            Path path = Paths.get(file.getPath());
            data = Files.readAllBytes(path);
            log.error(String.valueOf(data.length));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // creates the structured message
        Struct messageStruct = new Struct(schema);
        messageStruct.put("name", file.getName());
        messageStruct.put("binary", data);
        // creates the record
        // no need to save offsets
        return new SourceRecord(Collections.singletonMap("file_binary", 0), Collections.singletonMap("0", 0), topic, messageStruct.schema(), messageStruct);
    }


    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
    }

}
