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

import java.util.*;


/**
 * DocumentSourceTask is a Task that extracts and sends document content to Kafka
 *
 * @author Sergio Spinatelli
 */
// TODO: comment code properly
public class DocumentSourceTask extends SourceTask {
    public static final String FILENAME_FIELD = "document";
    public static final String READ = "read";

    private final static Logger log = LoggerFactory.getLogger(DocumentSourceTask.class);
    private static Schema subschema;
    private static Schema schema = null;

    private String schemaName;
    private String subSchemaName;
    private String topic;
    private Queue<String> files;
    private String extractor_cfg;
    private boolean done;
    private ContentExtractor extractor;
    private String output_type;
    private String filePrefix = null;

    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>(0);
    private String prefix = "";

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
            throw new ConnectException("missing config "+DocumentSourceConnector.SCHEMA_NAME);
        subSchemaName = "sub_" + schemaName;
        topic = props.get(DocumentSourceConnector.TOPIC);
        if (topic == null)
            throw new ConnectException("missing config "+DocumentSourceConnector.TOPIC);

        String paths = props.get(DocumentSourceConnector.FILES);
        if (paths == null || paths.isEmpty())
            throw new ConnectException("missing config "+DocumentSourceConnector.FILES);


        prefix = props.get(DocumentSourceConnector.PREFIX);

        if (!prefix.equals(""))
            prefix = prefix+"_";

        files = new LinkedList<>(Arrays.asList(paths.split(",")));

        extractor_cfg = props.get(DocumentSourceConnector.CONTENT_EXTRACTOR);
        output_type = props.get(DocumentSourceConnector.OUTPUT_TYPE);

        log.trace("Creating schema");
        // Metadata schema
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

        loadOffsets();
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
        String file = null;
        Map<String, Object> value = null;

        do {
            if (files.isEmpty()) {
                stop();
                return records;
            }
            file = files.poll();
            value = offsets.get(offsetKey(file));
        } while (value != null && ((Long) value.get(READ)) == 1);

        if (!done) {
            try {
                if (extractor_cfg == ContentExtractor.ORACLE)
                    extractor = new OracleContentExtractor(file, output_type);
                else
                    extractor = new TikaContentExtractor(file);

                Struct messageStruct = new Struct(schema);
                messageStruct.put("name", extractor.getFileName());
                messageStruct.put("content", output_type.equals("text") ? "" : extractor.getXHTML());
                messageStruct.put("raw_content", output_type.equals("getXHTML") ? "" : extractor.getPlainText());

                Struct subStruct = extractor_cfg.equals("tika") ? tikaMetadata(extractor.getMetadata()) : oracleMetadata(extractor.getMetadata());
                messageStruct.put("metadata", subStruct);

                SourceRecord record = new SourceRecord(offsetKey(file), offsetValue((long) 1), topic, messageStruct.schema(), messageStruct);
                records.add(record);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        stop(files.isEmpty());

        return records;
    }

    /**
     * Construct Struct object from metadata output by Tika
     * @param md Metadata object
     * @return Struct containing all metadata fields needed
     */
    private Struct tikaMetadata(Metadata md) {
        Struct subStruct = new Struct(subschema);
        subStruct.put("ContentType", md.get("Content-Type"));
        subStruct.put("Created", md.get("Creation-Date"));
        subStruct.put("LastModified", md.get("Last-Modified"));
        subStruct.put("Author", md.get("Author"));
        subStruct.put("Title", md.get("dc:title"));
        return subStruct;
    }

    /**
     * Construct Struct object from metadata output by Clean Content
     * @param md Metadata object
     * @return Struct containing all metadata fields needed
     */
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

    public void stop(boolean stop) {
        done = stop;
    }

    private Map<String, String> offsetKey(String partition) {
        return Collections.singletonMap(FILENAME_FIELD, prefix+partition);
    }

    private Map<String, Object> offsetValue(Long pos) {
        return Collections.singletonMap(READ, (Object) pos);
    }


    /**
     * Loads the current saved offsets.
     */
    private void loadOffsets() {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String file : files) {
            partitions.add(offsetKey(file));
        }
        try {
            offsets.putAll(context.offsetStorageReader().offsets(partitions));
        } catch (Exception nu) {
//            nu.printStackTrace();
        }
    }

}
