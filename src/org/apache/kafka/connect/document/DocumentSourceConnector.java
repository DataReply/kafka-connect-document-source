package org.apache.kafka.connect.document;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * DocumentSourceConnector implements the connector interface
 * to send extracted contents to Kafka
 *
 * @author Sergio Spinatelli
 */
public class DocumentSourceConnector extends SourceConnector {
    public static final String SCHEMA_NAME = "schema.name";
    public static final String TOPIC = "topic";
    public static final String FILE_PATH = "filename.path";

    private String schema_name;
    private String topic;
    private String filename_path;


    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        if(schema_name == null || schema_name.isEmpty())
            throw new ConnectException("missing schema.name");
        if(topic == null || topic.isEmpty())
            throw new ConnectException("missing topic");

        if(filename_path == null || filename_path.isEmpty())
            throw new ConnectException("missing filename.path");

    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return DocumentSourceTask.class;
    }


    /**
     * Returns a set of configurations for the Task based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for(int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(FILE_PATH, filename_path);
            config.put(SCHEMA_NAME, schema_name);
            config.put(TOPIC, topic);
            configs.add(config);
        }
        return configs;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

}
