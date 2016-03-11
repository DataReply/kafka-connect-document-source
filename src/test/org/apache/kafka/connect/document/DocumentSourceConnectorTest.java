package org.apache.kafka.connect.document;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sergio Spinatelli
 */
public class DocumentSourceConnectorTest {
    private DocumentSourceConnector connector;
    private ConnectorContext context;
    private Map<String, String> sourceProperties;


    @Before
    public void setup() {
        connector = new DocumentSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic", "topic");
        sourceProperties.put("filename.path", "/home/vagrant/testpath/text.txt");

    }

    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("schema", taskConfigs.get(0).get("schema.name"));
        Assert.assertEquals("topic", taskConfigs.get(0).get("topic"));
        Assert.assertEquals("/home/vagrant/testpath/text.txt", taskConfigs.get(0).get("filename.path"));
        PowerMock.verifyAll();
    }

    @Test
    public void testMultipleTasks() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        Assert.assertEquals(2, taskConfigs.size());
        Assert.assertEquals("schema", taskConfigs.get(0).get("schema.name"));
        Assert.assertEquals("topic", taskConfigs.get(0).get("topic"));
        Assert.assertEquals("/home/vagrant/testpath/text.txt", taskConfigs.get(0).get("filename.path"));

        Assert.assertEquals("schema", taskConfigs.get(1).get("schema.name"));
        Assert.assertEquals("topic", taskConfigs.get(1).get("topic"));
        Assert.assertEquals("/home/vagrant/testpath/text.txt", taskConfigs.get(1).get("filename.path"));
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        Assert.assertEquals(DocumentSourceTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }
}