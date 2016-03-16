package org.apache.kafka.connect.document;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * @author Sergio Spinatelli
 */
public class DocumentSourceTaskTest {
    private DocumentSourceTask task;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;
    private Map<String, String> sourceProperties;
    private String prefix = "tmp";
    private int MIN_FILE_TESTS = 100;
    private int MAX_FILE_TESTS = 1000;

    @Before
    public void setup() {
        task = new DocumentSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic", "topic");
    }

    @Test
    public void testNormalLifecycle() {
        try {
            expectOffsetLookupReturnNone();
            replay();
            List<SourceRecord> records;

            Integer numberOfFiles = new Random().nextInt(new Random().nextInt(MAX_FILE_TESTS - MIN_FILE_TESTS - 1) + 1) + MIN_FILE_TESTS;
            System.out.println("CREATING " + numberOfFiles.toString() + " FILES");

            for (int i = 0; i < numberOfFiles; i++) {
                File f = File.createTempFile("document-source-test", null);
                FileWriter fw = new FileWriter(f);
                fw.write(Integer.toString(numberOfFiles - i));
                fw.close();

                sourceProperties.put("filename.path", f.getAbsolutePath());
                sourceProperties.put("content.extractor", "tika");
                sourceProperties.put("output.type", "text");
                task.start(sourceProperties);
                records = task.poll();
                Struct val = (Struct) records.get(0).value();
                System.out.println(val.getString("raw_content"));
                Assert.assertEquals(1, records.size());
                Assert.assertEquals((numberOfFiles - i), Integer.parseInt(val.getString("raw_content")));
                Assert.assertEquals(f.getName(), val.getString("name"));

                task.stop();
                f.delete();

            }
        } catch (Exception e) {
            System.out.println("------------------------EXCEPTION-------------------------");
            e.printStackTrace();
            System.out.println("---------------------------END----------------------------");
        }
    }

    private void replay() {
        PowerMock.replayAll();
    }

    private void expectOffsetLookupReturnNone() {
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.anyObject(List.class))).andReturn(new HashMap<Map<String, String>, Map<String, Object>>(0));
    }
}