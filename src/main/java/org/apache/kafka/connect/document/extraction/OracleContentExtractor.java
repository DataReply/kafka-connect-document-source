package org.apache.kafka.connect.document.extraction;

import net.bitform.api.secure.SecureOptions;
import net.bitform.api.secure.SecureRequest;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.serialization.JsonMetadata;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class OracleContentExtractor implements ContentExtractor {
    private final File file;
    private Map<String, Class<?>> handlers = new HashMap<>(4);

    private SecureRequest request;
    private Handler handler;
    private String xml = "";
    private String text = "";
    private boolean extracted;
    private String metadata = "";

    public OracleContentExtractor(String filename) throws Exception {
        this(filename, OUTPUT_XML_TEXT);
    }

    public OracleContentExtractor(String filename, String output) throws Exception {
        file = new File(filename);
        handlers.put(OUTPUT_XML_TEXT, SimultaneousHandler.class);
        handlers.put(OUTPUT_TEXT_XML, SimultaneousHandler.class);
        handlers.put(OUTPUT_TEXT, PlainTextHandler.class);
        handlers.put(OUTPUT_XML, XMLHandler.class);

        if (!handlers.containsKey(output))
            throw new Exception("Invalid output type for OracleContentExtractor. Has to be one of [OUTPUT_XML, OUTPUT_TEXT, OUTPUT_XML_TEXT, OUTPUT_TEXT_XML]");

        extracted = false;
        handler = (Handler) handlers.get(output).newInstance();

        request = new SecureRequest();
        request.setOption(SecureOptions.JustAnalyze, true);
        request.setOption(SecureOptions.OutputType, SecureOptions.OutputTypeOption.ToHandler);
        request.setOption(SecureOptions.SourceDocument, file);
        request.setOption(SecureOptions.ElementHandler, handler);
    }

    @Override
    public String plainText() throws IOException, TikaException, SAXException {
        if (!extracted)
            extract();
        return text;
    }

    @Override
    public String xml() throws IOException, TikaException, SAXException {
        if (!extracted)
            extract();
        return xml;
    }

    @Override
    public String fileName() {
        return file.getName();
    }

    @Override
    public String metadata() {
        return handler.getMetadata();
    }

    public void extract() throws IOException {
        request.execute();
        String t = handler.getText();
        if (t != null) text = t;
        String x = handler.getXML();
        if (x != null) xml = x;

        extracted = true;
    }
}
