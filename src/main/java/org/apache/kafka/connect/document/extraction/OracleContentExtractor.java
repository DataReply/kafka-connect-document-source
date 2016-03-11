package org.apache.kafka.connect.document.extraction;

import net.bitform.api.secure.SecureOptions;
import net.bitform.api.secure.SecureRequest;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class OracleContentExtractor implements ContentExtractor {
    public final static int OUTPUT_XML = 0;
    public final static int OUTPUT_TEXT = 1;
    public final static int OUTPUT_XML_TEXT = 2;


    private Map<Integer, Class<?>> handlers = new HashMap<>(3);

    private SecureRequest request;
    private Handler handler;
    private int output;
    private String xml = "";
    private String text = "";
    private boolean extracted;

    public OracleContentExtractor(String file) throws Exception {
        this(file, 2);
    }

    public OracleContentExtractor(String file, int output) throws Exception {
        if (output > OUTPUT_XML_TEXT || output < OUTPUT_XML)
            throw new Exception("Invalid output type for OracleContentExtractor. Has to be one of [OUTPUT_XML, OUTPUT_TEXT, OUTPUT_XML_TEXT]");

        handlers.put(OUTPUT_XML_TEXT, SimultaneousHandler.class);
        handlers.put(OUTPUT_TEXT, XMLHandler.class);
        handlers.put(OUTPUT_XML, PlainTextHandler.class);

        this.output = output;
        extracted = false;
        handler = (Handler) handlers.get(output).newInstance();

        request = new SecureRequest();
        request.setOption(SecureOptions.JustAnalyze, true);
        request.setOption(SecureOptions.OutputType, SecureOptions.OutputTypeOption.ToHandler);
        request.setOption(SecureOptions.SourceDocument, new File(file));
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

    public void extract() throws IOException {
        request.execute();
        String t = handler.getText();
        if (t != null) text = t;
        String x = handler.getXML();
        if (x != null) xml = t;
    }
}
