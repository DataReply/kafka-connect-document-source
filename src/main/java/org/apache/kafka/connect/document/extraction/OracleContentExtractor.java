package org.apache.kafka.connect.document.extraction;

import net.bitform.api.secure.SecureOptions;
import net.bitform.api.secure.SecureRequest;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Sergio Spinatelli
 *
 * Content extractor that uses the Oracle Clean Content library
 */
public class OracleContentExtractor implements ContentExtractor {
    private File file;
    private String filename;
    private Map<String, Class<?>> handlers = new HashMap<>(4);

    private SecureRequest request;
    private Handler handler;
    private String xml = "";
    private String text = "";
    private boolean extracted;
    private String metadata = "";

    /**
     * Constructor extracting XHTML and plain text
     * @param filename Path to the file from where to extract content
     * @throws Exception
     */
    public OracleContentExtractor(String filename) throws Exception {
        this(filename, OUTPUT_XML_TEXT);
    }

    /**
     * Constructor
     * @param filename Path to the file from where to extract content
     * @param output Type of output to retrieve (XHTML and/or plain text)
     * @throws Exception
     */
    public OracleContentExtractor(String filename, String output) throws Exception {
        file = new File(filename);
        this.filename = filename;
        handlers.put(OUTPUT_XML_TEXT, SimultaneousHandler.class);
        handlers.put(OUTPUT_TEXT_XML, SimultaneousHandler.class);
        handlers.put(OUTPUT_TEXT, PlainTextHandler.class);
        handlers.put(OUTPUT_XML, XMLHandler.class);

        if (!handlers.containsKey(output))
            throw new Exception("Invalid output type for OracleContentExtractor. Has to be one of [OUTPUT_XML, OUTPUT_TEXT, OUTPUT_XML_TEXT, OUTPUT_TEXT_XML]");

        extracted = false;
        handler = (Handler) handlers.get(output).newInstance();

        // Prepare SecureRequest for later content extraction
        request = new SecureRequest();
        request.setOption(SecureOptions.JustAnalyze, true);
        request.setOption(SecureOptions.OutputType, SecureOptions.OutputTypeOption.ToHandler);
        request.setOption(SecureOptions.SourceDocument, file);
        request.setOption(SecureOptions.ElementHandler, handler);
    }

    /**
     * Retrieve the plain text from the file
     * @return A String containing the plain text content of the file
     */
    @Override
    public String getPlainText() throws IOException, TikaException, SAXException {
        if (!extracted)
            extract();
        return text;
    }

    /**
     * Extract content in form of an XHTML string
     * @return a String containing the content of the document as XHTML
     * @throws IOException
     * @throws TikaException
     * @throws SAXException
     */
    @Override
    public String getXHTML() throws IOException, TikaException, SAXException {
        if (!extracted)
            extract();
        return xml;
    }

    /**
     * Method to get the name of the file from where content is extracted
     * @return a String of the file name
     */
    @Override
    public String getFileName() {
        return file.getName();
    }

    /**
     * Method to get the name of the file from where content is extracted
     * @return a String of the file name
     */
    @Override
    public String getFile() {
        return filename;
    }

    /**
     * Retrieve Metadata as a JSON String
     * @return A JSON String representing the metadata of the file
     */
    @Override
    public String getMetadataJson() {
        return handler.getMetadataJson();
    }

    /**
     * Method to get the Metadata object
     * @return A Metadata object
     */
    @Override
    public Metadata getMetadata() {
        return handler.getMetadata();
    }

    /**
     * This method does the extraction. It calls the execute function of the
     * Oracle Clean Content library and then gets the plain text and XHTML content
     * from the handler, updating the stored fields if necessary.
     * @throws IOException
     */
    private void extract() throws IOException {
        request.execute();
        String t = handler.getPlainText();
        if (t != null) text = t;
        String x = handler.getXHTML();
        if (x != null) xml = x;

        extracted = true;
    }
}
