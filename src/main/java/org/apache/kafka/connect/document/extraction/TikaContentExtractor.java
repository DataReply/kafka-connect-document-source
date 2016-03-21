package org.apache.kafka.connect.document.extraction;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;

/**
 * @author Sergio Spinatelli
 *
 * Content extractor that uses the Apache Tika library
 */
public class TikaContentExtractor implements ContentExtractor {
    private String filename;
    private AutoDetectParser parser = new AutoDetectParser();
    private String metadata = "";
    private Metadata md = new Metadata();
    private File file;

    /**
     * Constructor
     * @param filename Path to the file from where to extract content
     * @throws Exception
     */
    public TikaContentExtractor(String filename) {
        file = new File(filename);
        this.filename = filename;
    }

    /**
     * Retrieve the plain text from the file
     * @return A String containing the plain text content of the file
     */
    @Override
    public String getPlainText() throws IOException, TikaException, SAXException {
        return extract(new BodyContentHandler(-1), metadata == "");
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
        return extract(new ToXMLContentHandler(), true);
    }

    /**
     * Retrieve getMetadata as a JSON String
     * @return A JSON String representing the getMetadata of the file
     */
    @Override
    public String getMetadataJson() {
        return metadata;
    }

    /**
     * Method to get the Metadata object
     * @return A Metadata object
     */
    @Override
    public Metadata getMetadata() {
        return md;
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
     * This method does the extraction. It calls the execute function of the
     * Apache Tika library and then gets the plain text and XHTML content
     * from the handler, updating the stored fields if necessary.
     * @throws IOException
     */
    private String extract(ContentHandler handler, boolean updateMetadata) throws TikaException, SAXException, IOException {
        InputStream stream = new FileInputStream(file);

        if (updateMetadata) {
            parser.parse(stream, handler, md);
            StringWriter writer = new StringWriter();
            JsonMetadata.toJson(md, writer);
            writer.close();
            metadata = writer.toString();
        } else {
            Metadata m = new Metadata();
            parser.parse(stream, handler, m);
        }
        stream.close();
        return handler.toString().trim();
    }
}
