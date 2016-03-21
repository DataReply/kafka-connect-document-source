package org.apache.kafka.connect.document.extraction;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

import java.io.IOException;

/**
 * @author Sergio Spinatelli
 *
 * This interface represents a content extractor, one of Tika and Oracle Clean Content.
 */
public interface ContentExtractor {
    /**
     * Output types for the extractor
     */
    String OUTPUT_XML = "getXHTML";
    String OUTPUT_TEXT = "text";
    String OUTPUT_TEXT_XML = "text_xml";
    String OUTPUT_XML_TEXT = "xml_text";

    String TIKA = "tika";
    String ORACLE = "oracle";

    /**
     * Extract content in form of plain text
     * @return a String containing the plain text of the document
     * @throws IOException
     * @throws TikaException
     * @throws SAXException
     */
    String getPlainText() throws IOException, TikaException, SAXException;

    /**
     * Extract content in form of an XHTML string
     * @return a String containing the content of the document as XHTML
     * @throws IOException
     * @throws TikaException
     * @throws SAXException
     */
    String getXHTML() throws IOException, TikaException, SAXException;

    /**
     * Method to get the name of the file from where content is extracted
     * @return a String of the file name
     */
    String getFileName();

    /**
     * Method to get the path of the file from where content is extracted
     * @return A String with the path
     */
    String getFile();

    /**
     * Method to get the Metadata object containing the file's getMetadata
     * @return the Metadata
     */
    Metadata getMetadata();

    /**
     * Method to get the getMetadata in form of a JSON String
     * @return a JSON String containing the getMetadata
     */
    String getMetadataJson();
}
