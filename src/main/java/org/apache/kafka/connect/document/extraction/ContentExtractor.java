package org.apache.kafka.connect.document.extraction;

import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import java.io.IOException;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public interface ContentExtractor {
    String OUTPUT_XML = "xml";
    String OUTPUT_TEXT = "text";
    String OUTPUT_TEXT_XML = "text_xml";
    String OUTPUT_XML_TEXT = "xml_text";

    String TIKA = "tika";
    String ORACLE = "oracle";

    String plainText() throws IOException, TikaException, SAXException;

    String xml() throws IOException, TikaException, SAXException;

    String fileName();

    String metadata();
}
