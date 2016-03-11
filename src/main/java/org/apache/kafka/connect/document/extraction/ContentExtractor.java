package org.apache.kafka.connect.document.extraction;

import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import java.io.IOException;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public interface ContentExtractor {
    String TIKA = "tika";
    String ORACLE = "oracle";

    String plainText() throws IOException, TikaException, SAXException;

    String xml() throws IOException, TikaException, SAXException;
}
