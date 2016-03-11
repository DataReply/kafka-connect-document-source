package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.Element;
import net.bitform.api.elements.ElementHandlerException;
import net.bitform.api.elements.GenericElementHandler;
import net.bitform.api.elements.RootElement;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class XMLHandler extends GenericElementHandler implements Handler {
    @Override
    public String getText() {
        return null;
    }

    @Override
    public String getXML() {
        return toString();
    }
}