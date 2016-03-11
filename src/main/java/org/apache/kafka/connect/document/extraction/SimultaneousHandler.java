package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.GenericElementHandler;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class SimultaneousHandler extends GenericElementHandler implements Handler {
    PlainTextHandler plainHandler;
    XMLHandler xmlHandler;

    public SimultaneousHandler() {
        plainHandler = new PlainTextHandler();
        xmlHandler = new XMLHandler();
    }

    public String getText() {
        return plainHandler.toString();
    }

    public String getXML() {
        return xmlHandler.toString();
    }
}