package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.ElementHandler;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public interface Handler extends ElementHandler {
    String getText();
    String getXML();
}
