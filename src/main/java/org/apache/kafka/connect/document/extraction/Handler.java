package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.ElementHandler;
import org.apache.tika.metadata.Metadata;

/**
 * @author Sergio Spinatelli
 *
 * Interface representing an ElementHandler with a couple useful methods
 */
public interface Handler extends ElementHandler {

    /**
     * Retrieve the plain text from the file
     * @return A String containing the plain text content of the file
     */
    String getPlainText();

    /**
     * Retrieve the XHTML representation of the file's content
     * @return A String containing the XHTML representation of the content of the file
     */
    String getXHTML();

    /**
     * Retrieve getMetadata as a JSON String
     * @return A JSON String representing the getMetadata of the file
     */
    String getMetadataJson();

    /**
     * Retrieve the Metadata object for the file
     * @return The Metadata object
     */
    Metadata getMetadata();
}
