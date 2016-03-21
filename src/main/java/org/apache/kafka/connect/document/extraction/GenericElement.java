package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.Element;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author Sergio Spinatelli
 *
 * Generic wrapper for elements, around Oracle Clean Content's Element class.
 * Contains information to generate the XHTML
 */
public class GenericElement extends Element {
    private AttributesImpl atts = new AttributesImpl();
    public String content;
    public String name;
    public String prefix;

    public GenericElement(String name, String prefix) {
        this(name, null, null, prefix);
    }

    public GenericElement(String name, String metaName, String value, String prefix) {
        super(name, name);
        this.prefix = prefix;
        this.name = metaName;
        this.content = value;
    }

    /**
     * Override of the Element's toSaxStart method that simply adds two attributes to the element
     * if they are passed in the constructor and creates the element
     * @param handler The ContentHandler where to add the new element
     * @throws SAXException
     */
    public void toSaxStart(ContentHandler handler) throws SAXException {
        if (name != null)
            atts.addAttribute("", "name", "name", "CDATA", name);
        if (content != null)
            atts.addAttribute("", "content", "content", "CDATA", content);
        handler.startElement(prefix, internal_name, internal_name, atts);
    }

    /**
     * Override of the Element's toSaxEnd method
     * @param handler The ContentHandler where to add the new element
     * @throws SAXException
     */
    public void toSaxEnd(ContentHandler handler) throws SAXException {
        handler.endElement(prefix, internal_name, internal_name);
    }

    /**
     * Add a new attribute to the element
     * @param name Name of the attribute
     * @param value Value of the attribute
     */
    public void addAttribute(String name, String value) {
        atts.addAttribute("", name, name, "CDATA", value);
    }
}
