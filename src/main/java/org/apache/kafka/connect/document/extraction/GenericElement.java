package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.Element;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Created by Sergio Spinatelli on 15.03.2016.
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

    public void toSaxStart(ContentHandler handler) throws SAXException {
        if (name != null)
            atts.addAttribute("", "name", "name", "CDATA", name);
        if (content != null)
            atts.addAttribute("", "content", "content", "CDATA", content);
        handler.startElement(prefix, internal_name, internal_name, atts);
    }

    public void toSaxEnd(ContentHandler handler) throws SAXException {
        handler.endElement(prefix, internal_name, internal_name);
    }

    public void addAttribute(String name, String value) {
        atts.addAttribute("", name, name, "CDATA", value);
    }
}
