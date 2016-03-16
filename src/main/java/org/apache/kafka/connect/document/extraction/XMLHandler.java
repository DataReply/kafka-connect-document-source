package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.*;
import net.bitform.core.Helper;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;
import org.apache.tika.sax.ToXMLContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.CharBuffer;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class XMLHandler extends GenericElementHandler implements Handler {
    public static String PREFIX = "http://www.bitform.net/xml/schema/elements.xsd";
    private ToXMLContentHandler handler = new ToXMLContentHandler();
    private boolean documentEnded = false;
    private char[] array = null;
    private AttributesImpl attributes = new AttributesImpl();
    private int head = 0;

    private Metadata metadata = new Metadata();
    private String md = "";

    public void startRoot(RootElement element) throws IOException {
        try {
            handler.startDocument();
            handler.startPrefixMapping("", PREFIX);
            handler.startElement(PREFIX, "html", "html", attributes);
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    public void endRoot(Element element) throws IOException {
        try {
            if (!documentEnded) {
                handler.endElement(PREFIX, "html", "html");
                handler.endPrefixMapping("");
                handler.endDocument();
                documentEnded = true;
            }
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    public void startProcessingInfo(ProcessingInfoElement var1) throws IOException {
        startMetadata("file", String.valueOf(var1.file));
        endMetadata();
        startMetadata("ExtractionDate", Helper.toXSDDate(var1.date));
        endMetadata();
    }

    public void endProcessingInfo(Element var1) throws IOException {
    }

    public void close() {
        try {
            if (!documentEnded) {
                handler.endElement(PREFIX, "root", "root");
                handler.endPrefixMapping(PREFIX);
                handler.endDocument();
            }
            StringWriter writer = new StringWriter();
            JsonMetadata.toJson(metadata, writer);
            writer.close();
            md = writer.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start(Element element) throws IOException {
        try {
            element.toSaxStart(handler);
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    public void end(Element element) throws IOException {
        try {
            element.toSaxEnd(handler);
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    public void locator(long loc) throws IOException {
        attributes.clear();
        attributes.addAttribute(null, "p", "p", "CDATA", "0x" + Long.toHexString(loc));

        try {
            handler.startElement(PREFIX, "locator", "locator", attributes);
            handler.endElement(PREFIX, "locator", "locator");
        } catch (SAXException e) {
            e.printStackTrace();
        }
    }

    public void text(CharBuffer buffer) throws IOException {
        try {
            int remaining = buffer.remaining();
            int position = buffer.position();
            int i;
            for (i = 0; i < remaining; ++i) {
                char character = buffer.get(position);
                if (character < 32 && character != 9 && character != 10 && character != 13) {
                    buffer.put(position, '�');
                } else if (character == 13) { // carriage return
                    buffer.put(position, '\n');
                } else if (character >= '\ud800' && character <= '\udfff') {
                    buffer.put(position, '�');
                }

                ++position;
            }

            if (buffer.hasArray()) {
                handler.characters(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                buffer.position(buffer.arrayOffset() + buffer.position() + buffer.remaining());
            } else {
                if (array == null) {
                    array = new char[1024];
                }

                while (buffer.remaining() > array.length) {
                    buffer.get(array);
                    handler.characters(array, 0, array.length);
                }

                if (buffer.remaining() > 0) {
                    i = buffer.remaining();
                    buffer.get(array, 0, i);
                    handler.characters(array, 0, i);
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startCollection(CollectionElement var1) throws IOException {
    }

    public void endCollection(Element var1) throws IOException {
    }

    public void startContent(ContentElement var1) throws IOException {
        startMetadata("type", String.valueOf(var1.type));
        endMetadata();
        startMetadata("format", String.valueOf(var1.format));
        endMetadata();
    }

    public void endContent(Element var1) throws IOException {
        GenericElement e = new GenericElement("body", PREFIX);
        this.end(e);
    }

    public void startEmbeddedContent(EmbeddedContentElement var1) throws IOException {
    }

    public void endEmbeddedContent(Element var1) throws IOException {
    }

    public void startPage(PageElement var1) throws IOException {
        if (head == 1) {
            GenericElement h = new GenericElement("head", PREFIX);
            this.end(h);
            head = 2;
            h = new GenericElement("body", PREFIX);
            this.start(h);
        }
        GenericElement e = new GenericElement("div", PREFIX);
        e.addAttribute("class", "page");
        this.start(e);
    }

    public void startL(LElement var1) throws IOException {
        GenericElement p = new GenericElement("p", PREFIX);
        this.start(p);
    }

    public void endL(Element var1) throws IOException {
        GenericElement p = new GenericElement("p", PREFIX);
        this.end(p);
    }

    public void startSecureResult(SecureResultElement var1) throws IOException {
    }

    public void endSecureResult(Element var1) throws IOException {
    }


    public void endPage(Element var1) throws IOException {
        GenericElement e = new GenericElement("div", PREFIX);
        this.end(e);
    }

    public void startMetadata(String name, String value) throws IOException {
        if (head == 0) {
            GenericElement h = new GenericElement("head", PREFIX);
            this.start(h);
            head = 1;
        }
        metadata.add(name, value);
        GenericElement e = new GenericElement("meta", name, value, PREFIX);
        this.start(e);
    }

    public void endMetadata() throws IOException {
        Element e = new GenericElement("meta", PREFIX);
        this.end(e);
    }

    public void startAnnot(AnnotElement var1) throws IOException {
        GenericElement p = new GenericElement("p", PREFIX);
        p.addAttribute("class", "annotation");
        this.start(p);
    }

    public void endAnnot(Element var1) throws IOException {
        GenericElement p = new GenericElement("p", PREFIX);
        this.end(p);
    }


    /**
     * Properties of the file, all "redirect" to Metadata methods
     */


    public void startTextProperty(TextPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, null);
    }

    public void endTextProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startStringProperty(StringPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, var1.value);
    }

    public void endStringProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startBooleanProperty(BooleanPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Boolean.toString(var1.value));
    }

    public void endBooleanProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startIntegerProperty(IntegerPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Long.toString(var1.value));
    }

    public void endIntegerProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startFloatProperty(FloatPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Double.toString(var1.value));
    }

    public void endFloatProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startDateProperty(DatePropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Helper.toXSDDate(var1.value));
    }

    public void endDateProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startDurationProperty(DurationPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, String.valueOf(var1.value));
    }

    public void endDurationProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startDataProperty(DataPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, null);
    }

    public void endDataProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startLocaleProperty(LocalePropertyElement var1) throws IOException {
        this.startMetadata("locale", var1.localeName);
    }

    public void endLocaleProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startCodepageProperty(CodepagePropertyElement var1) throws IOException {
        this.startMetadata("codepage", var1.codepageName);
    }

    public void endCodepageProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startListProperty(ListPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, null);
    }

    public void endListProperty(Element var1) throws IOException {
        this.endMetadata();
    }

    public void startFormField(FormFieldElement var1) throws IOException {
    }

    public void endFormField(Element var1) throws IOException {
    }

    // TODO: is this useful?
    public void startString(StringElement var1) throws IOException {
    }

    public void endString(Element var1) throws IOException {
    }

    /**
     * End properties section
     */


    @Override
    public String toString() {
        return handler.toString();
    }

    @Override
    public String getText() {
        return null;
    }

    @Override
    public String getXML() {
        return getXML(true);
    }

    public String getXML(boolean indent) {
        try {
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, indent ? "yes" : "no");
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StreamResult result = new StreamResult(new StringWriter());
            Source source = new StreamSource(new StringReader(toString()));
            transformer.transform(source, result);
            String xmlString = result.getWriter().toString();
            return xmlString;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    @Override
    public String getMetadata() {
        return md;
    }
}