package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.*;
import net.bitform.core.Helper;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.CharBuffer;

/**
 * @author Sergio Spinatelli
 *
 * Clean Content Handler class for outputting plain text
 */
public class PlainTextHandler extends GenericElementHandler implements Handler {
    StringBuilder builder = new StringBuilder();
    private boolean paragraph;
    private boolean has_text;
    private Metadata metadata = new Metadata();
    private String md = "";

    public PlainTextHandler() {
        paragraph = false;
        has_text = false;
    }

    /**
     * Closes the handler and writes metadata to JSON
     */
    public void close() {
        super.close();
        try {
            StringWriter writer = new StringWriter();
            JsonMetadata.toJson(metadata, writer);
            writer.close();
            md = writer.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Callbacks for Clean Content "events"
     */


     public void startRoot(RootElement root) throws IOException {
    }

    public void endRoot(Element element) throws IOException {
        if (paragraph) {
            paragraph = false;
            builder.append("\n");
        }

        builder.append("\n");
    }

    public void text(CharBuffer buffer) throws IOException {
        if (paragraph) {
            paragraph = false;
            builder.append("\n");
        }

        builder.append(buffer);
        has_text = true;
    }

    public void end(Element element) throws IOException {
        if (has_text) {
            paragraph = true;
            builder.append("\n");
        }
    }

    /**
     * Start of a Metadata element (<meta> tags in the XHTML)
     * @param name Name of the metadata entry
     * @param value Value of the metadata entry
     * @throws IOException
     */
    public void startMetadata(String name, String value) throws IOException {
        metadata.add(name, value);
    }
    
    public void startTextProperty(TextPropertyElement property) throws IOException {
        this.startMetadata(property.name, null);
    }

    public void startStringProperty(StringPropertyElement property) throws IOException {
        this.startMetadata(property.name, property.value);
    }

    public void startBooleanProperty(BooleanPropertyElement property) throws IOException {
        this.startMetadata(property.name, Boolean.toString(property.value));
    }

    public void startIntegerProperty(IntegerPropertyElement property) throws IOException {
        this.startMetadata(property.name, Long.toString(property.value));
    }

    public void startFloatProperty(FloatPropertyElement property) throws IOException {
        this.startMetadata(property.name, Double.toString(property.value));
    }

    public void startDateProperty(DatePropertyElement property) throws IOException {
        this.startMetadata(property.name, Helper.toXSDDate(property.value));
    }

    public void startDurationProperty(DurationPropertyElement property) throws IOException {
        this.startMetadata(property.name, String.valueOf(property.value));
    }

    public void startDataProperty(DataPropertyElement property) throws IOException {
        this.startMetadata(property.name, null);
    }

    public void startLocaleProperty(LocalePropertyElement property) throws IOException {
        this.startMetadata("locale", property.localeName);
    }

    public void startCodepageProperty(CodepagePropertyElement property) throws IOException {
        this.startMetadata("codepage", property.codepageName);
    }

    public void startFormField(FormFieldElement element) throws IOException {
    }

    // TODO: is this useful?
    public void startString(StringElement element) throws IOException {
    }

    /**
     * Retrieve the String representation of this object, i.e. the plain text
     * @return A String containing the representation of the file contents
     */
    public String toString() {
        return builder.toString();
    }

    /**
     * Retrieve the plain text from the file
     * @return A String containing the plain text content of the file
     */
    @Override
    public String getPlainText() {
        return toString();
    }

    /**
     * Retrieve getMetadata as a JSON String
     * @return A JSON String representing the getMetadata of the file
     */
    @Override
    public String getMetadataJson() {
        return md;
    }

    /**
     * Retrieve the Metadata object for the file
     * @return The Metadata object
     */
    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Retrieve the XHTML representation of the file's content
     * @return A String containing the XHTML representation of the content of the file
     */
    @Override
    public String getXHTML() {
        return null;
    }
}