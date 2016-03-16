package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.*;
import net.bitform.core.Helper;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.CharBuffer;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
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

    public void end(Element var1) throws IOException {
        if (has_text) {
            paragraph = true;
            builder.append("\n");
        }
    }

    public void startMetadata(String name, String value) throws IOException {
        metadata.add(name, value);
    }

    public void startTextProperty(TextPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, null);
    }

    public void startStringProperty(StringPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, var1.value);
    }

    public void startBooleanProperty(BooleanPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Boolean.toString(var1.value));
    }

    public void startIntegerProperty(IntegerPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Long.toString(var1.value));
    }

    public void startFloatProperty(FloatPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Double.toString(var1.value));
    }

    public void startDateProperty(DatePropertyElement var1) throws IOException {
        this.startMetadata(var1.name, Helper.toXSDDate(var1.value));
    }

    public void startDurationProperty(DurationPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, String.valueOf(var1.value));
    }

    public void startDataProperty(DataPropertyElement var1) throws IOException {
        this.startMetadata(var1.name, null);
    }

    public void startLocaleProperty(LocalePropertyElement var1) throws IOException {
        this.startMetadata("locale", var1.localeName);
    }

    public void startCodepageProperty(CodepagePropertyElement var1) throws IOException {
        this.startMetadata("codepage", var1.codepageName);
    }

    public void startFormField(FormFieldElement var1) throws IOException {
    }

    // TODO: is this useful?
    public void startString(StringElement var1) throws IOException {
    }

    public String toString() {
        return builder.toString();
    }

    @Override
    public String getText() {
        return toString();
    }

    @Override
    public String getMetadata() {
        return md;
    }

    @Override
    public String getXML() {
        return null;
    }
}