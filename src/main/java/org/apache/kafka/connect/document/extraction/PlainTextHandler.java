package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.Element;
import net.bitform.api.elements.GenericElementHandler;
import net.bitform.api.elements.RootElement;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class PlainTextHandler extends GenericElementHandler implements Handler {
    StringBuilder builder = new StringBuilder();
    private boolean paragraph;
    private boolean has_text;

    public PlainTextHandler() {
        paragraph = false;
        has_text = false;
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

    public String toString() {
        return builder.toString();
    }

    @Override
    public String getText() {
        return toString();
    }

    @Override
    public String getXML() {
        return null;
    }
}