package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.Element;
import net.bitform.api.elements.RootElement;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * @author Sergio Spinatelli
 *
 * Clean Content Handler to output both XHTML and plain text by
 * simultaneously using XMLHandler and PlainTextHandler.
 * Basically it inherits XML-specific methods from XMLHandler and
 * overrides PlainText ones, calling the PlainTextHandler and the XMLHandler
 * implementations of the methods.
 */
public class SimultaneousHandler extends XMLHandler implements Handler {
    PlainTextHandler plainHandler;

    public SimultaneousHandler() {
        plainHandler = new PlainTextHandler();
    }

    public void startRoot(RootElement element) throws IOException {
        super.startRoot(element);
        plainHandler.startRoot(element);
    }

    public void endRoot(Element element) throws IOException {
        plainHandler.endRoot(element);
        super.endRoot(element);
    }

    public void close() {
        super.close();
        plainHandler.close();
    }

    public void start(Element element) throws IOException {
        plainHandler.start(element);
        super.start(element);
    }

    public void end(Element element) throws IOException {
        plainHandler.end(element);
        super.end(element);
    }

    public void locator(long loc) throws IOException {
        plainHandler.locator(loc);
        super.locator(loc);
    }

    public void text(CharBuffer buffer) throws IOException {
        super.text(buffer.duplicate());
        plainHandler.text(buffer);
    }

    public String getPlainText() {
        return plainHandler.getPlainText();
    }

}