package org.apache.kafka.connect.document.extraction;

import net.bitform.api.elements.Element;
import net.bitform.api.elements.RootElement;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
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
        plainHandler.close();
        super.close();
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

    public String getText() {
        return plainHandler.getText();
    }
}