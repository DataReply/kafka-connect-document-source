package org.apache.kafka.connect.document.extraction;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;

/**
 * Created by Sergio Spinatelli on 11.03.2016.
 */
public class TikaContentExtractor implements ContentExtractor {
    private AutoDetectParser parser = new AutoDetectParser();
    private String metadata = "";
    private File file;

    public TikaContentExtractor(String filename) {
        file = new File(filename);
    }

    @Override
    public String plainText() throws IOException, TikaException, SAXException {
        return extract(new BodyContentHandler(-1), metadata == "");
    }

    @Override
    public String xml() throws IOException, TikaException, SAXException {
        return extract(new ToXMLContentHandler(), true);
    }

    @Override
    public String metadata() {
        return metadata;
    }

    @Override
    public String fileName() {
        return file.getName();
    }

    private String extract(ContentHandler handler, boolean updateMetadata) throws TikaException, SAXException, IOException {
        InputStream stream = new FileInputStream(file);
        Metadata md = new Metadata();
        parser.parse(stream, handler, md);
        if (updateMetadata) {
            StringWriter writer = new StringWriter();
            JsonMetadata.toJson(md, writer);
            writer.close();
            metadata = writer.toString();
        }
        stream.close();
        return handler.toString().trim();
    }
}
