package org.apache.flink.streaming.kafka.test;

import java.io.BufferedReader;
import java.io.IOException;

import java.io.InputStreamReader;

import javax.annotation.Nullable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.streaming.kafka.test.base.KafkaEvent;


public class KafkaEventRecordFormat extends SimpleStreamFormat<KafkaEvent> {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";

    private final String charsetName;

    public KafkaEventRecordFormat() {
        this(DEFAULT_CHARSET_NAME);
    }

    public KafkaEventRecordFormat(String charsetName) {
        this.charsetName = charsetName;
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream, charsetName));
        return new Reader(reader);
    }

    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }

    /** The actual reader for the {@code TextLineFormat}. */
    public static final class Reader implements StreamFormat.Reader<KafkaEvent> {

        private final BufferedReader reader;

        Reader(final BufferedReader reader) {
            this.reader = reader;
        }

        @Nullable
        @Override
        public KafkaEvent read() throws IOException {
            return KafkaEvent.fromString(reader.readLine());
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
