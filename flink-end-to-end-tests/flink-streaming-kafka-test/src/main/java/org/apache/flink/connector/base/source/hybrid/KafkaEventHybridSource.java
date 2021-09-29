package org.apache.flink.connector.base.source.hybrid;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.kafka.test.base.KafkaEvent;


public class KafkaEventHybridSource extends HybridSource<KafkaEvent> implements
                                                                     ResultTypeQueryable<KafkaEvent> {
    /** Protected for subclass.
     * @param sources*/
    protected KafkaEventHybridSource(List<HybridSource.SourceListEntry> sources) {
        super(sources);
    }

    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }
}
