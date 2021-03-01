package no.ssb.rawdata.api;

import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Flow;

public class RawdataFlowPublisherVerification extends FlowPublisherVerification<RawdataMessage> {

    public RawdataFlowPublisherVerification() {
        super(new TestEnvironment(100, 100), 500);
    }

    @Override
    public Flow.Publisher<RawdataMessage> createFlowPublisher(long l) {
        String topic = "RawdataFlowPublisherVerification";
        RawdataClient rawdataClient = new MemoryRawdataClientInitializer().initialize(Map.of());
        try (RawdataProducer producer = rawdataClient.producer(topic)) {
            for (int i = 0; i < Math.min(l, 1000); i++) {
                producer.publish(RawdataMessage.builder()
                        .position("p-" + (i + 1))
                        .put("key", ("Value of element # " + (i + 1)).getBytes(StandardCharsets.UTF_8))
                        .build());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return RawdataFlows.publisher(() -> rawdataClient.consumer(topic));
    }

    @Override
    public Flow.Publisher<RawdataMessage> createFailedFlowPublisher() {
        return RawdataFlows.publisher(null);
    }
}
