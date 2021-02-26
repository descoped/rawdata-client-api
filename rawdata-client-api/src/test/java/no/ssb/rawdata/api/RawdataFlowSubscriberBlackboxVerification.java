package no.ssb.rawdata.api;

import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowSubscriberBlackboxVerification;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Flow;

public class RawdataFlowSubscriberBlackboxVerification extends FlowSubscriberBlackboxVerification<RawdataMessage.Builder> {

    public RawdataFlowSubscriberBlackboxVerification() {
        super(new TestEnvironment(100, 100));
    }

    @Override
    public Flow.Subscriber<RawdataMessage.Builder> createFlowSubscriber() {
        RawdataClient rawdataClient = new MemoryRawdataClientInitializer().initialize(Map.of());
        RawdataProducer producer = rawdataClient.producer("RawdataFlowSubscriberBlackboxVerification");
        return RawdataFlows.subscriber(() -> producer);
    }

    @Override
    public RawdataMessage.Builder createElement(int element) {
        return RawdataMessage.builder()
                .position(String.valueOf(element))
                .put("key", ("Value of element " + element).getBytes(StandardCharsets.UTF_8));
    }
}
