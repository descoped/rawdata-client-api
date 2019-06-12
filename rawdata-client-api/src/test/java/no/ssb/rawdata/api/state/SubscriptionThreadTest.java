package no.ssb.rawdata.api.state;

import no.ssb.rawdata.api.persistence.Disposable;
import no.ssb.rawdata.api.persistence.Subscription;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.function.Consumer;

public class SubscriptionThreadTest {

    @Ignore
    @Test
    public void testName1() throws InterruptedException {
        try (Disposable disposable = subscribe(completedPosition -> {
            System.out.printf("Hello: %s%n", completedPosition);
        })
        ) {
            Thread.sleep(2000L);

            System.out.printf("Test1%n");

        }
    }

    @Ignore
    @Test
    public void testName2() throws InterruptedException {
        System.out.printf("Test2%n");
        Thread.sleep(2000L);
    }

    private Disposable subscribe(Consumer<CompletedPosition> completedPositionConsumer) {
        return new Subscription(null, null, null, null).subscribe(completedPositionConsumer);
    }

}
