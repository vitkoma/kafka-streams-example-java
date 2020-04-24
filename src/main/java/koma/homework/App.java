package koma.homework;

import koma.homework.process.TopologyBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

import java.util.concurrent.CountDownLatch;

/**
 * The application entry point.
 * Based on the original skeleton.
 */
public class App {

    public static void main(String[] args) {
        final var builder = new StreamsBuilder();

        final var source = builder.stream("homework", Consumed.with(Serdes.String(), Serdes.String()));
        source.print(Printed.toSysOut());
        // your code can start here

        final var result = new TopologyBuilder().prepareTopology(source);
        result.print(Printed.toSysOut());
        result.to("homework-output");

        // end
        final var topology = builder.build();
        System.out.println(topology.describe());
        final var streams = new KafkaStreams(topology, new AppConfig().getProps());
        final var latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread ( () -> {
            streams.close();
            latch.countDown(); }
        ));

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}
