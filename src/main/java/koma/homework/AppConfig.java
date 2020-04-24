package koma.homework;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;

/**
 * Application properties.
 */
public class AppConfig {

    public Properties getProps() {
        Properties props = new Properties();
        props.putAll(Map.of(

                StreamsConfig.APPLICATION_ID_CONFIG            , "homework-v0.2",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         , "localhost:9092",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   , Serdes.String().getClass(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.Integer().getClass(),
                StreamsConfig.NUM_STREAM_THREADS_CONFIG        , 1,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG     , 500

        ));
        return props;
    }
}
