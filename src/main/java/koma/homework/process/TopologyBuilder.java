package koma.homework.process;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import koma.homework.model.ModEvent;
import koma.homework.model.TGroup;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;

public class TopologyBuilder {

    private ObjectMapper jsonMapper = new ObjectMapper();

    public TopologyBuilder() {
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public KStream<String, Integer> prepareTopology(KStream<String, String> source) {
        return source
            .mapValues ( modJson -> {
                try {
                    return jsonMapper.readValue(modJson, ModEvent.class);
                } catch (IOException e) {
                    throw new StreamsException(e);
                }
            })
            .filter ( (k, modEvent) ->  modEvent.getOp().equals(ModEvent.OP_CREATE))
            .mapValues ( modEvent -> {
                try {
                    return jsonMapper.readValue(modEvent.getAfter(), TGroup.class);
                } catch (IOException e) {
                    throw new StreamsException(e);
                }
            })
            .filter ( (k, tGroup) -> tGroup.getLevels() != null && !tGroup.getLevels().isEmpty() )
            .selectKey ( (k, tGroup) -> tGroup.gettUnits().get(0).gettUnitId() )
            .mapValues ( tGroup -> tGroupConfirmed(tGroup) ? 1 : 0 )
            .groupByKey() // group by segment ID, the last event's confirmation status is the relevant one for the given segment
            .reduce ( (k, newValue) -> newValue )
            .groupBy ( (tUnitId, confirmed) -> KeyValue.pair(tUnitId.substring(0, tUnitId.indexOf(':')), confirmed) ) // group by task ID and sum the statuses
            .reduce(
                    (aggValue, newValue) -> aggValue + newValue,
                    (aggValue, oldValue) -> aggValue - oldValue)
            .toStream();

    }

    private boolean tGroupConfirmed(TGroup tGroup) {
       return tGroup.gettUnits().get(0).getConfirmedLevel().equals(tGroup.getLevels().get(0));
    }
}
