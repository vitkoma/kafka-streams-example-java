package koma.homework.process;

import koma.homework.AppConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Before;
import org.junit.Test;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TopologyBuilderTest {

    private TopologyTestDriver testDriver = null;
    private ConsumerRecordFactory<Integer, String> factory = new ConsumerRecordFactory("homework", new IntegerSerializer(), new StringSerializer());
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private IntegerDeserializer intDeserializer = new IntegerDeserializer();

    @Before
    public void setUp() {
        final var builder = new StreamsBuilder();

        final var source = builder.stream("homework", Consumed.with(Serdes.String(), Serdes.String()));
        final var result = new TopologyBuilder().prepareTopology(source);
        result.to("homework-output");

        final var topology = builder.build();
        testDriver = new TopologyTestDriver(topology, new AppConfig().getProps());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void confirmation_event_should_be_processed_correctly() {
        var input = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n";
        testDriver.pipeInput(factory.create((Integer) null, input));
        final var result = outputToMap();
        Assert.assertEquals("Number of confirmed segments is not correct", 1, result.get("TyazVPlL11HYaTGs1_dc1").intValue());
    }

    @Test
    public void confirmation_and_deconfirmation_events_should_be_processed_correctly() {
        var input1 = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n";
        testDriver.pipeInput(factory.create((Integer) null, input1));
        var input2 = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 0,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n";
        testDriver.pipeInput(factory.create((Integer) null, input2));
        var result = outputToMap();
        Assert.assertEquals("Number of confirmed segments is not correct", 0, result.get("TyazVPlL11HYaTGs1_dc1").intValue());
    }

    @Test
    public void repeated_confirmation_event_should_increase_the_number_of_segments_once() {
        var input = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n";
        IntStream.rangeClosed(1, 3).forEach(i -> testDriver.pipeInput(factory.create((Integer) null, input)));
        var result = outputToMap();
        Assert.assertEquals("Number of confirmed segments is not correct", 1, result.get("TyazVPlL11HYaTGs1_dc1").intValue());
    }

    @Test
    public void events_from_the_sample_input_file_should_be_processed_correctly() {
        pipeFileToDriver();
        var result = outputToMap();
        Assert.assertEquals("Number of confirmed segments is not correct", 0, result.get("jNazVPlL11HFhTGs1_dc1").intValue());
        Assert.assertEquals("Number of confirmed segments is not correct", 2, result.get("TyazVPlL11HYaTGs1_dc1").intValue());
    }

    @Test
    public void events_from_the_sample_input_file_should_be_processed_correctly_when_piped_repeatedly() {
        IntStream.rangeClosed(1, 3).forEach(i -> pipeFileToDriver());
        var result = outputToMap();
        Assert.assertEquals("Number of confirmed segments is not correct", 0, result.get("jNazVPlL11HFhTGs1_dc1").intValue());
        Assert.assertEquals("Number of confirmed segments is not correct", 2, result.get("TyazVPlL11HYaTGs1_dc1").intValue());
    }

    private void pipeFileToDriver() {
        try {
            Stream<String> stream = Files.lines(Paths.get(getClass().getResource("/kafka-messages.jsonline").toURI()));
            stream
                .filter(line -> !line.isBlank())
                .forEach(line -> testDriver.pipeInput(factory.create((Integer) null, line)));
        } catch (Exception e) {
            throw new RuntimeException("Error while reading kafka-messages.jsonline", e);
        }
    }

    private Map<String, Integer> outputToMap() {
        final var result = new HashMap<String, Integer>();
        ProducerRecord<String, Integer> out;
        do {
            out = testDriver.readOutput("homework-output", stringDeserializer, intDeserializer);
            if (out != null) {
                result.put(out.key(), out.value());
            }
        } while (out != null);
        return result;
    }


}
