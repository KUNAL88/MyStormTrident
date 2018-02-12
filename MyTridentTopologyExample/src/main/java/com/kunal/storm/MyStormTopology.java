package com.kunal.storm;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.*;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;


public class MyStormTopology {

    private static final String zkHosts="127.0.0.1:2181";
    private static final String kafkaBroker="127.0.0.1:9092";
    private static final String kafkaTopic="test";
    private static final String toplogyName="Kunal_Topology";


    public static void main(String[] args) throws AlreadyAliveException,
            InvalidTopologyException,AuthorizationException{

        System.out.println("Hello Kunal");

        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1") );
        /**
         * Trident Spout

        TridentTopology topology=new TridentTopology();
        BrokerHosts zk = new ZkHosts("localhost:2181");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "test");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());*/

        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector("test"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
       // OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        stream.partitionPersist(stateFactory, fields, new TridentKafkaUpdater(), new Fields());
        Config conf = new Config();

        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());





    }



}
