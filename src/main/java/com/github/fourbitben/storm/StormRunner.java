package com.github.fourbitben.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.github.fourbitben.storm.bolt.PrintAndSleepBolt;
import com.github.fourbitben.storm.spout.UUIDSpout;
import org.apache.log4j.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Class for initializing the topologies and running them locally.
 *
 * @author Ben Nelson
 */
public class StormRunner {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);


        Config conf = new Config();
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();

        final String misbehavingTopologyName = "misbehaving-topology";
        final String correctTopolgyName      = "correct-topology";
        cluster.submitTopology(misbehavingTopologyName, conf, createMisbehavingTopology(misbehavingTopologyName));
        cluster.submitTopology(correctTopolgyName, conf, createCorrectTopology(correctTopolgyName));

        Thread.sleep(1000 * 60 * 10); // Ten Minutes

        cluster.shutdown();
    }

    private static StormTopology createMisbehavingTopology(String misbehavingTopologyName) throws IOException {
        initializeLogger(misbehavingTopologyName);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new UUIDSpout(misbehavingTopologyName), 5);
        builder.setBolt("printSlow", new PrintAndSleepBolt("MinutePrinter", misbehavingTopologyName, 1, TimeUnit.MINUTES), 8).shuffleGrouping("spout");
        builder.setBolt("printFast", new PrintAndSleepBolt("MillisecondPrinter", misbehavingTopologyName, 1, TimeUnit.MILLISECONDS), 8).shuffleGrouping("spout");

        return builder.createTopology();
    }

    private static StormTopology createCorrectTopology(String correctTopologyName) throws IOException {
        initializeLogger(correctTopologyName);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new UUIDSpout(correctTopologyName), 5);
        builder.setBolt("print", new PrintAndSleepBolt("Printer", correctTopologyName, 1, TimeUnit.SECONDS), 8).shuffleGrouping("spout");

        return builder.createTopology();
    }

    private static void initializeLogger(String loggerName) throws IOException {
        Logger.getLogger(loggerName).removeAllAppenders();
        Logger.getLogger(loggerName).addAppender(new FileAppender(new PatternLayout("%d [%t] %-5p %c %x - %m%n"), loggerName + ".log", false));
        Logger.getLogger(loggerName).addAppender(new ConsoleAppender());
    }
}
