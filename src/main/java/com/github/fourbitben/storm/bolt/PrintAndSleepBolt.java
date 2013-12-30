package com.github.fourbitben.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Bolt that prints out the tuple and sleeps for a period of time. Useful to mimic a slow bolt and its
 * impact on the topology.
 *
 * @author Ben Nelson
 */
public class PrintAndSleepBolt extends BaseRichBolt {
    private final String boltName;
    private final String topologyName;
    private final int sleepTime;
    private final TimeUnit sleepTimeUnit;
    private OutputCollector outputCollector;

    public PrintAndSleepBolt(String boltName, String topologyName, int sleepTime, TimeUnit sleepTimeUnit) {
        this.boltName = boltName;
        this.topologyName = topologyName;
        this.sleepTime = sleepTime;
        this.sleepTimeUnit = sleepTimeUnit;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Logger.getLogger(topologyName).info("Bolt: " + boltName + ", Tuple: " + tuple);
        outputCollector.ack(tuple);
        try {
            Thread.sleep(TimeUnit.MILLISECONDS.convert(sleepTime, sleepTimeUnit));
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
