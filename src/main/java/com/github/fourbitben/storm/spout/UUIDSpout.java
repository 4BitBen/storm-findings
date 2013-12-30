package com.github.fourbitben.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.UUID;

/**
 * Spout that generates a UUID.
 *
 * @author Ben Nelson
 */
public class UUIDSpout extends BaseRichSpout {

    private final String topologyName;
    private SpoutOutputCollector spoutOutputCollector;

    public UUIDSpout(String topologyName) {
        this.topologyName = topologyName;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("uuid"));
    }

    @Override
    public void nextTuple() {
        String uuid = UUID.randomUUID().toString();
        String msgid = UUID.randomUUID().toString();
        Logger.getLogger(topologyName).info("Generating UUID: " + uuid + " for message: " + msgid);
        spoutOutputCollector.emit(new Values(uuid), msgid);
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        Logger.getLogger(topologyName).info("ACK received: " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);

        Logger.getLogger(topologyName).info("FAIL received: " + msgId);
    }
}
