package storm;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
/**
 * Created by CLY on 2015/5/27.
 */
public class StormMain {
    public static void main(String args[]) throws AlreadyAliveException,
            InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new ReadFileSpout(),1);
        builder.setBolt("DataQuality",new DataQualityBolt()).shuffleGrouping("spout");
        Config config = new Config();
        config.setNumWorkers(4);
        config.setMaxSpoutPending(1000);
        stormSubmitter.submitTopology("DataQuality-topology",config,builder.createTopology());
    }
}
