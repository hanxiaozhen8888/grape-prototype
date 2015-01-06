package ed.inf.grape.core;

import java.util.ArrayList;
import java.util.List;

import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.Strings;

/**
 * Partitioner, divide a whole graph into several partitions with predefined
 * strategy.
 * 
 * @author yecol
 *
 */
public class Partitioner {

	/**Partition strategy*/
	private static String PARTITION_STRATEGY;
	
	/**Partition count, and as partition id.*/
	private static int partitionCount;

	public Partitioner() {
		PARTITION_STRATEGY = Strings.PARTITION_STRATEGY_DEFAULT;
	}

	public List<Partition> partitionGraph(String graphFileName) {
		if (PARTITION_STRATEGY == Strings.PARTITION_STRATEGY_DEFAULT) {
			return this.simplePartition(graphFileName);
		}
		return null;
	}

	private List<Partition> simplePartition(String graphFileName) {
		List<Partition> partitions = new ArrayList<Partition>();
		
		
		
		
		
		
		return partitions;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
