package ed.inf.grape.graph;

import java.util.HashMap;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Partition {

	private int partitionId;
	private cg_graph fragment;
	/* store pair of vertex -> source-vertex-partitionId */
	private HashMap<String, Integer> incomingVertices;
	/* store pair of vertex -> target-vertex-partitionId */
	private HashMap<String, Integer> outgoingVertices;

	public Partition(int partitionId) {
		super();
		this.partitionId = partitionId;
		this.fragment = new cg_graph();
		this.incomingVertices = new HashMap<String, Integer>();
		this.outgoingVertices = new HashMap<String, Integer>();
	}

	public int getPartitionId() {
		return partitionId;
	}

	public cg_graph getFragment() {
		return fragment;
	}

	public HashMap<String, Integer> getIncomingVertices() {
		return incomingVertices;
	}

	public HashMap<String, Integer> getOutgoingVertices() {
		return outgoingVertices;
	}

}
