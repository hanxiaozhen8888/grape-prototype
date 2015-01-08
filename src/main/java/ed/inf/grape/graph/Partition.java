package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Partition implements Serializable {

	private static final long serialVersionUID = -4757004627010733180L;

	private int partitionID;
	private cg_graph fragment;
	/* store pair of vertex -> source-vertex-partitionId */
	private HashMap<String, Integer> incomingVertices;
	/* store pair of vertex -> target-vertex-partitionId */
	private HashMap<String, Integer> outgoingVertices;

	public Partition(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.fragment = new cg_graph();
		this.incomingVertices = new HashMap<String, Integer>();
		this.outgoingVertices = new HashMap<String, Integer>();
	}

	public int getPartitionID() {
		return partitionID;
	}

	public cg_graph getFragment() {
		return fragment;
	}

	public boolean addVertex(String vertex) {
		return fragment.addVertex(vertex);
	}

	public edge addEdge(String sourceVertex, String targetVertex) {
		return fragment.addEdge(sourceVertex, targetVertex);
	}

	// public boolean addIncommingEdge(String vertex) {
	// return fragment.addVertex(vertex);
	// }

	public HashMap<String, Integer> getIncomingVertices() {
		return incomingVertices;
	}

	public HashMap<String, Integer> getOutgoingVertices() {
		return outgoingVertices;
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = "
				+ fragment.vertexSet().size() + " | edges = "
				+ fragment.edgeSet().size() + " | iv = "
				+ incomingVertices.size() + " | ov = "
				+ outgoingVertices.size();
	}
}
