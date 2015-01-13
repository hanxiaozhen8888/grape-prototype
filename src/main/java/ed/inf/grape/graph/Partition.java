package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashSet;

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
	private HashSet<String> incomingVertices;
	private HashSet<String> outgoingVertices;

	public Partition(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.fragment = new cg_graph();
		this.incomingVertices = new HashSet<String>(5000000);
		this.outgoingVertices = new HashSet<String>(5000000);
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

		if (!fragment.containsVertex(sourceVertex)) {
			fragment.addVertex(sourceVertex);
		}

		if (!fragment.containsVertex(targetVertex)) {
			fragment.addVertex(targetVertex);
		}

		return fragment.addEdge(sourceVertex, targetVertex);
	}

	public HashSet<String> getIncomingVertices() {
		return incomingVertices;
	}

	public HashSet<String> getOutgoingVertices() {
		return outgoingVertices;
	}

	public void addOutgoingVertex(String vertex) {
		this.outgoingVertices.add(vertex);
	}

	public void addIncomingVertex(String vertex) {
		this.incomingVertices.add(vertex);
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = "
				+ fragment.vertexSet().size() + " | edges = "
				+ fragment.edgeSet().size() + " | iv = "
				+ incomingVertices.size() + " | ov = "
				+ outgoingVertices.size();
	}
}
