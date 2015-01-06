package ed.inf.grape.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graphs;

import ed.inf.grape.graph.Partition;
import ed.inf.grape.graph.cg_graph;
import ed.inf.grape.graph.edge;
import ed.inf.grape.util.Config;
import ed.inf.grape.util.IO;
import ed.inf.grape.util.Strings;

/**
 * Partitioner, divide a whole graph into several partitions with predefined
 * strategy.
 * 
 * @author yecol
 *
 */
public class Partitioner {

	/** Partition strategy */
	private static String PARTITION_STRATEGY;

	/** Partition count */
	private static int PARTITION_COUNT;

	private static String GRAPH_FILE_PATH;

	private static int GRAPH_VERTEX_COUNT;

	private static int GRAPH_EDGE_COUNT;

	/** Partition id */
	private static int currentPartitionId;

	static Logger log = LogManager.getLogger(Coordinator.class);

	static {

		currentPartitionId = 0;

		try {
			PARTITION_COUNT = Config.getInstance().getIntProperty(
					"PARTITION_COUNT");
			GRAPH_FILE_PATH = Config.getInstance().getStringProperty(
					"GRAPH_FILE_PATH");
			GRAPH_VERTEX_COUNT = Config.getInstance().getIntProperty(
					"GRAPH_VERTEX_COUNT");
			GRAPH_EDGE_COUNT = Config.getInstance().getIntProperty(
					"GRAPH_EDGE_COUNT");
		} catch (Exception e) {
			log.error(e.getStackTrace());
		}
	}

	public Partitioner() {
		PARTITION_STRATEGY = Strings.PARTITION_STRATEGY_DEFAULT;
	}

	public List<Partition> partitionGraph() throws IOException {
		if (PARTITION_STRATEGY == Strings.PARTITION_STRATEGY_DEFAULT) {
			return this.simplePartition();
		}
		return null;
	}

	private List<Partition> simplePartition() throws IOException {

		/** approximately compute the size of each partition */
		int sizeOfPartition = GRAPH_VERTEX_COUNT / PARTITION_COUNT;

		log.debug("graph_vertex count = " + GRAPH_VERTEX_COUNT);
		log.debug("size of partition = " + sizeOfPartition);

		/** the whole graph */
		cg_graph g = IO.loadGraphWithStreamScanner(GRAPH_FILE_PATH);

		/** vertices 2 partitionId */
		Map<String, Integer> verticesInPartition = new HashMap<String, Integer>();

		List<Partition> partitions = new ArrayList<Partition>();

		/** init partitions */
		for (int i = 0; i < PARTITION_COUNT; i++) {
			Partition partition = new Partition(currentPartitionId++);
			partitions.add(partition);
		}

		/** partition vertices by greedy strategy */
		Set<String> unPartitionedVertices = new HashSet<String>();
		unPartitionedVertices.addAll(g.vertexSet());
		Iterator<String> iter = null;

		Queue<String> toPartitionedVertices = new LinkedList<String>();

		int i = 0;

		while (!unPartitionedVertices.isEmpty()) {

			iter = unPartitionedVertices.iterator();

			String seedv = iter.next();
			toPartitionedVertices.add(seedv);

			while (!toPartitionedVertices.isEmpty()) {
				String v = toPartitionedVertices.poll();
				if (unPartitionedVertices.contains(v)) {
					unPartitionedVertices.remove(v);
					int pId = i++ / sizeOfPartition;
					partitions.get(pId).addVertex(v);
					verticesInPartition.put(v, pId);
					toPartitionedVertices.addAll(Graphs.neighborListOf(g, v));
				}
			}
		}

		log.debug("vertices partition finished.");

		int innerEdge = 0;

		/** add edges to each partition */
		for (edge e : g.edgeSet()) {
			String vsource = g.getEdgeSource(e);
			String vtarget = g.getEdgeTarget(e);
			int pIdOfSource = verticesInPartition.get(vsource);
			int pIdOfTarget = verticesInPartition.get(vtarget);
			if (pIdOfSource == pIdOfTarget) {
				innerEdge++;
				partitions.get(pIdOfSource).addEdge(vsource, vtarget);
			} else {
				partitions.get(pIdOfSource).getOutgoingVertices()
						.put(vtarget, pIdOfTarget);
				partitions.get(pIdOfTarget).getIncomingVertices()
						.put(vsource, pIdOfSource);
			}
		}

		/*
		 * FIXME:
		 * 
		 * 1.only 37% are inner edges 2.incoming and outgoing map need adjust.
		 * or will lose info.
		 * 
		 * edges=8112707 innerEdge=3031417 ratio=0.3736628230256559
		 */

		log.debug("edges=" + g.edgeSet().size() + " innerEdge=" + innerEdge
				+ " ratio=" + innerEdge * 1.0 / g.edgeSet().size());
		;

		log.debug("edges partition finished.");

		for (Partition p : partitions) {
			log.info(p.getPartitionInfo());
		}

		return partitions;
	}

	public static void main(String[] args) {

		Partitioner partitioner = new Partitioner();
		try {
			partitioner.partitionGraph();
		} catch (IOException e) {
			log.error(e.getStackTrace());
		}

	}

}
