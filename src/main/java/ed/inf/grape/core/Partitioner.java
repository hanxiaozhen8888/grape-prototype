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

/**
 * Partitioner, divide a whole graph into several partitions with predefined
 * strategy.
 * 
 * TODO: make it interface, implements by different strategy, invoke by
 * reflection.
 * 
 * @author yecol
 *
 */
public class Partitioner {

	public static int STRATEGY_SIMPLE = 0;
	public static int STRATEGY_METIS = 1;

	/** Partition strategy */
	private int strategy;

	/** Partition count */
	private static int PARTITION_COUNT;

	private static String GRAPH_FILE_PATH;

	private static int GRAPH_VERTEX_COUNT;

	/** Partition id */
	private static int currentPartitionId;

	static Logger log = LogManager.getLogger(Partitioner.class);

	static {

		currentPartitionId = 0;

		try {
			PARTITION_COUNT = Config.getInstance().getIntProperty(
					"PARTITION_COUNT");
			GRAPH_FILE_PATH = Config.getInstance().getStringProperty(
					"GRAPH_FILE_PATH");
			GRAPH_VERTEX_COUNT = Config.getInstance().getIntProperty(
					"GRAPH_VERTEX_COUNT");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Partitioner(int strategy) {
		this.strategy = strategy;
	}

	public int getNumOfPartitions() {
		return Partitioner.PARTITION_COUNT;
	}

	private List<Partition> simplePartition() throws IOException {

		/** approximately compute the size of each partition */
		int sizeOfPartition = GRAPH_VERTEX_COUNT / PARTITION_COUNT + 1;

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
				try {
					if (unPartitionedVertices.contains(v)) {
						unPartitionedVertices.remove(v);
						int pId = i++ / sizeOfPartition;
						partitions.get(pId).addVertex(v);
						verticesInPartition.put(v, pId);
						toPartitionedVertices.addAll(Graphs
								.neighborListOf(g, v));
					}
				} catch (Exception e) {
					log.error(e.getStackTrace());
					log.error(i + "-" + sizeOfPartition);
				}
			}
		}

		log.debug("vertices partition finished.");

		int innerEdge = 0;

		/** add edges to each partition */
		for (edge e : g.edgeSet()) {
			String vsource = g.getEdgeSource(e);
			String vtarget = g.getEdgeTarget(e);
			int pIDOfSource = verticesInPartition.get(vsource);
			int pIDOfTarget = verticesInPartition.get(vtarget);
			if (pIDOfSource == pIDOfTarget) {
				innerEdge++;
				partitions.get(pIDOfSource).addEdge(vsource, vtarget);
			} else {
				partitions.get(pIDOfSource).addOutgoingVertex(vtarget);
				partitions.get(pIDOfTarget).addIncomingVertex(vsource);
			}
		}

		/*
		 * FIXME:
		 * 
		 * 1.only 37% are inner edges in amazon data set and 53% in YouTube data
		 * set. 2.incoming and outgoing map need adjust. or will lose info.
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

	public Partition getNextPartition() {

		/** run program target gpartition */

		Partition p = null;

		if (currentPartitionId >= PARTITION_COUNT) {
			return p;
		}

		else {

			String partitionFilename = GRAPH_FILE_PATH + ".p"
					+ String.valueOf(currentPartitionId);
			try {
				p = IO.loadPartitions(currentPartitionId++, partitionFilename);
			} catch (IOException e) {
				log.error("read partition file error.");
				e.printStackTrace();
			}
		}
		log.info(p.getPartitionInfo());

		return p;
	}

	public Map<String, Integer> getVirtualVertex2PartitionMap() {

		try {
			return IO.loadString2IntMapFromFile(GRAPH_FILE_PATH + ".vvp");
		} catch (IOException e) {
			log.error("load virtual vertex 2 partition map failed.");
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {

		Partitioner partitioner = new Partitioner(STRATEGY_METIS);

		String filename = "/home/yecol/repo/grape/target/file.bin";

		Partition p = partitioner.getNextPartition();

		while (p != null) {

			log.info(p.getPartitionInfo());
			p = partitioner.getNextPartition();
		}
	}

}
