package ed.inf.grape.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.graph.Partition;
import ed.inf.grape.graph.cg_graph;
import ed.inf.grape.graph.edge;

public class IO {

	static Logger log = LogManager.getLogger(IO.class);

	static public cg_graph loadGraphWithStreamScanner(String graphFileName)
			throws IOException {

		log.info("loading graph " + graphFileName + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		cg_graph graph = new cg_graph();

		fileInputStream = new FileInputStream(graphFileName);
		sc = new Scanner(fileInputStream, "UTF-8");
		while (sc.hasNextLine()) {
			String line = sc.nextLine();
			String[] nodes = line.split("\t");
			String vsource = nodes[0];

			graph.addVertex(vsource);

			// TODO:label = nodes[1];
			for (int i = 2; i < nodes.length; i++) {
				graph.addVertex(nodes[i]);
				graph.addEdge(vsource, nodes[i]);
			}
		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info("graph loaded. with vertices = " + graph.vertexSet().size()
				+ ", edges = " + graph.edgeSet().size() + ", using "
				+ (System.currentTimeMillis() - startTime) + " ms");

		return graph;
	}

	static public Partition loadPartitions(final int partitionID,
			final String partitionFilename) throws IOException {

		/**
		 * Load partition from file. (maybe partitioned by Metis, etc.). Each
		 * partition consists two files: 1. partitionName.v: vertexID
		 * vertexLabel 2. partitionName.e: edgeType-edgeSource-edgeTarget
		 * */

		log.info("loading partition " + partitionFilename
				+ " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		Partition partition = new Partition(partitionID);

		/** load vertices */
		fileInputStream = new FileInputStream(partitionFilename + ".v");
		sc = new Scanner(fileInputStream, "UTF-8");
		while (sc.hasNextLine()) {
			String line = sc.nextLine();
			String[] nodes = line.split("\t");
			String vsource = nodes[0];
			String label = nodes[1];

			partition.addVertex(vsource);
			// TODO: add labels
			// notice: virtual nodes may not have label
		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.debug("load vertex finished.");

		/** load edges */
		fileInputStream = new FileInputStream(partitionFilename + ".e");
		sc = new Scanner(fileInputStream, "UTF-8");
		int lc = 0;
		while (sc.hasNextLine()) {

			if (lc % 100000 == 0) {
				log.debug("load line " + lc);
			}

			String[] line = sc.nextLine().split("-");

			partition.addEdge(line[1], line[2]);

			if (line[0].equals(edge.TYPE_INCOMING)) {
				partition.addIncomingVertex(line[1]);
			}

			else if (line[0].equals(edge.TYPE_OUTGOING)) {
				partition.addOutgoingVertex(line[2]);
			}
			lc++;
		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info("graph partition loaded." + partition.getPartitionInfo()
				+ ", using " + (System.currentTimeMillis() - startTime) + " ms");

		return partition;
	}

	static public Map<Integer, Integer> loadInt2IntMapFromFile(String filename)
			throws IOException {

		HashMap<Integer, Integer> retMap = new HashMap<Integer, Integer>();

		log.info("loading map " + filename + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		fileInputStream = new FileInputStream(filename);
		sc = new Scanner(fileInputStream, "UTF-8");
		while (sc.hasNextLine()) {

			int key = sc.nextInt();
			int value = sc.nextInt();
			retMap.put(key, value);

		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info(filename + " loaded to map. with size =  " + retMap.size()
				+ ", using " + (System.currentTimeMillis() - startTime) + " ms");

		return retMap;
	}

	static public Map<String, Integer> loadString2IntMapFromFile(String filename)
			throws IOException {

		HashMap<String, Integer> retMap = new HashMap<String, Integer>();

		log.info("loading map " + filename + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		fileInputStream = new FileInputStream(filename);
		sc = new Scanner(fileInputStream, "UTF-8");
		while (sc.hasNextLine()) {

			String key = sc.next();
			int value = sc.nextInt();
			retMap.put(key, value);
		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info(filename + " loaded to map. with size =  " + retMap.size()
				+ ", using " + (System.currentTimeMillis() - startTime) + " ms");

		return retMap;
	}
}
