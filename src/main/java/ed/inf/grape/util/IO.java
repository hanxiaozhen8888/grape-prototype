package ed.inf.grape.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.graph.cg_graph;

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

}
