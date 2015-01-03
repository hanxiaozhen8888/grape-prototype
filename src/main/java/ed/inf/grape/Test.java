package ed.inf.grape;

import org.jgrapht.UndirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		UndirectedGraph<String, DefaultEdge> g = new SimpleGraph<String, DefaultEdge>(
				DefaultEdge.class);

		String v1 = "v1";
		String v2 = "v2";
		String v3 = "v3";
		String v4 = "v4";

		// add the vertices
		g.addVertex(v1);
		g.addVertex(v2);
		g.addVertex(v3);
		g.addVertex(v4);

		// add edges to create a circuit
		g.addEdge(v1, v2);
		g.addEdge(v2, v3);
		g.addEdge(v3, v4);
		g.addEdge(v4, v1);

		System.out.println(g);

		System.out.println("hello world. -- yecol");

	}

}
