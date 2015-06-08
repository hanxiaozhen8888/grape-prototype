package inf.ed.graph.structure;

import inf.ed.graph.structure.adaptor.DirectedEdge;
import inf.ed.graph.structure.adaptor.VertexInt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class NeoGraph<V extends Vertex, E extends Edge> implements Graph<V, E> {

	private GraphDatabaseService graphDb;
	private VertexFactory<V> vertexFactory;
	private EdgeFactory<V, E> edgeFactory;

	static Logger log = LogManager.getLogger(NeoGraph.class);

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	public NeoGraph(String dbname, Class<V> vertexClass, Class<E> edgeClass) {
		this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbname)
				.setConfig(GraphDatabaseSettings.allow_store_upgrade, "true").newGraphDatabase();
		// java.util.logging.Logger.getLogger("org.neo4j").setLevel(Level.ALL);

		this.vertexFactory = new VertexFactory<V>(vertexClass);
		this.edgeFactory = new EdgeFactory<V, E>(vertexClass, edgeClass);
		registerShutdownHook(this.graphDb);
	}

	@Override
	public void finalizeGraph() {
		this.graphDb.shutdown();
	}

	public boolean addVertex(V vertex) {
		throw new IllegalArgumentException("NeoGraph doesn't support this method currently.");
	}

	public boolean addEdge(E edge) {
		throw new IllegalArgumentException("NeoGraph doesn't support this method currently.");
	}

	public boolean addEdge(V from, V to) {
		throw new IllegalArgumentException("NeoGraph doesn't support this method currently.");
	}

	public Set<V> getChildren(V vertex) {
		long start = System.currentTimeMillis();
		String query = "START n=node:nodeIDIndex(id={nodeID}) MATCH (n)-[r]->(v) RETURN v.id, v.label";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("nodeID", vertex.getID());

		Set<V> children = new HashSet<V>();

		try (Transaction ignored = graphDb.beginTx();
				Result result = graphDb.execute(query, params)) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				V v = this.vertexFactory.createVertexWithMap(row);
				children.add(v);
			}
		}
		log.debug("neo4j::getChildrenOfNodeTime-" + (System.currentTimeMillis() - start) + "ms");

		return children;
	}

	public Set<V> getParents(V vertex) {
		long start = System.currentTimeMillis();
		String query = "START n=node:nodeIDIndex(id={nodeID}) MATCH (n)<-[r]-(v) RETURN v.id, v.label";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("nodeID", vertex.getID());

		Set<V> parents = new HashSet<V>();

		try (Transaction ignored = graphDb.beginTx();
				Result result = graphDb.execute(query, params)) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				V v = this.vertexFactory.createVertexWithMap(row);
				parents.add(v);
			}
		}
		log.debug("neo4j::getParentsOfNode-" + (System.currentTimeMillis() - start) + "ms");

		return parents;
	}

	public Set<V> getNeighbours(V vertex) {

		long start = System.currentTimeMillis();
		String query = "START n=node:nodeIDIndex(id={nodeID}) MATCH (n)-[r]-(v) RETURN v.id, v.label";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("nodeID", vertex.getID());

		Set<V> neighbours = new HashSet<V>();

		try (Transaction ignored = graphDb.beginTx();
				Result result = graphDb.execute(query, params)) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				V v = this.vertexFactory.createVertexWithMap(row);
				neighbours.add(v);
			}
		}
		log.debug("neo4j::getNeighbourOfNode-" + (System.currentTimeMillis() - start) + "ms");

		return neighbours;
	}

	public int edgeSize() {
		long start = System.currentTimeMillis();
		int edgeCount = -1;
		try (Transaction ignored = graphDb.beginTx();
				Result result = graphDb.execute("MATCH (n)-[r]->() RETURN count(*)")) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Entry<String, Object> column : row.entrySet()) {
					edgeCount = ((Long) column.getValue()).intValue();
				}
			}
		}
		log.debug("neo4j::getEdgeCountTime-" + (System.currentTimeMillis() - start) + "ms");
		return edgeCount;
	}

	public int vertexSize() {
		long start = System.currentTimeMillis();
		int nodeCount = -1;
		try (Transaction ignored = graphDb.beginTx();
				Result result = graphDb.execute("MATCH (n) RETURN count(n)")) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for (Entry<String, Object> column : row.entrySet()) {
					nodeCount = ((Long) column.getValue()).intValue();
				}
			}
		}
		log.debug("neo4j::getNodeCountTime-" + (System.currentTimeMillis() - start) + "ms");
		return nodeCount;
	}

	public V getVertex(int vID) {
		long start = System.currentTimeMillis();
		String query = "START v=node:nodeIDIndex(id={nodeID}) RETURN v.id, v.label";

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("nodeID", vID);

		V v = null;

		try (Transaction ignored = graphDb.beginTx();
				Result result = graphDb.execute(query, params)) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				v = this.vertexFactory.createVertexWithMap(row);
			}
		}
		log.debug("neo4j::getNode-" + (System.currentTimeMillis() - start) + "ms");

		return v;
	}

	public V getRandomVertex() {
		throw new IllegalArgumentException("NeoGraph doesn't support this method.");
	}

	public Set<E> allEdges() {

		long start = System.currentTimeMillis();
		String query = "MATCH (f)-->(t) RETURN f.id, f.label, t.id, t.label";

		Set<E> edges = new HashSet<E>();

		try (Transaction ignored = graphDb.beginTx(); Result result = graphDb.execute(query)) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				E e = this.edgeFactory.createEdgeWithMap(row);
				edges.add(e);
			}
		}
		log.debug("neo4j::getAllEdge-" + (System.currentTimeMillis() - start) + "ms");

		return edges;
	}

	public Map<Integer, V> allVertices() {

		long start = System.currentTimeMillis();
		String query = "MATCH (v) RETURN v.id, v.label";

		Map<Integer, V> vertices = new HashMap<Integer, V>();

		try (Transaction ignored = graphDb.beginTx(); Result result = graphDb.execute(query)) {
			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				// FIXME: make this more elegant.
				if (row.get("v.id") != null) {
					V v = this.vertexFactory.createVertexWithMap(row);
					vertices.put((int) row.get("v.id"), v);
				}
			}
		}
		log.debug("neo4j::getAllNode-" + (System.currentTimeMillis() - start) + "ms");

		return vertices;

	}

	public void clear() {
		// TODO Auto-generated method stub

	}

	public void clearEdges() {
		// TODO Auto-generated method stub

	}

	public boolean contains(int vertexID) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean contains(V from, V to) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean contains(E edge) {
		// TODO Auto-generated method stub
		return false;
	}

	public int degree(V vertex) {
		// TODO Auto-generated method stub
		return 0;
	}

	public Set<E> getEdges(V vertex1, V vertex2) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean hasCycles() {
		// TODO Auto-generated method stub
		return false;
	}

//	public int allnodes() {
//		long start = System.currentTimeMillis();
//		String query = "MATCH (v) RETURN v.id, v.label";
//
//		Map<Integer, V> vertices = new HashMap<Integer, V>();
//
//		try (Transaction ignored = graphDb.beginTx(); Result result = graphDb.execute(query)) {
//			while (result.hasNext()) {
//				Map<String, Object> row = result.next();
//				// FIXME: make this more elegant.
//				System.out.println(row);
//			}
//		}
//		log.debug("neo4j::getAllNode-" + (System.currentTimeMillis() - start) + "ms");
//
//		return 0;
//	}

	public boolean removeEdge(E edge) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeEdge(V from, V to) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeVertex(V vertex) {
		// used for remove null vertex.
		System.out.println("this.is remove vertex.");
		long start = System.currentTimeMillis();
		String query = "MATCH n WHERE NOT (HAS (n.id)) DELETE n";
		try (Transaction ignored = graphDb.beginTx(); Result result = graphDb.execute(query)) {
			System.out.println(result.getExecutionPlanDescription().toString());
			while (result.hasNext()) {
				Map<String, Object> row = result.next();

				System.out.println("+++++++++++++++++++++++++++++++" + row.toString());
			}
		}
		log.debug("neo4j::getNodeCountTime-" + (System.currentTimeMillis() - start) + "ms");

		return false;
	}

	public static void main(String[] paras) {
		String filePath = "dataset/neoSampleDB";
		Graph<VertexInt, DirectedEdge> g = new NeoGraph<VertexInt, DirectedEdge>(filePath,
				VertexInt.class, DirectedEdge.class);
		System.out.println(g.vertexSize());
		System.out.println(g.edgeSize());
		VertexInt v0 = g.getVertex(0);
		System.out.println(v0);

		Set<VertexInt> children = g.getChildren(v0);
		System.out.println(children.size());

		System.out.println(g.allVertices());
		System.out.println(g.allEdges());
		g.finalizeGraph();
	}

	@Override
	public boolean loadGraphFromVEFile(String filePathWithoutExtension) {
		throw new IllegalArgumentException("Please use batch-import to initial the NeoGraph.");
	}

	@Override
	public boolean contains(int fromID, int toID) {
		throw new IllegalArgumentException("to be implemented.");
	}

	@Override
	public Graph<V, E> getSubgraph(Class<V> vertexClass, Class<E> edgeClass, V center, int bound) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void display(int limit) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getRadius(V vertex) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<Integer> getChildren(int vertexID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Integer> getParents(int vertexID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Integer> getNeighbours(int vertexID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getDiameter() {
		// TODO Auto-generated method stub
		return 0;
	}

}
