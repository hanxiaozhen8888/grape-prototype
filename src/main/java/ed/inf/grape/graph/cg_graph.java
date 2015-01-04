package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

public class cg_graph extends DefaultDirectedWeightedGraph<String, edge>
		implements Serializable {

	public String gfilename = "";
	public StrongConnectivityInspector<String, edge> sccIns;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public cg_graph() {
		super(edge.class);
	}

	/**
	 * find the parent nodes of the specified node
	 * 
	 * @param n
	 * @return
	 */
	public Vector<String> getParents(String n) {
		Vector<String> ps = new Vector<String>();
		for (edge e : this.incomingEdgesOf(n)) {
			ps.add((String) e.getSource());
		}
		return ps;
	}

	/**
	 * find the children nodes of the specified node
	 * 
	 * @param n
	 * @return
	 */
	public Vector<String> getChildren(String n) {
		Vector<String> cs = new Vector<String>();
		for (edge e : this.outgoingEdgesOf(n)) {
			cs.add((String) e.getTarget());
		}
		return cs;
	}

	/**
	 * dijkstra shortest path algorithm
	 * 
	 * @param start
	 * @return
	 */
	public HashMap<String, Integer> dijkstra(String start) {
		HashMap<String, Integer> dist = new HashMap<String, Integer>();
		HashMap<String, String> previous = new HashMap<String, String>();
		for (String n : this.vertexSet()) {
			dist.put(n, Integer.MAX_VALUE); // 100000 : infinity
			previous.put(n, null); // null : undefined
		}
		dist.put(start, 0);

		Queue<String> Q = new LinkedList<String>();
		Q.addAll(this.vertexSet());
		int smallest;
		String noderemoved = null;
		int v;
		int alt;
		while (!Q.isEmpty()) {
			String first = Q.peek();
			smallest = dist.get(first);
			for (String n : Q) {
				v = dist.get(n);
				if (v <= smallest) {
					smallest = v;
					noderemoved = n;
				}
			}

			if (smallest == Integer.MAX_VALUE)
				return dist;
			Q.remove(noderemoved);
			if (this.outDegreeOf(noderemoved) > 0) {
				for (edge e : this.outgoingEdgesOf(noderemoved)) {
					String n = this.getEdgeTarget(e);
					if (Q.contains(n)) {
						alt = smallest + 1; // 1 : distance(u, v)
						if (alt < dist.get(n)) {
							dist.put(n, alt);
							previous.put(n, noderemoved);
						}
					}
				}
			}
		}
		return dist;
	}

	/**
	 * dijkstra algorithm
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	public HashMap<String, Integer> dijkstra(String start, String end) {
		HashMap<String, Integer> dist = new HashMap<String, Integer>();
		HashMap<String, String> previous = new HashMap<String, String>();
		double start1 = System.currentTimeMillis();
		for (String n : this.vertexSet()) {
			dist.put(n, Integer.MAX_VALUE); // 100000 : infinity
			previous.put(n, null); // null : undefined
		}
		dist.put(start, 0);
		double end1 = System.currentTimeMillis();
		System.out.println("ini time: " + (end1 - start1));

		Vector<String> Q = new Vector<String>();
		Q.addAll(this.vertexSet());
		int smallest;
		String noderemoved = "";
		int v;
		int alt;
		while (!Q.isEmpty()) {
			smallest = dist.get(Q.get(0));
			for (String n : Q) {
				v = dist.get(n);
				if (v <= smallest) {
					smallest = v;
					noderemoved = n;
				}
			}

			if (smallest == Integer.MAX_VALUE)
				return dist;
			else if (noderemoved.equals(end))
				return dist;
			Q.remove(noderemoved);

			for (String n : this.getChildren(noderemoved)) {
				if (Q.contains(n)) {
					alt = smallest + 1; // 1 : distance(u, v)
					if (alt < dist.get(n)) {
						dist.put(n, alt);
						previous.put(n, noderemoved);
					}
				}
			}
		}
		return dist;
	}

	/**
	 * breadth first search return children nodes which are within bound
	 * distance away from start node
	 */
	public HashMap<String, Integer> khopbfsdw(String snode, int bound) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			if (distance >= bound) {
				break;
			}
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1);
				}
			}
		}
		// visited.remove(snode);
		return visited;
	}

	/**
	 * breadth first search return parent nodes which are within bound distance
	 * away from start node
	 */
	public HashMap<String, Integer> khopbfsup(String snode, int bound) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			if (distance >= bound) {
				break;
			}
			for (edge e : this.incomingEdgesOf(n)) {
				String current = this.getEdgeSource(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1);
				}
			}
		}
		visited.remove(snode);
		return visited;
	}

	/**
	 * this procedure computes k hops ancestors and descendants return nodes
	 * which are within bound distance from start node (the underlying graph is
	 * treated as undirected graph)
	 */
	public HashMap<String, Integer> khopNBs(String snode, int bound) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			if (distance >= bound) {
				break;
			}
			for (edge e : this.incomingEdgesOf(n)) {
				String current = this.getEdgeSource(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1);
				}
			}
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1);
				}
			}
		}
		visited.remove(snode);
		return visited;
	}

	/**
	 * breadth first search return distance between node u and node v otherwise
	 * return 0 which represents that node u can not reach node v with distance
	 * bound k
	 */
	public int kReachability(String snode, String enode, int bound) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0, result = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			if (distance > bound) {
				break;
			}
			if (n.equals(enode) && distance <= bound) {
				result = distance;
				break;
			}
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1);
				}
			}
		}
		return result;
	}

	/**
	 * breadth first search
	 * 
	 * @param snode
	 */
	public void bfs(String snode) {
		HashSet<String> visited = new HashSet<String>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.add(snode);
		while (!q.isEmpty()) {
			String n = q.poll();
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.contains(current)) {
					q.add(current);
					visited.add(current); // label the node which has been
											// visited.
				}
			}
		}
	}

	/**
	 * breadth first search with return node set return all visited nodes
	 * associated with distances
	 * 
	 * @param snode
	 * @return
	 */
	public HashMap<String, Integer> bfsreturn(String snode) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1); // label the node which
														// has been visited.
				}
			}
		}
		visited.remove(snode);
		return visited;
	}

	/**
	 * breadth first search with return node set(upwards) return all visited
	 * nodes associated with distances
	 * 
	 * @param snode
	 * @return
	 */
	public HashMap<String, Integer> bfsreturnup(String snode) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			for (edge e : this.incomingEdgesOf(n)) {
				String current = this.getEdgeSource(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1); // label the node which
														// has been visited.
				}
			}
		}
		visited.remove(snode);
		return visited;
	}

	/**
	 * breadth first search with return node set return all visited nodes
	 * 
	 * @param snode
	 * @return
	 */
	public HashSet<String> bfsDOWN(String snode) {
		HashSet<String> visited = new HashSet<String>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.add(snode);
		while (!q.isEmpty()) {
			String n = q.poll();
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.contains(current)) {
					q.add(current);
					visited.add(current); // label the node which has been
											// visited.
				}
			}
		}
		return visited;
	}

	/**
	 * breadth first search traverse from bottom to top
	 * 
	 * @param snode
	 * @return
	 */
	public HashSet<String> bfsUP(String snode) {
		HashSet<String> visited = new HashSet<String>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.add(snode);
		while (!q.isEmpty()) {
			String n = q.poll();
			for (edge e : this.incomingEdgesOf(n)) {
				String current = this.getEdgeSource(e);
				if (!visited.contains(current)) {
					q.add(current);
					visited.add(current);// label the node which has been
											// visited.
				}
			}
		}
		return visited;
	}

	/**
	 * breadth first search when start node meets the end node, the search
	 * halts. return distance between start node and end node
	 * 
	 * @param snode
	 * @param enode
	 * @return
	 */
	public int bfsdistance(String snode, String enode) {
		HashMap<String, Integer> visited = new HashMap<String, Integer>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.put(snode, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			String n = q.poll();
			distance = visited.get(n);
			if (n.equals(enode)) {
				break;
			}
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.keySet().contains(current)) {
					q.add(current);
					visited.put(current, distance + 1);
				}
			}
		}
		if (!visited.keySet().contains(enode)) {
			distance = -1;
		}
		return distance;
	}

	/**
	 * breadth first search when meet the end node, the search halts. return
	 * true if start node can reach end node otherwise return false
	 * 
	 * @param snode
	 * @param enode
	 * @return
	 */
	public boolean bfs(String snode, String enode) {
		boolean result = false;
		HashSet<String> visited = new HashSet<String>();
		Queue<String> q = new LinkedList<String>();
		q.add(snode);
		visited.add(snode);
		while (!q.isEmpty()) {
			String n = q.poll();
			if (n.equals(enode)) {
				result = true;
				break;
			}
			for (edge e : this.outgoingEdgesOf(n)) {
				String current = this.getEdgeTarget(e);
				if (!visited.contains(current)) {
					q.add(current);
					visited.add(current);
				}
			}
		}
		return result;
	}

	/**
	 * bidirectional breadth first search it is better to use two threads to
	 * implement this
	 * 
	 * @param snode
	 * @param enode
	 * @return
	 */
	public boolean bibfs(String snode, String enode) {
		boolean result = false;
		HashSet<String> svisited = new HashSet<String>();
		HashSet<String> evisited = new HashSet<String>();
		Queue<String> q1 = new LinkedList<String>();
		Queue<String> q2 = new LinkedList<String>();
		q1.add(snode);
		q2.add(enode);
		svisited.add(snode);
		evisited.add(enode);
		Search: while (!q1.isEmpty() && !q2.isEmpty()) {
			String n1 = q1.poll();
			String n2 = q2.poll();
			for (edge e : this.outgoingEdgesOf(n1)) {
				String current = this.getEdgeTarget(e);
				if (evisited.contains(current)) {
					result = true;
					break Search;
				} else if (!svisited.contains(current)) {
					q1.add(current);
					svisited.add(current);
				}
			}
			for (edge e : this.incomingEdgesOf(n2)) {
				String current = this.getEdgeSource(e);
				if (svisited.contains(current)) {
					result = true;
					break Search;
				} else if (!evisited.contains(current)) {
					q2.add(current);
					evisited.add(current);
				}
			}
		}
		return result;
	}

	/**
	 * transitive closure computing return a hashmap with key:nodesets structure
	 */
	public HashMap<String, HashSet<String>> transitiveclosure() {
		HashMap<String, HashSet<String>> rNodeset = new HashMap<String, HashSet<String>>();
		for (String n : this.vertexSet()) {
			HashSet<String> hset = (HashSet<String>) this.bfsreturn(n).keySet();
			rNodeset.put(n, hset);
			System.out.println(n + " reachable nodes number "
					+ rNodeset.get(n).size());
		}
		return rNodeset;
	}

	/**
	 * for each node, compute reachable descents and ancestors
	 */
	public Vector<HashMap<String, HashSet<String>>> preProcess() {
		HashMap<String, HashSet<String>> reachAncestor1 = new HashMap<String, HashSet<String>>();
		HashMap<String, HashSet<String>> reachDescendent1 = new HashMap<String, HashSet<String>>();
		TopologicalOrderIterator<String, edge> toi = new TopologicalOrderIterator<String, edge>(
				this);
		Stack<String> reverseOrder = new Stack<String>();
		HashSet<String> cancc = new HashSet<String>();
		HashSet<String> cdess = new HashSet<String>();
		while (toi.hasNext()) {
			String currentnode = toi.next();
			reverseOrder.push(currentnode);
			if (this.inDegreeOf(currentnode) == 0) {
				reachAncestor1.put(currentnode, null);
			}
			cancc = reachAncestor1.get(currentnode);
			for (edge e : this.outgoingEdgesOf(currentnode)) {
				String endnode = (String) e.getTarget();
				HashSet<String> vec = reachAncestor1.get(endnode);
				if (vec == null) {
					vec = new HashSet<String>();
					vec.add(currentnode);
					if (cancc != null) {
						System.out.println("00: " + cancc.size());
						vec.addAll(cancc);
					}
					reachAncestor1.put(endnode, vec);
					vec = null;
				} else {
					vec.add(currentnode);
					if (cancc != null) {
						System.out.println("01: " + cancc.size());
						vec.addAll(cancc);
					}
					reachAncestor1.put(endnode, vec);
					cancc = null;
				}
			}
			cancc = null;
		}

		while (!reverseOrder.empty()) {
			String currentnode = reverseOrder.pop();
			if (this.outDegreeOf(currentnode) == 0) {
				reachDescendent1.put(currentnode, null);
			}
			cdess = reachDescendent1.get(currentnode);
			for (edge e : this.incomingEdgesOf(currentnode)) {
				String startnode = (String) e.getSource();
				HashSet<String> vec = reachDescendent1.get(startnode);
				if (vec == null) {
					vec = new HashSet<String>();
					vec.add(currentnode);
					if (cdess != null) {
						if (cdess.size() > 10000) {
							System.out.println("-----------------------"
									+ currentnode + "-----------------------");
						}
						vec.addAll(cdess);
					}
					reachDescendent1.put(startnode, vec);
					vec = null;
				} else {
					vec.add(currentnode);
					if (cdess != null) {
						System.out.println("11: " + cdess.size());
						vec.addAll(cdess);
					}
					reachDescendent1.put(startnode, vec);
					cancc = null;
				}
			}
			cdess = null;
		}
		Vector<HashMap<String, HashSet<String>>> result = new Vector<HashMap<String, HashSet<String>>>();
		result.add(reachAncestor1); // 0:ancestor
		result.add(reachDescendent1); // 1:descendent
		return result;
	}

	/**
	 * compress by k-hop neighbours actually k=1 : two nodes are in the same
	 * relation iff parent and child node set are equal return : compressed
	 * Graph
	 */
	public cg_graph compressBykhop(
			HashMap<String, Vector<HashSet<String>>> khopinfo) {
		cg_graph condensedGraph = new cg_graph();

		HashMap<Vector<HashSet<String>>, Vector<String>> cluster = new HashMap<Vector<HashSet<String>>, Vector<String>>(); // from
																															// parents
																															// and
																															// children
																															// mapped
																															// to
																															// cluster
		HashMap<String, Vector<String>> clusterMap = new HashMap<String, Vector<String>>(); // super
																							// node
																							// label
																							// mapped
																							// to
																							// cluster
																							// node
																							// set
		HashMap<String, String> nodeMap = new HashMap<String, String>(); // original
																			// node
																			// label
																			// mapped
																			// to
																			// super
																			// node
																			// label

		for (String node : this.vertexSet()) {
			Vector<HashSet<String>> reach = khopinfo.get(node); // retrieve
																// 1-hop info of
																// the node
			Vector<String> clu = cluster.get(reach); // retrieve cluster info by
														// reach info
			if (clu == null) {
				Vector<String> vec = new Vector<String>();
				vec.add(node);
				cluster.put(reach, vec);
			} else {
				clu.add(node);
				cluster.put(reach, clu);
			}
		}

		// initialise vertex set of the condensed graph
		for (Vector<String> nodetag : cluster.values()) {
			String node = new String();
			String tag = new String();
			tag = "";
			for (String s : nodetag) {
				if (tag == "") {
					tag = s;
				} else
					tag = tag + "/" + s;
			}
			node = tag;
			condensedGraph.addVertex(node);
			clusterMap.put(tag, nodetag);

			// associate each vertex in the original graph with the new hyper
			// node in the condensed graph
			for (String s : nodetag) {
				nodeMap.put(s, tag);
			}
		}

		// initialise edge set of the condensed graph
		for (Vector<String> out : cluster.values()) {
			if (out.size() > 0) {
				String node = out.get(0);
				for (edge e : this.outgoingEdgesOf(node)) {
					String oldnode = (String) e.getTarget();
					String newnode = nodeMap.get(oldnode);
					if (condensedGraph.containsEdge(node, newnode) == false)
						condensedGraph.addEdge(node, newnode);
				}

				for (edge e : this.incomingEdgesOf(node)) {
					String oldnode = (String) e.getSource();
					String newnode = nodeMap.get(oldnode);
					if (condensedGraph.containsEdge(newnode, node) == false) {
						condensedGraph.addEdge(newnode, node);
					}
				}
			}
		}
		return condensedGraph;
	}

	/**
	 * cast a default graph to directed graph
	 */
	public DirectedGraph<String, edge> Cast2DG() {
		DirectedGraph<String, edge> DG = new DefaultDirectedGraph<String, edge>(
				edge.class);
		for (String n : this.vertexSet())
			DG.addVertex(n);
		for (edge e : this.edgeSet()) {
			String from = (String) e.getSource();
			String to = (String) e.getTarget();
			DG.addEdge(from, to, e);
		}
		return DG;
	}

	/**
	 * creates a vector from a string
	 */
	public Vector<String> createVectorFromString(String strContent,
			String strDelimiter) {
		Vector<String> vec = new Vector<String>();
		String[] words = strContent.split(strDelimiter);

		for (int i = 0; i < words.length; i++) {
			vec.addElement(words[i]);
		}
		return vec;
	}

	/**
	 * expands an scc node into a node set
	 */
	public HashSet<String> expSCC(String sccnode) {
		String s = sccnode;
		Vector<String> nids = createVectorFromString(s, "_");
		HashSet<String> nset = new HashSet<String>();
		for (String id : nids) {
			nset.add(id);
		}
		return nset;
	}

	/**
	 * constructs a graph from a list of nodes and edges
	 */
	public void ConstructGraphFromVec(String gfname, Vector<String> vlist,
			HashSet<edge> elist) {
		this.gfilename = gfname;
		for (String n : vlist) {
			this.addVertex(n);
		}
		System.out.println("Constructed nodes: " + this.vertexSet().size());
		for (edge e : elist) {
			String fn = (String) e.getSource();
			String tn = (String) e.getTarget();
			this.addEdge(fn, tn, e);
		}
		System.out.println("Constructed edges: " + this.edgeSet().size());
	}

	/**
	 * sort the node in descending order according to degree
	 * 
	 * @param g
	 * @return
	 */
	public Vector<String> sortwithdegree() {
		Vector<String> nodeSet = new Vector<String>();
		nodeSet.addAll(this.vertexSet());
		Comparator<String> comparator = new nodeComparator(this);
		Collections.sort(nodeSet, comparator);
		return nodeSet;
	}

	/**
	 * sort the node in descending order according to degree
	 * 
	 * @param hs
	 *            : hashset as data structure
	 * @return
	 */
	public Vector<String> sortwithdegree(HashSet<String> hs) {
		Vector<String> nodeSet = new Vector<String>();
		nodeSet.addAll(hs);
		Comparator<String> comparator = new nodeComparator(this);
		Collections.sort(nodeSet, comparator);
		return nodeSet;
	}

	/**
	 * randomly pick a node in the graph
	 * 
	 * @return
	 */
	public String getAnode() {
		Vector<String> vlist = new Vector<String>();
		vlist.addAll(this.vertexSet());
		return vlist.elementAt((int) (Math.random() * vlist.size()));
	}

	/**
	 * randomly pick an edge in the graph
	 * 
	 * @return
	 */
	public edge getAedge() {
		Vector<edge> elist = new Vector<edge>();
		elist.addAll(this.edgeSet());
		return elist.elementAt((int) (Math.random() * elist.size()));
	}

	/**
	 * condense strongly connected component to be a node
	 */
	public cg_graph sccDAG() {
		StrongConnectivityInspector<String, edge> sccIns = new StrongConnectivityInspector<String, edge>(
				this);
		List<Set<String>> sccsets = sccIns.stronglyConnectedSets();
		cg_graph sccDAG = new cg_graph();

		HashMap<String, String> clusterMap = new HashMap<String, String>();
		// int i = 0;
		for (Set<String> s : sccsets) {
			String newnode = new String();
			String newtag = "";
			for (String n : s) {
				newtag = newtag + n + "_";
			}
			newtag = newtag.substring(0, newtag.lastIndexOf("_"));
			/**
			 * where there is a very large scc, use below two lines to label the
			 * node, to avoid the long node id.
			 */
			// newtag = String.valueOf(i);
			// i++;
			for (String n : s) {
				clusterMap.put(n, newtag);
			}
			newnode = newtag;
			sccDAG.addVertex(newnode);
		}
		for (Set<String> scc : sccsets) {
			for (String n : scc) {
				String newfrom = clusterMap.get(n);
				if (this.outDegreeOf(n) > 0) {
					for (edge e : this.outgoingEdgesOf(n)) {
						String end = this.getEdgeTarget(e);
						if (!scc.contains(end)) { // get the node outside the
													// current cluster
							String newend = clusterMap.get(end);
							if (!sccDAG.containsEdge(newfrom, newend)) {
								sccDAG.addEdge(newfrom, newend);
							}
						}
					}
				}
			}
		}
		return sccDAG;
	}

	/**
	 * Generate an induced subgraph
	 * 
	 * @param graph
	 * @param nSet
	 * @return
	 */
	public cg_graph insubGraph(Set<String> nSet) {
		cg_graph insubG = new cg_graph();
		for (String n : nSet) {
			insubG.addVertex(n);
		}
		for (String n1 : nSet) {
			for (String n2 : nSet) {
				if (this.getEdge(n1, n2) != null) {
					insubG.addEdge(n1, n2);
				}
			}
		}
		return insubG;
	}

	/**
	 * display information for one graph
	 */
	public void Display(HashMap<String, String> attr) {
		// node set info
		for (String n : this.vertexSet()) {
			System.out.println(n + ": " + attr.get(n));
		}
		for (edge e : this.edgeSet()) {
			System.out.println(e.getSource() + " " + e.getTarget());
		}
	}

	/**
	 * @return a set of strongly connected components
	 */
	public List<Set<String>> sccSet() {
		StrongConnectivityInspector<String, edge> sccIns = new StrongConnectivityInspector<String, edge>(
				this);
		List<Set<String>> sccsets = sccIns.stronglyConnectedSets();
		return sccsets;
	}

	/**
	 * a heuristic to partition a bipartite graph to be a set of bicliques
	 */
	public Vector<Vector<HashSet<String>>> bicliquepartition_backup() {
		Vector<Vector<HashSet<String>>> bcSet = new Vector<Vector<HashSet<String>>>();
		Vector<String> nodeSet = this.sortwithdegree(); // the order of degree
														// is ascending
		HashSet<String> Uset = new HashSet<String>();
		HashSet<String> Vset = new HashSet<String>();
		// initialise Uset : choose the node with the max degree.
		String u = nodeSet.firstElement();
		if (this.outDegreeOf(u) > 0) {
			Uset.add(u);
		} else
			Vset.add(u);
		// greedily pick nodes
		HashSet<String> joinU = new HashSet<String>();
		HashSet<String> joinV = new HashSet<String>();
		while (this.vertexSet() != null) {

			// if all the edges are removed, the loop breaks off
			if (this.edgeSet().size() == 0) {
				break;
			}
			// union children set of each node in the upper node set
			for (String n : Uset) {
				Vector<String> cset = this.getChildren(n);
				if (joinV.size() == 0 && cset != null) {
					joinV.addAll(cset);
				}
				joinV.retainAll(cset);
			}

			// pick a node with the largest degree
			if (!joinV.equals(Vset)) {
				for (int i = 0; i < nodeSet.size(); i++) {
					String v = nodeSet.get(i);
					if (joinV.contains(v) && !Vset.contains(v)) {
						Vset.add(v);
						break;
					}
				}
			}
			// union parent set of each node in the lower node set
			for (String n : Vset) {
				Vector<String> pset = this.getParents(n);
				if (joinU.size() == 0 && pset != null) {
					joinU.addAll(pset);
				}
				joinU.retainAll(pset);
			}

			// pick a node with the largest degree
			if (!joinU.equals(Uset)) {
				for (int i = 0; i < nodeSet.size(); i++) {
					String v = nodeSet.get(i);
					if (joinU.contains(v) && !Uset.contains(v)) {
						Uset.add(v);
						break;
					}
				}
			}

			if (joinU.equals(Uset) && joinV.equals(Vset)) {
				Vector<HashSet<String>> biclique = new Vector<HashSet<String>>();
				HashSet<String> Uuset = new HashSet<String>();
				HashSet<String> Vvset = new HashSet<String>();
				Uuset.addAll(Uset);
				Vvset.addAll(Vset);
				biclique.add(Uuset);
				biclique.add(Vvset);
				this.removeAllVertices(Uset);
				this.removeAllVertices(Vset);
				nodeSet = this.sortwithdegree();
				bcSet.add(biclique);

				joinU.clear();
				joinV.clear();
				Uset.clear();
				Vset.clear();
				if (nodeSet.size() > 0) {
					String uu = nodeSet.firstElement();
					if (this.outDegreeOf(uu) > 0) {
						Uset.add(uu);
					} else
						Vset.add(uu);
				}
			}
		}
		return bcSet;
	}

	public void displaySize() {
		System.out.println("Vertext amount: " + this.vertexSet().size()
				+ " , Edge amount: " + this.edgeSet().size());
	}

	/**
	 * this procedure computes topological rank of the graph
	 */
	public HashMap<String, Integer> topRank() {
		cg_graph DAGP = this.sccDAG(); // shrink the graph as a DAG first
		Stack<String> s = new Stack<String>(); // stack not queue
		HashMap<String, Integer> visited = new HashMap<String, Integer>();

		/**
		 * add one virtual vertex
		 */
		String virtualvertex = "v";
		DAGP.addVertex(virtualvertex);
		for (String v : DAGP.vertexSet()) {
			if (!v.equals(virtualvertex)) {
				if (DAGP.inDegreeOf(v) == 0) {
					DAGP.addEdge(virtualvertex, v);
				}
			}
		}

		int max = 0;
		boolean flag = true;
		s.add(virtualvertex);

		while (!s.isEmpty()) {
			String current = s.peek(); // remains the node in the stack until
										// its rank is evaluated.
			flag = true;
			max = 0;
			for (edge e : DAGP.outgoingEdgesOf(current)) {
				String n = (String) e.getTarget();
				if (!visited.keySet().contains(n)) {
					s.add(n);
					flag = false;
				} else if (visited.keySet().contains(n) && flag) {
					if (max < visited.get(n)) {
						max = visited.get(n);
					}
				}
			}
			if (flag == true) {
				visited.put(current, max + 1);
				s.remove(current);
			}
		}

		// for each node in scc, assign its rank
		visited.remove(virtualvertex);
		HashMap<String, Integer> toprank = new HashMap<String, Integer>();
		for (String hv : visited.keySet()) {
			String[] scc = hv.split("_");
			int rank = visited.get(hv);
			if (scc.length > 1) {
				for (String v : scc) {
					toprank.put(v, rank);
				}
			} else
				toprank.put(hv, rank);
		}
		return toprank;
	}

	/**
	 * this procedure computes a bidirectional bfs tree of G, centered at v
	 * 
	 * @param Q
	 * @param u_l
	 * @return
	 */
	public cg_graph bidirBFS(String v) {
		cg_graph subG = new cg_graph();
		Queue<String> Queue = new LinkedList<String>();
		HashSet<String> visited = new HashSet<String>();
		Queue.add(v);
		visited.add(v);
		subG.addVertex(v);
		while (!Queue.isEmpty()) {
			String node = Queue.poll();
			for (edge e : this.outgoingEdgesOf(node)) {
				String tv = (String) e.getTarget();
				if (!visited.contains(tv)) {
					Queue.add(tv);
					visited.add(tv);
					subG.addVertex(tv);
					subG.addEdge(node, tv);
				}
			}
		}
		Queue.add(v);
		while (!Queue.isEmpty()) {
			String node = Queue.poll();
			for (edge e : this.incomingEdgesOf(node)) {
				String fv = (String) e.getSource();
				if (!visited.contains(fv)) {
					Queue.add(fv);
					visited.add(fv);
					subG.addVertex(fv);
					subG.addEdge(fv, node);
				}
			}
		}
		return subG;
	}

}