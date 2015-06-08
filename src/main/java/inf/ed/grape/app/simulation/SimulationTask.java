//package inf.ed.grape.app.simulation;
//
//import inf.ed.grape.graph.Partition;
//import inf.ed.grape.interfaces.LocalComputeTask;
//import inf.ed.grape.interfaces.Message;
//import inf.ed.graph.structure.Graph;
//import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
//import it.unimi.dsi.fastutil.ints.IntSet;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map.Entry;
//import java.util.Queue;
//import java.util.Set;
//
//public class SimulationTask extends LocalComputeTask {
//
//	public HashMap<Integer, HashSet<Integer>> sim; // node -> match nodes
//	public HashMap<Integer, HashSet<Integer>> premv; // node -> cannot
//														// matchparents
//	private HashMap<Integer, HashSet<Integer>> pre; // node -> parents
//	private HashMap<Integer, HashSet<Integer>> suc; // node -> children
//	private HashSet<Integer> PSET; // set of all parent nodes
//
//	public HashMap<Integer, HashSet<Integer>> toRet = new HashMap<Integer, HashSet<Integer>>();
//	public HashSet<Integer> gil = new HashSet<Integer>();
//	public HashMap<Integer, HashSet<Integer>> rvset = new HashMap<Integer, HashSet<Integer>>();
//	public HashSet<Integer> truth = new HashSet<Integer>();
//
//	/**
//	 * Constructor
//	 */
//	public SimulationTask() {
//		this.sim = new HashMap<Integer, HashSet<Integer>>(); // node -> match
//																// nodes
//		this.premv = new HashMap<Integer, HashSet<Integer>>(); // node -> can
//																// not match
//																// parents
//		this.pre = new HashMap<Integer, HashSet<Integer>>(); // node -> parents
//		this.suc = new HashMap<Integer, HashSet<Integer>>(); // node -> children
//		this.PSET = new HashSet<Integer>(); // set of all parent nodes
//	}
//
//	/**
//	 * this procedure computes simulation relation between P and G G inclueds a
//	 * set of output nodes which should not be included in the results
//	 * 
//	 * @param P
//	 * @param G
//	 * @return
//	 */
//	public ArrayList<Message> EfficientSimilarity(cg_graph P, cg_graph G) {
//		ArrayList<Message> messages = new ArrayList<Message>();
//		System.out.println("Started partial evaluation, total size: " + P.vertexSet().size());
//		int vnum = 0;
//		for (cg_node u : P.vertexSet()) {
//			if (vnum % 10000 == 0)
//				System.out.println(vnum + "\r\r");
//			HashSet<cg_node> posmat = new HashSet<cg_node>(); // a node set
//																// which
//																// contains
//																// nodes that
//																// possibly
//																// match parents
//																// of v
//			HashSet<cg_node> remove = new HashSet<cg_node>(); // a node set
//																// which
//																// contains
//																// nodes that
//																// can not match
//																// any parent
//																// node of v
//			remove.addAll(this.PSET); // all parents node
//
//			HashSet<cg_node> simset = this.sim.get(u); // sim(u)
//			if (simset == null) {
//				simset = new HashSet<cg_node>(); // initialize simset
//			}
//
//			if (P.outDegreeOf(u) == 0) {
//				for (cg_node v : G.vertexSet()) {
//					if (this.check(u.a, v.a) && v.isOut == false) { //
//						simset.add(v);
//						truth.add(v);
//						if (!this.pre.get(v).isEmpty()) { // v.parent is not
//															// empty
//							posmat.addAll(this.pre.get(v)); // add all v's
//															// parent to posmat
//						}
//					}
//				}
//			} else {
//				for (cg_node v : G.vertexSet()) {
//					// String lv = attrG.get(v);
//					if (this.check(u.a, v.a)) {
//						if (G.outDegreeOf(v) != 0 && v.isOut == true) {
//							simset.add(v);
//							if (!this.pre.get(v).isEmpty()) {
//								posmat.addAll(this.pre.get(v));
//							}
//						} else if (v.isInn == true) {
//							messages.add(new Message(u, v));
//						}
//					}
//				}
//			}
//			this.sim.put(u, simset);
//			remove.removeAll(posmat);
//			this.premv.put(u, remove);
//		}
//
//		Queue<cg_node> q = new LinkedList<cg_node>(); // those node with non
//														// empty premv
//		for (cg_node n : this.premv.keySet()) {
//			HashSet<cg_node> hs = this.premv.get(n);
//			if (!hs.isEmpty()) {
//				q.add(n);
//			}
//		}
//
//		// check for isolated node
//		while (!q.isEmpty()) {
//			System.out.println("queue size is " + q.size() + "\r\r");
//			cg_node n = q.poll();
//			for (cg_edge e : P.incomingEdgesOf(n)) {
//				cg_node u = P.getEdgeSource(e);
//				HashSet<cg_node> sim = this.sim.get(u);
//				for (cg_node w : this.premv.get(n)) {
//					if (sim.contains(w)) {
//						sim.remove(w); // w in G can not match u in P
//						if (w.isInn == true) {
//							messages.add(new Message(u, w));
//						}
//						for (cg_edge ee : G.incomingEdgesOf(w)) {
//							cg_node ww = G.getEdgeSource(ee);
//							HashSet<cg_node> cset = new HashSet<cg_node>();
//							cset.addAll(this.suc.get(ww));
//							cset.retainAll(sim);
//							if (cset.isEmpty()) {
//								this.premv.get(u).add(ww);
//								if (!q.contains(u)) {
//									q.add(u);
//								}
//							}
//						}
//					}
//				}
//			}
//			this.premv.get(n).clear();
//		}
//
//		// cg_graph resGraph = genResultGraph(P, G);
//		// System.out.println(resGraph.vertexSet().size());
//		/*
//		 * for (cg_edge edge : P.edgeSet()) { cg_node fnode = edge.from_node;
//		 * cg_node tnode = edge.to_node; HashSet<cg_node> fnodes =
//		 * this.sim.get(fnode); HashSet<cg_node> tnodes = this.sim.get(tnode);
//		 * for (cg_node fn : fnodes) { for (cg_node tn : tnodes) { if
//		 * (G.containsEdge(fn, tn)) { System.out.println(fn.toString() + "->" +
//		 * tn.toString()); } } } }
//		 */
//		return messages;
//	}
//
//	public ArrayList<Message> remove(cg_node status, cg_node node, cg_graph P, cg_graph G) {
//		ArrayList<Message> messages = new ArrayList<Message>();
//		HashSet<cg_node> simu = this.sim.get(status);
//		Queue<cg_node> q = new LinkedList<cg_node>(); // those node with non
//														// empty premv
//		// assert(simu.contains(node));
//		if (!simu.contains(node))
//			return messages;
//		simu.remove(node); // node in G can not match status in P
//		// if (this.sim.get(status).contains(node))
//		// TODO: double-check here
//		if (node.isInn == true) {
//			System.out.println("IS INN NODE");
//			messages.add(new Message(status, node));
//		}
//
//		for (cg_edge ee : G.incomingEdgesOf(node)) {
//			cg_node ww = G.getEdgeSource(ee);
//			HashSet<cg_node> cset = new HashSet<cg_node>();
//			cset.addAll(this.suc.get(ww));
//			cset.retainAll(simu);
//			if (cset.isEmpty()) {
//				this.premv.get(status).add(ww);
//				if (!q.contains(status)) {
//					q.add(status);
//				}
//			}
//		}
//
//		while (!q.isEmpty()) {
//			cg_node n = q.poll();
//			for (cg_edge e : P.incomingEdgesOf(n)) {
//				cg_node u = P.getEdgeSource(e);
//				HashSet<cg_node> sim = this.sim.get(u);
//				for (cg_node w : this.premv.get(n)) {
//					if (sim.contains(w)) {
//						sim.remove(w); // w in G can not match u in P
//						if (w.isInn == true) {
//							messages.add(new Message(u, w));
//						}
//						for (cg_edge ee : G.incomingEdgesOf(w)) {
//							cg_node ww = G.getEdgeSource(ee);
//							HashSet<cg_node> cset = new HashSet<cg_node>();
//							cset.addAll(this.suc.get(ww));
//							cset.retainAll(sim);
//							if (cset.isEmpty()) {
//								this.premv.get(u).add(ww);
//								if (!q.contains(u)) {
//									q.add(u);
//								}
//							}
//						}
//					}
//				}
//			}
//			this.premv.get(n).clear();
//		}
//		return messages;
//	}
//
//	/**
//	 * This procedure compares the search condition with the attribute values of
//	 * the data node
//	 * 
//	 * @param operator
//	 * @param searchvalue
//	 * @param value
//	 * @return
//	 */
//	public boolean check(String[] searchcondition, String[] value) {
//		/*
//		 * if (!searchcondition[0].equalsIgnoreCase("ANY")) { if(value[0] ==
//		 * null || !searchcondition[0].contains(value[0])){ return false; } }
//		 */
//
//		if (!searchcondition[1].equalsIgnoreCase("ANY")) {
//			if (value[1] == null) {
//				return false;
//			}
//			if (!searchcondition[1].equals(value[1])) {
//				return false;
//			}
//			/*
//			 * float searchu = Float.valueOf(searchcondition[1]); float valuev =
//			 * Float.valueOf(value[1].trim()); if(valuev < searchu){ return
//			 * false; }
//			 */
//		}
//		/*
//		 * if (!searchcondition[2].equalsIgnoreCase("ANY")) { if(value[2] ==
//		 * null || !searchcondition[2].contains(value[2])){ return false; } }
//		 */
//		return true;
//	}
//
//	public void output() {
//		for (cg_node u : this.sim.keySet()) {
//			HashSet<cg_node> vset = this.sim.get(u);
//			String s = "";
//			for (cg_node v : vset) {
//				s = v + ", " + s;
//			}
//			System.out.println(u + ": " + s);
//		}
//	}
//
//	/**
//	 * this procedure computes result graph based on the simulation relation
//	 * 
//	 * @param P
//	 * @param G
//	 * @return
//	 */
//
//	public cg_graph genResultGraph(cg_graph P, cg_graph G) {
//		cg_graph resultgraph = new cg_graph();
//
//		int outNodeNum = 0;
//		int inNodeNum = 0;
//		for (Entry<cg_node, HashSet<cg_node>> entry : this.sim.entrySet()) {
//			for (cg_node node : entry.getValue()) {
//				if (node.isInn) {
//					inNodeNum++;
//				} else if (node.isOut) {
//					outNodeNum++;
//				}
//			}
//		}
//		System.out.println("In node num: " + inNodeNum);
//		System.out.println("Out node num: " + outNodeNum);
//		for (cg_edge eu : P.edgeSet()) {
//			cg_node fnu = eu.getSource();
//			cg_node tnu = eu.getTarget();
//			System.out.println("fnu" + fnu.toString() + "  tnu " + tnu.toString());
//
//			HashSet<cg_node> fnumat = this.sim.get(fnu);
//			HashSet<cg_node> tnumat = this.sim.get(tnu);
//			System.out.println(fnumat.size() + " this is funmat size");
//			System.out.println(tnumat.size() + " this is tunmat size");
//
//			for (cg_node fv : fnumat) {
//				for (cg_node tv : tnumat) {
//					if (!resultgraph.containsVertex(fv)) {
//						resultgraph.addVertex(fv);
//					}
//					if (!resultgraph.containsVertex(tv)) {
//						resultgraph.addVertex(tv);
//					}
//					if (G.containsEdge(fv, tv)) {
//						cg_edge ev = new cg_edge(fv, tv);
//						resultgraph.addEdge(fv, tv, ev);
//					}
//				}
//			}
//		}
//		return resultgraph;
//	}
//
//	public cg_graph patternGen() {
//		cg_graph P = new cg_graph();
//		cg_node n1 = new cg_node("1", "B", "", "");
//		cg_node n2 = new cg_node("2", "B", "", "");
//		cg_node n3 = new cg_node("3", "D", "", "");
//
//		P.addVertex(n1);
//		P.addVertex(n2);
//		P.addVertex(n3);
//
//		cg_edge e2 = new cg_edge(n2, n1);
//		cg_edge e3 = new cg_edge(n3, n2);
//		cg_edge e4 = new cg_edge(n3, n1);
//
//		P.addEdge(n2, n1, e2);
//		P.addEdge(n3, n2, e3);
//		P.addEdge(n3, n1, e4);
//		return P;
//	}
//
//	private void initIndex(Partition partition){
//		for (int vID : partition..allVertices().keySet()) {
//			HashSet<Integer> pset = new HashSet<Integer>(); // initialise pset
//			for (int parentID : g.getParents(vID)) {
//				pset.add(parentID);
//			}
//			this.pre.put(vID, pset);
//
//			if (pset.size() > 0) {
//				PSET.addAll(pset);
//			}
//
//			HashSet<Integer> cset = new HashSet<Integer>(); // initialise cset
//			for (int childID : g.getChildren(vID)) {
//				cset.add(childID);
//			}
//			this.suc.put(vID, cset);
//		}
//	}
//
//	@Override
//	public void compute(Partition partition) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void incrementalCompute(Partition partition, List<Message<?>> incomingMessages) {
//		// TODO Auto-generated method stub
//
//	}
//
//}
