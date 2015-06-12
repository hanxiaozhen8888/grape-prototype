package inf.ed.grape.app.simulation;

import inf.ed.grape.graph.Partition;
import inf.ed.grape.interfaces.LocalComputeTask;
import inf.ed.grape.interfaces.Message;
import inf.ed.graph.structure.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimulationTask extends LocalComputeTask {

	public HashMap<Integer, HashSet<Integer>> sim; // node -> match nodes
	public HashMap<Integer, HashSet<Integer>> premv; // node -> cannot match
														// parents
	private HashMap<Integer, HashSet<Integer>> pre; // node -> parents
	private HashMap<Integer, HashSet<Integer>> suc; // node -> children
	private HashSet<Integer> PSET; // set of all parent nodes

	static Logger log = LogManager.getLogger(SimulationTask.class);

	private Pattern pattern;

	/**
	 * Constructor
	 */
	public SimulationTask() {
		this.sim = new HashMap<Integer, HashSet<Integer>>(); // node -> match
																// nodes
		this.premv = new HashMap<Integer, HashSet<Integer>>(); // node -> can
																// not match
																// parents
		this.pre = new HashMap<Integer, HashSet<Integer>>(); // node -> parents
		this.suc = new HashMap<Integer, HashSet<Integer>>(); // node -> children
		this.PSET = new HashSet<Integer>(); // set of all parent nodes
	}

	public String displayResult() {
		String ret = "";
		for (int u : this.sim.keySet()) {
			HashSet<Integer> vset = this.sim.get(u);
			String s = "";
			for (Integer v : vset) {
				s = v + ", " + s;
			}
			ret += (u + ": " + s + "\n");
		}
		return ret;
	}

	private void initIndex(Partition partition) {
		for (int vID : partition.getGraph().allVertices().keySet()) {
			HashSet<Integer> pset = new HashSet<Integer>(); // initialise pset
			for (int parentID : partition.getGraph().getParents(vID)) {
				pset.add(parentID);
			}
			this.pre.put(vID, pset);

			if (pset.size() > 0) {
				PSET.addAll(pset);
			}

			HashSet<Integer> cset = new HashSet<Integer>(); // initialise cset
			for (int childID : partition.getGraph().getChildren(vID)) {
				cset.add(childID);
			}
			this.suc.put(vID, cset);
		}
	}

	@Override
	public void compute(Partition partition) {

		pattern = (Pattern) this.query;

		this.initIndex(partition);

		log.debug("Init finished. Start partial evaluation. pattern vertex size = "
				+ pattern.getGraph().vertexSize());

		for (int u : pattern.getGraph().allVertices().keySet()) {

			HashSet<Integer> posmat = new HashSet<Integer>();
			/* a node set which contains nodes that possibly match parents of v */
			HashSet<Integer> remove = new HashSet<Integer>();
			/* node set contains nodes that cannot match any parent node of v */

			remove.addAll(this.PSET); // all parents node

			HashSet<Integer> simset = this.sim.get(u); // sim(u)
			if (simset == null) {
				simset = new HashSet<Integer>(); // initialize simset
			}

			if (pattern.getGraph().getChildren(u).size() == 0) {
				// u has no children.
				for (int v : partition.getGraph().allVertices().keySet()) {
					if (partition.getGraph().getVertex(v).match(pattern.getGraph().getVertex(u))) {
						// regardless v has child or not.
						simset.add(v);
						if (!this.pre.get(v).isEmpty()) {
							/* v.parent is not empty */
							posmat.addAll(this.pre.get(v));
							/* add all v's parent to posmat */
						}
					}
				}
			} else {
				// u has children, then v should have children.
				for (int v : partition.getGraph().allVertices().keySet()) {
					if (partition.getGraph().getVertex(v).match(pattern.getGraph().getVertex(u))) {
						if (!partition.getGraph().getChildren(v).isEmpty()
								|| partition.isVirtualVertex(v)) {
							// v has children or v is virtual vertex.
							simset.add(v);
							if (!this.pre.get(v).isEmpty()) {
								posmat.addAll(this.pre.get(v));
							}
						} else if (partition.isIncomingVertex(v)) {
							// else v is a match of u, but has no children.
							log.error("Never run into this line");
							Message<Pair<Integer>> m = new Message<Pair<Integer>>(
									this.getPartitionID(), v, new Pair<Integer>(u, v));
							generatedMessages.add(m);
						}
					}
				}
			}

			this.sim.put(u, simset);
			remove.removeAll(posmat);
			this.premv.put(u, remove);

			log.info("u=" + u + ", sim= " + simset.toString() + " premv= " + remove.toString());
		}

		Queue<Integer> q = new LinkedList<Integer>(); // those node with non
														// empty premv
		for (int n : this.premv.keySet()) {
			HashSet<Integer> hs = this.premv.get(n);
			if (!hs.isEmpty()) {
				q.add(n);
			}
		}

		log.debug("current Queue=" + q.toString());

		// check for isolated node
		while (!q.isEmpty()) {

			int n = q.poll();
			for (int u : pattern.getGraph().getParents(n)) {
				HashSet<Integer> sim = this.sim.get(u);
				for (int w : this.premv.get(n)) {
					if (sim.contains(w)) {
						sim.remove(w); // w in G can not match u in P

						log.debug("remove " + w);
						if (!partition.isIncomingVertex(w) && !partition.isVirtualVertex(w)) {
							log.debug(w + "is inner vertex");
							for (int parent : partition.getGraph().getParents(w)) {
								if (partition.isIncomingVertex(parent)) {
									log.debug("add new message: n=" + n + ", u=" + u + ", sim="
											+ sim + ", w=" + w + ", parent = " + parent);
									Message<Pair<Integer>> m = new Message<Pair<Integer>>(
											this.getPartitionID(), w, new Pair<Integer>(u, parent));
									generatedMessages.add(m);
								}
							}
						}
						for (int ww : partition.getGraph().getParents(w)) {
							HashSet<Integer> cset = new HashSet<Integer>();
							cset.addAll(this.suc.get(ww));
							cset.retainAll(sim);
							if (cset.isEmpty()) {
								this.premv.get(u).add(ww);
								if (!q.contains(u)) {
									q.add(u);
								}
							}
						}
					}
				}
			}
			this.premv.get(n).clear();
		}

		log.info("init compute finished. partial evaluation result:");
		log.debug(this.displayResult());
		log.debug("message size:" + this.generatedMessages.size());
		log.debug(generatedMessages.toString());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void incrementalCompute(Partition partition, List<Message<?>> incomingMessages) {

		log.debug("this is incremental computation. with incomming messages size = "
				+ incomingMessages.size());

		for (Message<?> message : incomingMessages) {

			Pair<Integer> pair = (Pair<Integer>) message.getContent();
			int u = pair.x;
			int v = pair.y;

			HashSet<Integer> simu = this.sim.get(u);
			Queue<Integer> q = new LinkedList<Integer>(); // those node with non
			// empty premv
			// assert(simu.contains(node));
			if (!simu.contains(u))
				return;
			simu.remove(u); // node in G can not match status in P
			// if (this.sim.get(status).contains(node))
			// TODO: double-check here
			if (partition.isIncomingVertex(v)) {
				// System.out.println();
				log.warn("virtual vertex: " + v);
				Message<Pair<Integer>> m = new Message<Pair<Integer>>(this.getPartitionID(), v,
						new Pair<Integer>(u, v));
				generatedMessages.add(m);
			}

			for (int ww : partition.getGraph().getParents(v)) {
				HashSet<Integer> cset = new HashSet<Integer>();
				cset.addAll(this.suc.get(ww));
				cset.retainAll(simu);
				if (cset.isEmpty()) {
					this.premv.get(u).add(ww);
					if (!q.contains(u)) {
						q.add(u);
					}
				}
			}

			while (!q.isEmpty()) {
				int n = q.poll();
				for (int uu : partition.getGraph().getParents(n)) {
					HashSet<Integer> sim = this.sim.get(uu);
					for (int w : this.premv.get(n)) {
						if (sim.contains(w)) {
							sim.remove(w); // w in G can not match u in P
							if (partition.isIncomingVertex(w)) {
								Message<Pair<Integer>> m = new Message<Pair<Integer>>(
										this.getPartitionID(), w, new Pair<Integer>(uu, w));
								generatedMessages.add(m);
							}
							for (int ww : partition.getGraph().getParents(w)) {
								HashSet<Integer> cset = new HashSet<Integer>();
								cset.addAll(this.suc.get(ww));
								cset.retainAll(sim);
								if (cset.isEmpty()) {
									this.premv.get(uu).add(ww);
									if (!q.contains(uu)) {
										q.add(uu);
									}
								}
							}
						}
					}
				}
				this.premv.get(n).clear();
			}

		}

		log.info("incremental compute finished. Ievaluation result:");
		log.debug(this.displayResult());
		log.debug("message size:" + this.generatedMessages.size());
		log.debug(generatedMessages.toString());
	}

}
