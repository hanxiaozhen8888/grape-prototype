package ed.inf.grape.app;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.graph.Edge;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.interfaces.LocalComputeTask;
import ed.inf.grape.interfaces.Message;

public class PageRankTask extends LocalComputeTask {

	static Logger log = LogManager.getLogger(PageRankTask.class);

	@Override
	public void compute(Partition partition) {

		log.debug("local compute.");

		double initValue = 0.5;

		for (int vertex : partition.vertexSet()) {

			if (partition.isInnerVertex(vertex)) {

				((PageRankResult) this.getResult()).ranks
						.put(vertex, initValue);

				int numOutgoingEdges = partition.outDegreeOf(vertex);
				for (Edge edge : partition.outgoingEdgesOf(vertex)) {
					Message<Double> message = new Message<Double>(
							this.getPartitionID(),
							partition.getEdgeTarget(edge), initValue
									/ numOutgoingEdges);
					this.getMessages().add(message);
				}
			}

		}
	}

	@Override
	public void incrementalCompute(Partition partition,
			List<Message> incomingMessages) {

		log.debug("local incremental compute.");
		log.debug("incommingMessages.size = " + incomingMessages.size());

		Map<Integer, Double> aggregateByVertex = new HashMap<Integer, Double>();
		for (Message<Double> m : incomingMessages) {

			double oldv = 0.0;
			if (aggregateByVertex.containsKey(m.getDestinationVertexID())) {
				oldv = aggregateByVertex.get(m.getDestinationVertexID());
			}
			aggregateByVertex.put(m.getDestinationVertexID(),
					oldv + m.getContent());
		}

		log.debug("aggregateIncommingMessages.size = "
				+ aggregateByVertex.size());

		for (Entry<Integer, Double> entry : aggregateByVertex.entrySet()) {

			int source = entry.getKey();
			int numOutgoingEdges = 1 + partition.outDegreeOf(source);

			if (this.getSuperstep() < 10) {

				double updatedRank = (0.15 / numOutgoingEdges + 0.85 * entry
						.getValue());

				((PageRankResult) this.getResult()).ranks.put(source,
						updatedRank);

				for (Edge edge : partition.outgoingEdgesOf(source)) {

					Message<Double> message = new Message<Double>(
							this.getPartitionID(),
							partition.getEdgeTarget(edge), updatedRank
									/ numOutgoingEdges);
					this.getMessages().add(message);
				}

			}
		}
	}

}