package ed.inf.grape.core;

import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.communicate.Client2Coordinator;
import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.communicate.WorkerProxy;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.Config;
import ed.inf.grape.util.KV;

/**
 * The Class Coordinator.
 * 
 * @author yecol
 */
@SuppressWarnings("deprecation")
public class Coordinator extends UnicastRemoteObject implements
		Worker2Coordinator, Client2Coordinator {

	private static final long serialVersionUID = 7264167926318903124L;

	/** The master thread. */
	// private Thread masterThread;

	/** The total number of worker threads. */
	private static AtomicInteger totalWorkerThreads = new AtomicInteger(0);

	// /** The health manager *. */
	// private HealthManager healthManager;

	/** The workerID to WorkerProxy map. */
	private Map<String, WorkerProxy> workerProxyMap = new ConcurrentHashMap<String, WorkerProxy>();

	/** The workerID to Worker map. **/
	private Map<String, Worker> workerMap = new HashMap<String, Worker>();

	/** The partitionID to workerID map. **/
	private Map<Integer, String> partitionWorkerMap;

	/** The virtual vertexID to partitionID map. */
	private Map<String, Integer> virtualVertexPartitionMap;

	/** Set of Workers maintained for acknowledgement. */
	private Set<String> workerAcknowledgementSet = new HashSet<String>();

	/** Set of workers who will be active in the next super step. */
	private Set<String> activeWorkerSet = new HashSet<String>();

	/** The start time. */
	long startTime;

	long superstep = 0;

	/** Partition manager. */
	private Partitioner partitioner;

	/** The result queue. */
	private BlockingQueue<String> resultQueue = new LinkedBlockingDeque<String>();

	static Logger log = LogManager.getLogger(Coordinator.class);

	/**
	 * Instantiates a new coordinator.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 * @throws PropertyNotFoundException
	 *             the property not found exception
	 */
	public Coordinator() throws RemoteException {
		super();

	}

	/**
	 * Gets the active worker set.
	 * 
	 * @return the active worker set
	 */
	public Set<String> getActiveWorkerSet() {
		return activeWorkerSet;
	}

	/**
	 * Sets the active worker set.
	 * 
	 * @param activeWorkerSet
	 *            the new active worker set
	 */
	public void setActiveWorkerSet(Set<String> activeWorkerSet) {
		this.activeWorkerSet = activeWorkerSet;
	}

	/**
	 * Registers the worker computation nodes with the master.
	 * 
	 * @param worker
	 *            Represents the {@link system.WorkerImpl Worker}
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            Represents the number of worker threads available in the
	 *            worker computation node
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */
	public Worker2Coordinator register(Worker worker, String workerID,
			int numWorkerThreads) throws RemoteException {

		log.debug("Coordinator: Register");
		totalWorkerThreads.getAndAdd(numWorkerThreads);
		WorkerProxy workerProxy = new WorkerProxy(worker, workerID,
				numWorkerThreads, this);
		workerProxyMap.put(workerID, workerProxy);
		workerMap.put(workerID, worker);
		return (Worker2Coordinator) UnicastRemoteObject.exportObject(
				workerProxy, 0);
	}

	/**
	 * Gets the worker proxy map info.
	 * 
	 * @return Returns the worker proxy map info
	 */
	public Map<String, WorkerProxy> getWorkerProxyMap() {
		return workerProxyMap;
	}

	/**
	 * Send worker partition info.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void sendWorkerPartitionInfo() throws RemoteException {
		log.debug("Coordinator: sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setWorkerPartitionInfo(virtualVertexPartitionMap,
					partitionWorkerMap, workerMap);
		}
	}

	public void sendQuery(Query query) throws RemoteException {
		log.debug("Coordinator: sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setQuery(query);
		}
	}

	/**
	 * Sets the initial message for the Worker that has the source vertex.
	 * 
	 * @param <T>
	 *            the generic type
	 * @param sourceVertex_partitionID
	 *            the source vertex partition id
	 * @param sourceVertexID
	 *            the source vertex id
	 * @param initData
	 *            the data
	 * @throws RemoteException
	 *             the remote exception
	 */
	/*
	 * private <T> void setInitialMessage(int sourceVertex_partitionID, long
	 * sourceVertexID, Data<T> initData) throws RemoteException {
	 * System.out.println("Master: setInitialMessage"); List<Message>
	 * messageList = new ArrayList<>(); messageList.add(new Message(null,
	 * initData)); Map<VertexID, List<Message>> map = new HashMap<>(); VertexID
	 * sourceVertex = new VertexID(sourceVertex_partitionID, sourceVertexID);
	 * map.put(sourceVertex, messageList); ConcurrentHashMap<Integer,
	 * Map<VertexID, List<Message>>> initialMessage = new ConcurrentHashMap<>();
	 * initialMessage.put(sourceVertex_partitionID, map);
	 * workerProxyMap.get(activeWorkerSet.toArray()[0]).setInitialMessage(
	 * initialMessage);
	 * 
	 * }
	 */

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.setSecurityManager(new RMISecurityManager());
		Coordinator coordinator;
		try {
			coordinator = new Coordinator();
			Registry registry = LocateRegistry.createRegistry(KV.RMI_PORT);
			registry.rebind(KV.COORDINATOR_SERVICE_NAME, coordinator);
			log.info("Coordinator instance is bound to " + KV.RMI_PORT
					+ " and ready.");
		} catch (RemoteException e) {
			Coordinator.log.error(e);
			e.printStackTrace();
		}
	}

	/**
	 * Halts all the workers and prints the final solution.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		// healthManager.exit();
		log.info("Master: halt");
		log.debug("Worker Proxy Map " + workerProxyMap);

		// String outputDir = null;
		// try {
		// outputDir = Props.getInstance().getStringProperty("OUTPUT_DIR");
		// } catch (PropertyNotFoundException e) {
		// e.printStackTrace();
		// }
		//
		// // Create the output dir if it doesn't exist
		// File file = new File(outputDir);
		// if (!file.exists()) {
		// file.mkdirs();
		// }

		// String outputFilePath = outputDir + File.separator
		// + System.currentTimeMillis() + ".txt";

		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			// workerProxy.writeOutput(outputFilePath);
			workerProxy.halt();
		}

		// healthManager.exit();
		long endTime = System.currentTimeMillis();
		log.info("Time taken: " + (endTime - startTime) + " ms");
		// Restore the system back to its initial state
		restoreInitialState();
		// Inform the client about the result.
		// resultQueue.add(outputFilePath);

	}

	/**
	 * Restore initial state of the system.
	 */
	private void restoreInitialState() {
		this.activeWorkerSet.clear();
		this.workerAcknowledgementSet.clear();
		this.partitionWorkerMap.clear();
		// this.superstep = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */

	/**
	 * Removes the worker.
	 * 
	 * @param workerID
	 *            the worker id
	 */
	public void removeWorker(String workerID) {
		workerProxyMap.remove(workerID);
		workerMap.remove(workerID);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker2Master#superStepCompleted(java.lang.String,
	 * java.util.Set)
	 */
	// @Override
	// public synchronized void superStepCompleted(String workerID,
	// Set<String> activeWorkerSet) throws RemoteException {
	// // System.out.println("Master: superStepCompleted");
	// // System.out.println("Acknowledgment from Worker: " + workerID +
	// // " - activeWorkerSet " + activeWorkerSet);
	// this.activeWorkerSet.addAll(activeWorkerSet);
	// this.workerAcknowledgementSet.remove(workerID);
	// // System.out.println("WorkerAcknowledgmentSet: " +
	// // this.workerAcknowledgementSet);
	// // If the acknowledgment has been received from all the workers, start
	// // the next superstep
	// if (this.workerAcknowledgementSet.size() == 0) {
	// // System.out.println("Acknowledgment received from all workers " +
	// // activeWorkerSet);
	// superstep++;
	// if (activeWorkerSet.size() != 0)
	// startSuperStep();
	// else
	// halt();
	// }
	// }

	/**
	 * Gets the partition worker map.
	 * 
	 * @return the partition worker map
	 */
	public Map<Integer, String> getPartitionWorkerMap() {
		return partitionWorkerMap;
	}

	/**
	 * Sets the partition worker map.
	 * 
	 * @param partitionWorkerMap
	 *            the partition worker map
	 */
	public void setPartitionWorkerMap(Map<Integer, String> partitionWorkerMap) {
		this.partitionWorkerMap = partitionWorkerMap;
	}

	/**
	 * Defines a deployment convenience to stop each registered.
	 * 
	 * @throws RemoteException
	 *             the remote exception {@link system.Worker Worker} and then
	 *             stops itself.
	 */

	public void shutdown() throws RemoteException {
		// if (healthManager != null)
		// healthManager.exit();
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			try {
				workerProxy.shutdown();
			} catch (Exception e) {
				continue;
			}
		}
		java.util.Date date = new java.util.Date();
		log.info("Master goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	public void loadGraph(String graphFilename) throws RemoteException {

		log.info("load Graph = " + graphFilename);

		startTime = System.currentTimeMillis();

		assignDistributedPartitions();
		sendWorkerPartitionInfo();
	}

	public void putTask(Query query) throws RemoteException {
		// TODO Auto-generated method stub

		log.info("receive task with query = " + query);

		sendQuery(query);
		nextLocalCompute();
	}

	public void assignDistributedPartitions() {

		/**
		 * Graph file has been partitioned, and the partitioned graph have been
		 * distributed to workers.
		 * 
		 * */

		partitioner = new Partitioner(Partitioner.STRATEGY_METIS);
		partitionWorkerMap = new HashMap<Integer, String>();

		int totalPartitions = partitioner.getNumOfPartitions(), partitionID;

		// Assign partitions to workers in the ratio of the number of worker
		// threads that each worker has.
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {

			WorkerProxy workerProxy = entry.getValue();

			// Compute the number of partitions to assign
			int numThreads = workerProxy.getNumThreads();
			double ratio = ((double) (numThreads)) / totalWorkerThreads.get();
			log.info("Worker " + workerProxy.getWorkerID());
			log.info("ThreadNum = " + numThreads + ", Ratio: " + ratio);
			int numPartitionsToAssign = (int) (ratio * totalPartitions);
			log.info("numPartitionsToAssign: " + numPartitionsToAssign);

			List<Integer> workerPartitionIDs = new ArrayList<Integer>();
			for (int i = 0; i < numPartitionsToAssign; i++) {
				if (partitioner.hasNextPartitionID()) {
					partitionID = partitioner.getNextPartitionID();

					activeWorkerSet.add(entry.getKey());
					log.info("Adding partition  " + partitionID + " to worker "
							+ workerProxy.getWorkerID());
					workerPartitionIDs.add(partitionID);
					partitionWorkerMap.put(partitionID,
							workerProxy.getWorkerID());
				}
				workerProxy.addPartitionIDList(workerPartitionIDs);
			}
		}

		if (partitioner.hasNextPartitionID()) {
			// Add the remaining partitions (if any) in a round-robin fashion.
			Iterator<Map.Entry<String, WorkerProxy>> workerMapIter = workerProxyMap
					.entrySet().iterator();

			partitionID = partitioner.getNextPartitionID();

			while (partitionID != -1) {
				// If the remaining partitions is greater than the number of the
				// workers, start iterating from the beginning again.
				if (!workerMapIter.hasNext()) {
					workerMapIter = workerProxyMap.entrySet().iterator();
				}

				WorkerProxy workerProxy = workerMapIter.next().getValue();

				activeWorkerSet.add(workerProxy.getWorkerID());
				log.info("Adding partition  " + partitionID + " to worker "
						+ workerProxy.getWorkerID());
				partitionWorkerMap.put(partitionID, workerProxy.getWorkerID());
				workerProxy.addPartitionID(partitionID);

				partitionID = partitioner.getNextPartitionID();
			}
		}

		// get virtual vertex to partition map
		this.virtualVertexPartitionMap = partitioner
				.getVirtualVertex2PartitionMap();

	}

	public void assignPartitions() {

		partitioner = new Partitioner(Partitioner.STRATEGY_METIS);

		int totalPartitions = partitioner.getNumOfPartitions();

		Partition partition = null;
		partitionWorkerMap = new HashMap<Integer, String>();

		// Assign partitions to workers in the ratio of the number of worker
		// threads that each worker has.
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {

			WorkerProxy workerProxy = entry.getValue();

			// Compute the number of partitions to assign
			int numThreads = workerProxy.getNumThreads();
			double ratio = ((double) (numThreads)) / totalWorkerThreads.get();
			log.info("Worker " + workerProxy.getWorkerID());
			log.info("ThreadNum = " + numThreads + ", Ratio: " + ratio);
			int numPartitionsToAssign = (int) (ratio * totalPartitions);
			log.info("numPartitionsToAssign: " + numPartitionsToAssign);

			List<Partition> workerPartitions = new ArrayList<Partition>();
			for (int i = 0; i < numPartitionsToAssign; i++) {
				partition = partitioner.getNextPartition();

				activeWorkerSet.add(entry.getKey());
				log.info("Adding partition  " + partition.getPartitionID()
						+ " to worker " + workerProxy.getWorkerID());
				workerPartitions.add(partition);
				partitionWorkerMap.put(partition.getPartitionID(),
						workerProxy.getWorkerID());
			}

			// FIXME:uneffective ~ for amazon dataset, it takes 10 mins to
			// transfer data.
			workerProxy.addPartitionList(workerPartitions);
		}

		// Add the remaining partitions (if any) in a round-robin fashion.
		Iterator<Map.Entry<String, WorkerProxy>> workerMapIter = workerProxyMap
				.entrySet().iterator();

		partition = partitioner.getNextPartition();

		while (partition != null) {
			// If the remaining partitions is greater than the number of the
			// workers, start iterating from the beginning again.
			if (!workerMapIter.hasNext()) {
				workerMapIter = workerProxyMap.entrySet().iterator();
			}

			WorkerProxy workerProxy = workerMapIter.next().getValue();

			activeWorkerSet.add(workerProxy.getWorkerID());
			log.info("Adding partition  " + partition.getPartitionID()
					+ " to worker " + workerProxy.getWorkerID());
			partitionWorkerMap.put(partition.getPartitionID(),
					workerProxy.getWorkerID());
			workerProxy.addPartition(partition);

			partition = partitioner.getNextPartition();
		}

		// get virtual vertex to partition map
		this.virtualVertexPartitionMap = partitioner
				.getVirtualVertex2PartitionMap();

	}

	/**
	 * Start super step.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public synchronized void nextLocalCompute() throws RemoteException {
		log.info("Coordinator: next local compute. superstep = " + superstep);

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).nextLocalCompute(superstep);
		}
		this.activeWorkerSet.clear();
	}

	@Override
	public synchronized void localComputeCompleted(String workerID,
			Set<String> activeWorkerIDs) throws RemoteException {

		log.info("Coordinator received activeWorkerIDs from worker " + workerID
				+ " saying: " + activeWorkerIDs);
		this.activeWorkerSet.addAll(activeWorkerIDs);

		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {
			superstep++;
			if (activeWorkerSet.size() != 0)
				nextLocalCompute();
			else {
				finishLocalCompute();
			}
		}
	}

	public void finishLocalCompute() {

		/**
		 * TODO: send flag to coordinator. the coordinator determined the next
		 * step, whether save results or assemble results.
		 * 
		 * TODO: is assemble enabled, then assemble results from workers. and
		 * write results to file.
		 */

		log.info("finish local compute. with round = " + superstep);

	}

	@Override
	public void preProcess() throws RemoteException {
		this.loadGraph(KV.GRAPH_FILE_PATH);
	}

	@Override
	public void postProcess() throws RemoteException {
		// TODO Auto-generated method stub

	}

}
