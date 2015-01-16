package ed.inf.grape.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.L.LocalComputeTask;
import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.communicate.Worker2WorkerProxy;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.KV;
import ed.inf.grape.util.Dev;
import ed.inf.grape.util.IO;

/**
 * Represents the computation node.
 * 
 * @author Yecol
 */

public class WorkerImpl extends UnicastRemoteObject implements Worker {

	private static final long serialVersionUID = 8653095027537771705L;

	/** The num threads. */
	private int numThreads;

	/** The total partitions assigned. */
	private int totalPartitionsAssigned;

	/**
	 * boolean variable to determine if a Worker can send messages to other
	 * Workers and to Master. It is set to true when a Worker is sending
	 * messages to other Workers.
	 */
	private boolean stopSendingMessage;

	/** The queue of partitions in the current superstep. */
	private BlockingQueue<LocalComputeTask> currentLocalComputeTaskQueue;

	/** The queue of partitions in the next superstep. */
	private BlockingQueue<LocalComputeTask> nextLocalComputeTasksQueue;

	/** hosting partitions */
	private Map<Integer, Partition> partitions;

	/** Hostname of the node with timestamp information. */
	private String workerID;

	/** Master Proxy object to interact with Master. */
	private Worker2Coordinator coordinatorProxy;

	/** VertexID 2 PartitionID Map */
	private Map<String, Integer> mapVertexIdToPartitionId;

	/** PartitionID to WorkerID Map. */
	private Map<Integer, String> mapPartitionIdToWorkerId;

	/** Worker2WorkerProxy Object. */
	private Worker2WorkerProxy worker2WorkerProxy;

	/** Worker to Outgoing Messages Map. */
	private ConcurrentHashMap<String, List<Message>> outgoingMessages;

	/** partitionId to Previous Incoming messages - Used in current Super Step. */
	private ConcurrentHashMap<Integer, List<Message>> previousIncomingMessages;

	/** partitionId to Current Incoming messages - used in next Super Step. */
	private ConcurrentHashMap<Integer, List<Message>> currentIncomingMessages;

	/**
	 * boolean variable indicating whether the partitions can be worked upon by
	 * the workers in each superstep.
	 **/
	private boolean startSuperStep = false;

	/** The super step counter. */
	private long superstep = 0;

	static Logger log = LogManager.getLogger(WorkerImpl.class);

	/**
	 * Instantiates a new worker.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public WorkerImpl() throws RemoteException {
		InetAddress address = null;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyyMMdd.HHmmss.SSS");
		String timestamp = simpleDateFormat.format(new Date());

		String hostName = new String();
		try {
			address = InetAddress.getLocalHost();
			hostName = address.getHostName();
		} catch (UnknownHostException e) {
			hostName = "UnKnownHost";
			log.error(e);
		}

		this.workerID = hostName + "_" + timestamp;
		// this.currentPartitionQueue = new LinkedBlockingDeque<Partition>();
		// this.partitionQueue = new LinkedBlockingQueue<Partition>();
		this.partitions = new HashMap<Integer, Partition>();
		this.currentIncomingMessages = new ConcurrentHashMap<Integer, List<Message>>();
		this.previousIncomingMessages = new ConcurrentHashMap<Integer, List<Message>>();
		this.outgoingMessages = new ConcurrentHashMap<String, List<Message>>();
		this.numThreads = Math.min(Runtime.getRuntime().availableProcessors(),
				KV.MAX_THREAD_LIMITATION);
		this.stopSendingMessage = false;
		for (int i = 0; i < numThreads; i++) {
			log.debug("Starting Thread " + (i + 1));
			WorkerThread workerThread = new WorkerThread();
			workerThread.start();
		}
	}

	/**
	 * Adds the partition to be assigned to the worker.
	 * 
	 * @param partition
	 *            the partition to be assigned
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void addPartition(Partition partition) throws RemoteException {
		log.info("receive partition:" + partition.getPartitionInfo());
		log.debug(Dev.currentRuntimeState());
		this.partitions.put(partition.getPartitionID(), partition);
		log.debug(Dev.currentRuntimeState());
	}

	/**
	 * Adds the partition list.
	 * 
	 * @param workerPartitions
	 *            the worker partitions
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void addPartitionList(List<Partition> workerPartitions)
			throws RemoteException {
		log.debug(Dev.currentRuntimeState());
		for (Partition p : workerPartitions) {
			log.info("receive partition:" + p.getPartitionInfo());
			this.partitions.put(p.getPartitionID(), p);
		}
		log.debug(Dev.currentRuntimeState());
	}

	@Override
	public void addPartitionIDList(List<Integer> workerPartitionIDs)
			throws RemoteException {

		for (int partitionID : workerPartitionIDs) {

			if (!this.partitions.containsKey(partitionID)) {

				String filename = KV.GRAPH_FILE_PATH + ".p"
						+ String.valueOf(partitionID);

				Partition partition;
				try {
					partition = IO.loadPartitions(partitionID, filename);
					this.partitions.put(partitionID, partition);
				} catch (IOException e) {
					log.error("load partition file failed.");
					e.printStackTrace();
				}
			}
		}

	}

	@Override
	public void addPartitionID(int partitionID) throws RemoteException {

		if (!this.partitions.containsKey(partitionID)) {

			String filename = KV.GRAPH_FILE_PATH + ".p"
					+ String.valueOf(partitionID);

			Partition partition;
			try {
				partition = IO.loadPartitions(partitionID, filename);
				this.partitions.put(partitionID, partition);
			} catch (IOException e) {
				log.error("load partition file failed.");
				e.printStackTrace();
			}
		}

	}

	/**
	 * Gets the num threads.
	 * 
	 * @return the num threads
	 */
	public int getNumThreads() {
		return numThreads;
	}

	/**
	 * Gets the worker id.
	 * 
	 * @return the worker id
	 */
	public String getWorkerID() {
		return workerID;
	}

	/**
	 * The Class WorkerThread.
	 */
	private class WorkerThread extends Thread {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				while (startSuperStep) {
					log.debug(this + "Superstep loop started for superstep "
							+ superstep);
					try {

						// startSuperStep = false;
						// checkAndSendMessage();

						LocalComputeTask localComputeTask = currentLocalComputeTaskQueue
								.take();
						Partition workingPartition = partitions
								.get(localComputeTask.getPartitionID());

						if (superstep == 0) {
							/** begin step. initial compute */
							localComputeTask.compute(workingPartition);
							updateOutgoingMessages(localComputeTask
									.getMessages());
						}

						else {
							/** not begin step. incremental compute */
							List<Message> messageForWorkingPartition = previousIncomingMessages
									.get(localComputeTask.getPartitionID());

							if (messageForWorkingPartition != null) {

								// TODO: since we record messages mapped to
								// vertex. it is possible to compute on
								// central of vertex.

								localComputeTask.incrementalCompute(
										workingPartition,
										messageForWorkingPartition);

								updateOutgoingMessages(localComputeTask
										.getMessages());
							}
						}

						nextLocalComputeTasksQueue.add(localComputeTask);
						checkAndSendMessage();

					} catch (Exception e) {
						log.error(e.getStackTrace());
					}
				}
			}
		}

		/**
		 * Check and send message.
		 * 
		 * @throws RemoteException
		 */
		private synchronized void checkAndSendMessage() {
			log.debug("checkAndSendMessage");

			if (!stopSendingMessage
					&& (nextLocalComputeTasksQueue.size() == totalPartitionsAssigned)) {
				stopSendingMessage = true;
				log.debug(this + " WorkerImpl: Superstep " + superstep
						+ " completed.");

				startSuperStep = false;

				for (Entry<String, List<Message>> entry : outgoingMessages
						.entrySet()) {
					try {
						worker2WorkerProxy.sendMessage(entry.getKey(),
								entry.getValue());
					} catch (RemoteException e) {
						System.out.println("Can't send message to Worker "
								+ entry.getKey() + " which is down");
					}
				}

				// This worker will be active only if it has some messages
				// queued up in the next superstep.
				// activeWorkerSet will have all the workers who will be active
				// in the next superstep.
				Set<String> activeWorkerSet = new HashSet<String>();
				activeWorkerSet.addAll(outgoingMessages.keySet());
				if (currentIncomingMessages.size() > 0) {
					activeWorkerSet.add(workerID);
				}
				// Send a message to the Master saying that this superstep has
				// been completed.
				try {
					coordinatorProxy.localComputeCompleted(workerID,
							activeWorkerSet);
				} catch (RemoteException e) {
					e.printStackTrace();
				}

			}
			// System.out.println(this + " after sendMessage check " +
			// sendingMessage);

		}
	}

	/**
	 * Halts the run for this application and prints the output in a file.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		System.out.println("Worker Machine " + workerID + " halts");
		this.restoreInitialState();
	}

	/**
	 * Restore the worker to the initial state
	 */
	private void restoreInitialState() {
		// this.partitionQueue.clear();
		this.currentIncomingMessages.clear();
		this.outgoingMessages.clear();
		this.mapPartitionIdToWorkerId.clear();
		// this.currentPartitionQueue.clear();
		this.previousIncomingMessages.clear();
		this.stopSendingMessage = false;
		this.startSuperStep = false;
		this.totalPartitionsAssigned = 0;
	}

	/**
	 * Updates the outgoing messages for every superstep.
	 * 
	 * @param messagesFromCompute
	 *            Represents the map of destination vertex and its associated
	 *            message to be send
	 */
	private void updateOutgoingMessages(List<Message> messagesFromCompute) {
		log.debug("updateOutgoingMessages");

		String workerID = null;
		int vertexID = -1;
		int partitionID = -1;
		List<Message> workerMessages = null;
		for (Message message : messagesFromCompute) {
			vertexID = message.getTarget();
			partitionID = mapVertexIdToPartitionId.get(vertexID);
			workerID = mapPartitionIdToWorkerId.get(partitionID);
			if (workerID.equals(this.workerID)) {

				/** send message to self. */
				updateIncomingMessages(partitionID, message);
			} else {

				if (outgoingMessages.containsKey(workerID)) {
					outgoingMessages.get(workerID).add(message);
				} else {
					workerMessages = new ArrayList<Message>();
					workerMessages.add(message);
					outgoingMessages.put(workerID, workerMessages);
				}
			}
		}
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param totalPartitionsAssigned
	 *            the total partitions assigned
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 * 
	 * @param totalPartitionsAssigned
	 *            the total partitions assigned
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void setWorkerPartitionInfo(int totalPartitionsAssigned,
			Map<String, Integer> mapVertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId,
			Map<String, Worker> mapWorkerIdToWorker) throws RemoteException {
		log.info("WorkerImpl: setWorkerPartitionInfo");
		log.info("totalPartitionsAssigned " + totalPartitionsAssigned
				+ " mapPartitionIdToWorkerId: " + mapPartitionIdToWorkerId);
		log.info("vertex2partitionMapSize: " + mapVertexIdToPartitionId.size());
		this.totalPartitionsAssigned = totalPartitionsAssigned;
		this.mapVertexIdToPartitionId = mapVertexIdToPartitionId;
		this.mapPartitionIdToWorkerId = mapPartitionIdToWorkerId;
		this.worker2WorkerProxy = new Worker2WorkerProxy(mapWorkerIdToWorker);
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		try {
			String coordinatorMachineName = args[0];
			log.info("masterMachineName " + coordinatorMachineName);

			String masterURL = "//" + coordinatorMachineName + "/"
					+ KV.COORDINATOR_SERVICE_NAME;
			Worker2Coordinator worker2Coordinator = (Worker2Coordinator) Naming
					.lookup(masterURL);
			Worker worker = new WorkerImpl();
			Worker2Coordinator coordinatorProxy = worker2Coordinator.register(
					worker, worker.getWorkerID(), worker.getNumThreads());

			worker.setCoordinatorProxy(coordinatorProxy);
			log.info("Worker is bound and ready for computations ");

		} catch (Exception e) {
			log.error("ComputeEngine exception:");
			e.printStackTrace();
		}
	}

	/**
	 * Sets the master proxy.
	 * 
	 * @param masterProxy
	 *            the new master proxy
	 */
	public void setCoordinatorProxy(Worker2Coordinator coordinatorProxy) {
		this.coordinatorProxy = coordinatorProxy;
	}

	/**
	 * Receive message.
	 * 
	 * @param incomingMessages
	 *            the incoming messages
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void receiveMessage(List<Message> incomingMessages)
			throws RemoteException {

		log.debug("recevie incomingMessages: " + incomingMessages.size());
		log.debug(incomingMessages.toString());

		/** partitionID to message list */
		List<Message> partitionMessages = null;

		int partitionID = -1;
		int vertexID = -1;

		for (Message message : incomingMessages) {
			vertexID = message.getTarget();
			partitionID = mapVertexIdToPartitionId.get(vertexID);
			if (currentIncomingMessages.containsKey(partitionID)) {
				currentIncomingMessages.get(vertexID).add(message);
			} else {
				partitionMessages = new ArrayList<Message>();
				partitionMessages.add(message);
				currentIncomingMessages.put(partitionID, partitionMessages);
			}
		}
	}

	/**
	 * Receives the messages sent by all the vertices in the same node and
	 * updates the current incoming message queue.
	 * 
	 * @param destinationVertex
	 *            Represents the destination vertex to which the message has to
	 *            be sent
	 * @param incomingMessage
	 *            Represents the incoming message for the destination vertex
	 */
	public void updateIncomingMessages(int partitionID, Message incomingMessage) {
		List<Message> partitionMessages = null;
		if (currentIncomingMessages.containsKey(partitionID)) {
			currentIncomingMessages.get(partitionID).add(incomingMessage);
		} else {
			partitionMessages = new ArrayList<Message>();
			partitionMessages.add(incomingMessage);
			currentIncomingMessages.put(partitionID, partitionMessages);
		}
	}

	/**
	 * The worker receives the command to start the next superstep from the
	 * master. Set startSuperStep to true; assign previousIncomingMessages to
	 * currentIncomingMessages; reset currentIncomingMessages;
	 * 
	 * @param superStepCounter
	 *            the super step counter
	 */
	// public void startSuperStep(long superStepCounter) {
	// log.debug("WorkerImpl: startSuperStep - superStepCounter: "
	// + superStepCounter);

	// this.superstep = superStepCounter;
	// // Put all elements in current incoming queue to previous incoming
	// queue
	// // and clear the current incoming queue.
	// this.previousIncomingMessages.clear();
	// this.previousIncomingMessages.putAll(this.currentIncomingMessages);
	// this.currentIncomingMessages.clear();
	//
	// this.stopSendingMessage = false;
	// this.startSuperStep = true;
	//
	// this.outgoingMessages.clear();
	// // Put all elements in completed partitions back to partition queue
	// and
	// // clear the completed partitions.
	// // Note: To avoid concurrency issues, it is very important that
	// // completed partitions is cleared before the Worker threads start to
	// // operate on the partition queue in the next superstep
	// BlockingQueue<Partition> temp = new LinkedBlockingDeque<Partition>(
	// nextPartitionQueue);
	// this.nextPartitionQueue.clear();
	// this.currentPartitionQueue.addAll(temp);

	// System.out.println("Partition queue: " + partitionQueue.size());
	// }

	/** shutdown the worker */
	public void shutdown() throws RemoteException {
		java.util.Date date = new java.util.Date();
		log.info("Worker" + workerID + " goes down now at :"
				+ new Timestamp(date.getTime()));
		System.exit(0);
	}

	public void writeOutput(String outputFilePath) throws RemoteException {
		// TODO Auto-generated method stub

	}

	public void startWork() throws RemoteException {
		// TODO Auto-generated method stub
		log.info("Worker receive the flag from coordinator, begin to work.");
		log.debug("hosting" + this.partitions.size() + "partitions");
		this.startSuperStep = true;
	}

}
