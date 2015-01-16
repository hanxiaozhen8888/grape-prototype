package ed.inf.grape.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.communicate.Worker2WorkerProxy;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.Config;
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

	// /** The queue of partitions in the current superstep. */
	// private BlockingQueue<Partition> currentPartitionQueue;

	/** The queue of partitions in the next superstep. */
	// private BlockingQueue<Partition> partitionQueue;

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

	/** Limitation of threads on each worker */
	private static int MAX_THREAD_LIMITATION = 0;

	private static String GRAPH_FILE_PATH = null;
	private static int PARTITION_COUNT = 0;

	static Logger log = LogManager.getLogger(WorkerImpl.class);

	static {

		try {
			MAX_THREAD_LIMITATION = Config.getInstance().getIntProperty(
					"THREAD_LIMIT_ON_EACH_MACHINE");
			GRAPH_FILE_PATH = Config.getInstance().getStringProperty(
					"GRAPH_FILE_PATH");
			PARTITION_COUNT = Config.getInstance().getIntProperty(
					"PARTITION_COUNT");
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (MAX_THREAD_LIMITATION == 0) {
			MAX_THREAD_LIMITATION = Integer.MAX_VALUE;
		}
	}

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
		// TODO:change this when deploy
		this.numThreads = Math.min(Runtime.getRuntime().availableProcessors(),
				MAX_THREAD_LIMITATION);
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

				String filename = GRAPH_FILE_PATH + ".p"
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

			String filename = GRAPH_FILE_PATH + ".p"
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

						startSuperStep = false;
						checkAndSendMessage();
						// Partition partition = currentPartitionQueue.take();
						// log.info("runned in the loop");
						// nextPartitionQueue.add(partition);
						// checkAndSendMessage();

						// } catch (InterruptedException e) {
						// log.error("InterruptedException");
						// log.error(e.getStackTrace());
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

			Message m2Worker = new Message("message to worker: hello worker");
			Message m2Coordinator = new Message(
					"message to coordinator: hello coordinator");

			try {
				coordinatorProxy.localComputeCompleted(workerID, m2Coordinator);
			} catch (RemoteException e) {
				e.printStackTrace();
			}

			log.debug(this + "sendingMessage: " + m2Worker);

			// try {
			// worker2WorkerProxy.sendMessage(entry.getKey(),
			// entry.getValue());
			// } catch (RemoteException e) {
			// System.out.println("Can't send message to Worker "
			// + entry.getKey() + " which is down");
			// }

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

		// String workerID = null;
		// VertexID vertexID = null;
		// Message message = null;
		// Map<VertexID, List<Message>> workerMessages = null;
		// List<Message> messageList = null;
		// for (Entry<VertexID, Message> entry : messagesFromCompute.entrySet())
		// {
		// vertexID = entry.getKey();
		// message = entry.getValue();
		// workerID = mapPartitionIdToWorkerId.get(vertexID.getPartitionID());
		// if (workerID.equals(this.workerID)) {
		// updateIncomingMessages(vertexID, message);
		// } else {
		// if (outgoingMessages.containsKey(workerID)) {
		// workerMessages = outgoingMessages.get(workerID);
		// if (workerMessages.containsKey(vertexID)) {
		// workerMessages.get(vertexID).add(message);
		// } else {
		// messageList = new ArrayList<Message>();
		// messageList.add(message);
		// workerMessages.put(vertexID, messageList);
		// }
		// } else {
		// messageList = new ArrayList<Message>();
		// messageList.add(message);
		// workerMessages = new HashMap<Object, Object>();
		// workerMessages.put(vertexID, messageList);
		// outgoingMessages.put(workerID, workerMessages);
		// }
		// }
		// }
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
					+ Worker2Coordinator.SERVICE_NAME;
			// Registry registry =
			// LocateRegistry.getRegistry(masterMachineName);
			// Worker2Master worker2Master = (Worker2Master) registry
			// .lookup(Worker2Master.SERVICE_NAME);
			Worker2Coordinator worker2Coordinator = (Worker2Coordinator) Naming
					.lookup(masterURL);
			Worker worker = new WorkerImpl();
			// System.out.println("here " + worker2Master.getClass());
			Worker2Coordinator coordinatorProxy = worker2Coordinator.register(
					worker, worker.getWorkerID(), worker.getNumThreads());
			// Worker2Master masterProxy = (Worker2Master)
			// worker2Master.register(null, null, 0);
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
	 * Receive message. <<<<<<< HEAD
	 * 
	 * @param incomingMessages
	 *            the incoming messages
	 * 
	 * @param incomingMessages
	 *            the incoming messages
	 * @throws RemoteException
	 *             the remote exception
	 */
	// public void receiveMessage(Map<VertexID, List<Message>> incomingMessages)
	// throws RemoteException {
	// Map<VertexID, List<Message>> partitionMessages = null;
	// int partitionID = 0;
	// VertexID vertexID = null;
	// List<Message> messageList = null;
	// Map<VertexID, List<Message>> vertexMessageMap = null;
	// for (Entry<VertexID, List<Message>> entry : incomingMessages.entrySet())
	// {
	// vertexID = entry.getKey();
	// messageList = entry.getValue();
	// partitionID = vertexID.getPartitionID();
	// if (currentIncomingMessages.containsKey(partitionID)) {
	// partitionMessages = currentIncomingMessages.get(partitionID);
	// if (partitionMessages.containsKey(vertexID)) {
	// partitionMessages.get(vertexID).addAll(messageList);
	// } else {
	// partitionMessages.put(vertexID, messageList);
	// }
	// } else {
	// vertexMessageMap = new HashMap<Object, Object>();
	// vertexMessageMap.put(vertexID, messageList);
	// currentIncomingMessages.put(partitionID, vertexMessageMap);
	// }
	//
	// }
	// }

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
	// public void updateIncomingMessages(VertexID destinationVertex,
	// Message incomingMessage) {
	// Map<VertexID, List<Message>> partitionMessages = null;
	// List<Message> newMessageList = null;
	// int partitionID = destinationVertex.getPartitionID();
	// // partitionMessages = currentIncomingMessages.get(partitionID);
	// if (currentIncomingMessages.containsKey(partitionID)) {
	// partitionMessages = currentIncomingMessages.get(partitionID);
	// if (partitionMessages.containsKey(destinationVertex)) {
	// partitionMessages.get(destinationVertex).add(incomingMessage);
	// } else {
	// newMessageList = new ArrayList<Message>();
	// newMessageList.add(incomingMessage);
	// partitionMessages.put(destinationVertex, newMessageList);
	// }
	// } else {
	// partitionMessages = new HashMap<Object, Object>();
	// newMessageList = new ArrayList<Message>();
	// newMessageList.add(incomingMessage);
	// partitionMessages.put(destinationVertex, newMessageList);
	// currentIncomingMessages.put(partitionID, partitionMessages);
	// }
	// }

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

	/**
	 * Sets the initial message for the Worker that has the source vertex.
	 * 
	 * @param initialMessage
	 *            the initial message ======= Sets the initial message for the
	 *            Worker that has the source vertex.
	 * 
	 * @param initialMessage
	 *            the initial message
	 * @throws RemoteException
	 *             the remote exception >>>>>>>
	 *             42b91fb45356bdb8ce40222761cb75525693696a
	 */
	// public void setInitialMessage(
	// ConcurrentHashMap<Integer, Map<VertexID, List<Message>>> initialMessage)
	// throws RemoteException {
	// this.currentIncomingMessages = initialMessage;
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker#checkPoint(long)
	 */
	// @Override
	// public void checkPoint(long superstep) throws Exception {
	// System.out.println("WorkerImpl: checkPoint " + superstep);
	// this.superstep = superstep;
	// WorkerData wd = new WorkerData(this.nextPartitionQueue,
	// this.currentIncomingMessages);
	// // Serialization
	//
	// // Don't update the currentCheckpointFile until the Master confirms that
	// // the checkpointing had succeeded in all the Workers.
	// this.nextCheckpointFile = CHECKPOINTING_DIRECTORY + File.separator
	// + workerID + "_" + superstep;
	// // String newFilePath = CHECKPOINTING_DIRECTORY + File.separator +
	// // workerID;
	// GeneralUtils.serialize(this.nextCheckpointFile, wd);
	// // nextCheckpointFile = tmpFilePath;
	// // GeneralUtils.renameFile(tmpFilePath, newFilePath);
	// }

	/**
	 * Master checks the heart beat of the worker by calling this method.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	// @Override
	// public void sendHeartBeat() throws RemoteException {
	// }

	/**
	 * Method to prepare the worker.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	// @Override
	// public void startRecovery() throws RemoteException {
	// System.out.println("WorkerImpl: startRecovery");
	// this.stopSendingMessage = false;
	// this.startSuperStep = false;
	// this.currentPartitionQueue.clear();
	// this.previousIncomingMessages.clear();
	// this.outgoingMessages.clear();
	//
	// WorkerData workerData = (WorkerData) GeneralUtils
	// .deserialize(this.currentCheckpointFile);
	// this.currentIncomingMessages = (ConcurrentHashMap<Integer, Map<VertexID,
	// List<Message>>>) workerData
	// .getMessages();
	// this.nextPartitionQueue = (BlockingQueue<Partition>) workerData
	// .getPartitions();
	//
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker#finishRecovery()
	 */
	// @Override
	// public void finishRecovery() throws RemoteException {
	// System.out.println("WorkerImpl: finishRecovery");
	// try {
	// // Do checkpointing after assigning recovered partitions.
	// checkPoint(this.superstep);
	// } catch (Exception e) {
	// System.out.println("checkpoint failure");
	// throw new RemoteException();
	// }
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker#addRecoveredData(system.Partition, java.util.Map)
	 */
	// public void addRecoveredData(Partition partition,
	// Map<VertexID, List<Message>> messages) throws RemoteException {
	// System.out.println("WorkerImpl: addRecoveredData");
	// // System.out.println("Partition " + partition.getPartitionID());
	// // System.out.println("Messages: " + messages);
	// if (messages != null) {
	// this.currentIncomingMessages.put(partition.getPartitionID(),
	// messages);
	// }
	// this.nextPartitionQueue.add(partition);
	// }

	/** shutdown the worker */
	public void shutdown() throws RemoteException {
		java.util.Date date = new java.util.Date();
		log.info("Worker" + workerID + " goes down now at :"
				+ new Timestamp(date.getTime()));
		System.exit(0);
	}

	/**
	 * Writes the result of the graph computation to a output file
	 * 
	 * @param outputFilePath
	 *            Represents the output file for writing the results
	 */
	// public void writeOutput(String outputFilePath) throws RemoteException {
	// System.out.println("Printing the final state of the partitions");
	// Iterator<Partition> iter = nextPartitionQueue.iterator();
	// // Append the appropriate content to the output file.
	// StringBuilder contents = new StringBuilder();
	// while (iter.hasNext()) {
	// contents.append(iter.next());
	// }
	// GeneralUtils.writeToFile(outputFilePath, contents.toString(), true);
	// }

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker#updateCheckpointFile()
	 */
	// @Override
	// public void updateCheckpointFile() throws RemoteException {
	// if (this.currentCheckpointFile != null) {
	// GeneralUtils.removeFile(this.currentCheckpointFile);
	// }
	// this.currentCheckpointFile = this.nextCheckpointFile;
	// System.out.println("WorkerImpl: Updating checkpoint file: "
	// + this.currentCheckpointFile);
	// }

	public void receiveMessage(Message incomingMessages) throws RemoteException {
		// TODO Auto-generated method stub
		log.info(incomingMessages);
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
