package ed.inf.grape.communicate;

import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.core.Coordinator;
import ed.inf.grape.core.Query;
import ed.inf.grape.core.Result;
import ed.inf.grape.core.Worker;
import ed.inf.grape.graph.Partition;

/**
 * Represents a thread which is used by the master to talk to workers and
 * vice-versa.
 * 
 * @author Yecol
 */

public class WorkerProxy implements Runnable, Worker2Coordinator {

	private static final long serialVersionUID = 3730860769731338654L;

	/** The worker. */
	private Worker worker;

	/** The master. */
	private Coordinator coordinator;

	/** The thread */
	private Thread t;

	/** The num worker threads. */
	private int numWorkerThreads;

	/** The worker id. */
	String workerID;

	/** The partition list. */
	BlockingQueue<Partition> partitionList;

	/** The partition list. */
	BlockingQueue<Integer> partitionIDList;

	/** The total partitions. */
	private int totalPartitions = 0;

	static Logger log = LogManager.getLogger(WorkerProxy.class);

	/**
	 * Instantiates a new worker proxy.
	 * 
	 * @param worker
	 *            Represents the remote {@link system.WorkerImpl Worker}
	 * @param workerID
	 *            Represents the unique serviceName to identify the worker
	 * @param numWorkerThreads
	 *            the num worker threads
	 * @param master
	 *            Represents the {@link system.Master Master}
	 * @throws AccessException
	 *             the access exception
	 * @throws RemoteException
	 *             the remote exception
	 */

	public WorkerProxy(Worker worker, String workerID, int numWorkerThreads,
			Coordinator coordinator) throws AccessException, RemoteException {
		this.worker = worker;
		this.workerID = workerID;
		this.numWorkerThreads = numWorkerThreads;
		this.coordinator = coordinator;
		partitionList = new LinkedBlockingQueue<Partition>();
		t = new Thread(this);
		t.start();
	}

	/**
	 * Represents a thread which removes {@link api.Task tasks} from a queue,
	 * invoking the associated {@link system.Computer Computer's} execute method
	 * with the task as its argument, and putting the returned
	 * {@link api.Result Result} back into the {@link api.Space Space} for
	 * retrieval by the client
	 */

	public void run() {
		Partition partition = null;
		while (true) {
			try {
				partition = partitionList.take();
				log.info("Partition taken");
				worker.addPartition(partition);
			} catch (RemoteException e) {
				log.fatal("Remote Exception received from the Worker "
						+ workerID);
				log.info("RemoteException: Removing Worker from Master");
				coordinator.removeWorker(workerID);
			} catch (InterruptedException e) {
				log.fatal("Thread interrupted");
				log.info("InterruptedException: Removing Worker from Master");
				coordinator.removeWorker(workerID);
			}
		}
	}

	/**
	 * Exit.
	 */
	public void exit() {
		try {
			t.interrupt();
		} catch (Exception e) {
			System.out.println("Worker Stopped");
		}
	}

	/**
	 * Gets the num threads.
	 * 
	 * @return the num threads
	 */
	public int getNumThreads() {
		return numWorkerThreads;
	}

	/**
	 * Halts the worker and prints the final solution.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		this.restoreInitialState();
		worker.halt();
	}

	/**
	 * Adds the partition.
	 * 
	 * @param partition
	 *            the partition
	 */
	public void addPartition(Partition partition) {

		totalPartitions += 1;
		partitionList.add(partition);
	}

	/**
	 * Adds the partition list.
	 * 
	 * @param workerPartitions
	 *            the worker partitions
	 */
	public void addPartitionList(List<Partition> workerPartitions) {
		try {
			totalPartitions += workerPartitions.size();
			worker.addPartitionList(workerPartitions);
		} catch (RemoteException e) {
			log.fatal("Remote Exception received from the Worker.");
			log.fatal("Giving back the partition to the Master.");

			e.printStackTrace();
			// give the partition back to Master
			coordinator.removeWorker(workerID);
			return;
		}
	}

	/**
	 * Adds the partition.
	 * 
	 * @param partition
	 *            the partition
	 */
	public void addPartitionID(int partitionID) {

		totalPartitions += 1;
		partitionIDList.add(partitionID);
	}

	/**
	 * Adds the partition list.
	 * 
	 * @param workerPartitions
	 *            the worker partitions
	 */
	public void addPartitionIDList(List<Integer> workerPartitionIDs) {
		try {
			totalPartitions += workerPartitionIDs.size();
			worker.addPartitionIDList(workerPartitionIDs);
		} catch (RemoteException e) {
			log.fatal("Remote Exception received from the Worker.");
			log.fatal("Giving back the partition to the Master.");

			e.printStackTrace();
			// give the partition back to Master
			coordinator.removeWorker(workerID);
			return;
		}
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void setWorkerPartitionInfo(
			Map<Integer, Integer> vertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId,
			Map<String, Worker> mapWorkerIdToWorker) throws RemoteException {
		worker.setWorkerPartitionInfo(totalPartitions, vertexIdToPartitionId,
				mapPartitionIdToWorkerId, mapWorkerIdToWorker);
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void setQuery(Query query) throws RemoteException {
		worker.setQuery(query);
	}

	/**
	 * Gets the worker id.
	 * 
	 * @return the worker id
	 */
	public String getWorkerID() {
		return workerID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker2Master#register(system.Worker, java.lang.String, int)
	 */
	public Worker2Coordinator register(Worker worker, String workerID,
			int numWorkerThreads) throws RemoteException {
		return null;
	}

	/**
	 * Restore initial state.
	 */
	private void restoreInitialState() {
		this.totalPartitions = 0;
	}

	/**
	 * Shutdowns the worker and exits
	 */
	public void shutdown() {
		try {
			worker.shutdown();
		} catch (RemoteException e) {
			this.exit();
		}
	}

	@Override
	public void localComputeCompleted(String workerID,
			Set<String> activeWorkerIDs) throws RemoteException {
		this.coordinator.localComputeCompleted(workerID, activeWorkerIDs);
	}

	public void nextLocalCompute(long superstep) throws RemoteException {
		this.worker.nextLocalCompute(superstep);
	}

	public void processPartialResult() throws RemoteException {
		this.worker.processPartialResult();
	}

	@Override
	public void sendPartialResult(String workerID,
			Map<Integer, Result> mapPartitionID2Result) throws RemoteException {
		this.coordinator.assembleResults(workerID,mapPartitionID2Result);
	}
}
