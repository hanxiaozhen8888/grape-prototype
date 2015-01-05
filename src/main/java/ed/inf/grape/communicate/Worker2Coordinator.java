package ed.inf.grape.communicate;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Set;

import ed.inf.grape.core.Worker;

/**
 * Defines an interface to register remote ({@link system.WorkerImpl Worker})
 * with the {@link system.Master Master}.
 * 
 * @author Prakash Chandrasekaran
 * @author Gautham Narayanasamy
 * @author Vijayaraghavan Subbaiah
 */

public interface Worker2Coordinator extends java.rmi.Remote, Serializable {

	/** The Constant SERVICE_NAME. */
	public static final String SERVICE_NAME = "grape-coordinator";

	/**
	 * Registers remote workers with the master.
	 * 
	 * @param worker
	 *            the worker
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            the num worker threads
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */

	public Worker2Coordinator register(Worker worker, String workerID,
			int numWorkerThreads) throws RemoteException;

	/**
	 * Send a message to the Master saying that the current superstep has been
	 * completed.
	 * 
	 * @param workerID
	 *            the worker id
	 */
//	public void superStepCompleted(String workerID, Set<String> activeWorkerIDs)
//			throws RemoteException;

	/**
	 * Defines a deployment convenience to stop each registered
	 * {@link system.Worker Worker} and then stop the {@link system.Master
	 * Master}.
	 * 
	 * @throws java.rmi.RemoteException
	 *             Throws RemoteException when registered workers are stopped.
	 */

	public void shutdown() throws RemoteException;

}
