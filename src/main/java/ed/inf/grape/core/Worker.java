package ed.inf.grape.core;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.graph.Partition;

public interface Worker extends Remote {

	public String getWorkerID() throws RemoteException;

	public int getNumThreads() throws RemoteException;

	public void setMasterProxy(Worker2Coordinator coordinatorProxy)
			throws RemoteException;

	public void addPartition(Partition partition) throws RemoteException;

	public void addPartitionList(List<Partition> workerPartitions)
			throws RemoteException;

	public void setWorkerPartitionInfo(int totalPartitionsAssigned,
			Map<Integer, String> mapPartitionIdToWorkerId,
			Map<String, Worker> mapWorkerIdToWorker) throws RemoteException;

	public void halt() throws RemoteException;

	// for pregel model

	/*
	 * public void startSuperStep(long superStepCounter) throws RemoteException;
	 * 
	 * public void setInitialMessage( ConcurrentHashMap<Integer, Map<VertexID,
	 * List<Message>>> initialMessage) throws RemoteException;
	 * 
	 * public void receiveMessage(Map<VertexID, List<Message>> incomingMessages)
	 * throws RemoteException;
	 */

	// for fault-tolerance

	public void sendHeartBeat() throws RemoteException;

	public void checkPoint(long superstep) throws Exception;

	public void startRecovery() throws RemoteException;

	public void finishRecovery() throws RemoteException;

	// public void addRecoveredData(Partition partition,
	// Map<VertexID, List<Message>> messages) throws RemoteException;

	public void writeOutput(String outputFilePath) throws RemoteException;

	// public void updateCheckpointFile() throws RemoteException;

	public void shutdown() throws RemoteException;

}