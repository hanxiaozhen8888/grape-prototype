package ed.inf.grape.client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import ed.inf.grape.communicate.Client2Coordinator;
import ed.inf.grape.util.Constants;

public class CommunicateTest {

	public static void main(String[] args) throws RemoteException,
			NotBoundException, MalformedURLException, ClassNotFoundException {
		String masterMachineName = args[0];
		String masterURL = "//" + masterMachineName + "/"
				+ Constants.COORDINATOR_SERVICE_NAME;
		Client2Coordinator client2Coordinator = (Client2Coordinator) Naming
				.lookup(masterURL);
		runApplication(client2Coordinator);
	}

	/**
	 * Run application.
	 */
	private static void runApplication(Client2Coordinator client2Coordinator)
			throws RemoteException {
		client2Coordinator.putTask("yecolTestGraph.txt");
	}

}
