package ed.inf.grape.util;

public class Constants {

	/** coordinator service name */
	public static final String COORDINATOR_SERVICE_NAME = "grape-coordinator";

	/** partition strategy default */
	public static final String PARTITION_STRATEGY_DEFAULT = "grape-partition-simple";

	/** coordinator RMI service port */
	public static int RMI_PORT = 1099;

	public static int MAX_THREAD_LIMITATION = Integer.MAX_VALUE;

	public static String GRAPH_FILE_PATH = null;

	public static int PARTITION_COUNT = 0;

	/** load constant from properties file */
	static {
		try {
			RMI_PORT = Config.getInstance().getIntProperty("RMI_PORT");
			MAX_THREAD_LIMITATION = Config.getInstance().getIntProperty(
					"THREAD_LIMIT_ON_EACH_MACHINE");
			GRAPH_FILE_PATH = Config.getInstance().getStringProperty(
					"GRAPH_FILE_PATH");
			PARTITION_COUNT = Config.getInstance().getIntProperty(
					"PARTITION_COUNT");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
