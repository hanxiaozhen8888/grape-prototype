package ed.inf.grape.util;

public class KV {

	/** coordinator service name */
	public static final String COORDINATOR_SERVICE_NAME = "grape-coordinator";

	/** coordinator RMI service port */
	public static int RMI_PORT = 1099;

	public static int MAX_THREAD_LIMITATION = Integer.MAX_VALUE;

	public static String GRAPH_FILE_PATH = null;

	public static int PARTITION_COUNT = 0;

	public static boolean ENABLE_COORDINATOR = false;
	public static boolean ENABLE_ASSEMBLE = false;
	public static boolean ENABLE_SYNC = false;
	public static boolean ENABLE_LOCAL_BATCH = false;
	public static boolean ENABLE_LOCAL_INCREMENTAL = false;
	public static boolean ENABLE_LOCAL_MESSAGE = false;

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

			ENABLE_COORDINATOR = Config.getInstance().getBooleanProperty(
					"ENABLE_COORDINATOR");

			ENABLE_ASSEMBLE = Config.getInstance().getBooleanProperty(
					"ENABLE_ASSEMBLE");

			ENABLE_SYNC = Config.getInstance()
					.getBooleanProperty("ENABLE_SYNC");

			ENABLE_LOCAL_BATCH = Config.getInstance().getBooleanProperty(
					"ENABLE_LOCAL_BATCH");

			ENABLE_LOCAL_INCREMENTAL = Config.getInstance().getBooleanProperty(
					"ENABLE_LOCAL_INCREMENTAL");

			ENABLE_LOCAL_MESSAGE = Config.getInstance().getBooleanProperty(
					"ENABLE_LOCAL_MESSAGE");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
