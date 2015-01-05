package ed.inf.grape.util;

import org.apache.logging.log4j.LogManager;

public class Logger {

	private static Logger instance;

	public static Logger log() {
		if (instance == null) {
			instance = (Logger) LogManager.getLogger(Logger.class);
		}
		return instance.log();
	}

}
