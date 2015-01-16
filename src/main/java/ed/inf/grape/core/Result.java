package ed.inf.grape.core;

import java.io.Serializable;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Result implements Serializable {

	private static final long serialVersionUID = -2747898136416052009L;

	static Logger log = LogManager.getLogger(Result.class);

	/** a function how to assemble partial results to a final result. */
	public Result assembleResults(Collection<Result> results) {

		log.debug("assemble results to a final result.");

		Result assembledResult = new Result();

		/** add how to assemble a final result from results */
		return assembledResult;
	}

	/** a function how write file results to a final result. */
	public void writeToFile(String filename) {

		/** add how to assemble a final result from results */
		log.debug("write result to file: " + filename);
	}
}
