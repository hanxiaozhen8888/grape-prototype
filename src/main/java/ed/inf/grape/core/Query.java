package ed.inf.grape.core;

import java.io.Serializable;

public class Query implements Serializable {

	// TODO to be overwrite in subclass.

	private static final long serialVersionUID = -95796656026667561L;

	private String QUERY_TYPE = "PageRankQuery";

	public String toString() {
		return QUERY_TYPE;
	}

}
