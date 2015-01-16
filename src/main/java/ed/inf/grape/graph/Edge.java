package ed.inf.grape.graph;

import java.io.Serializable;

public class Edge implements Serializable {

	private static final long serialVersionUID = 1009364742090719813L;
	private int source;
	private int target;

	public Edge(int source, int target) {
		this.source = source;
		this.target = target;
	}

	public int getSource() {
		return this.source;
	}

	public int getTarget() {
		return this.target;
	}

}
