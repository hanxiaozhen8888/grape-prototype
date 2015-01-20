package ed.inf.grape.core;

import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = -6215563083195145662L;

	private int sourcePartitionID;

	private int destinationVertexID;

	private Double content;

	public Message(int fromPartitionID, int destination, Double message) {
		super();
		this.sourcePartitionID = fromPartitionID;
		this.destinationVertexID = destination;
		this.content = message;
	}

	public int getSourcePartitionID() {
		return this.sourcePartitionID;
	}

	public int getDestinationVertexID() {
		return this.destinationVertexID;
	}

	public Double getContent() {
		return this.content;
	}

	@Override
	public String toString() {
		return "Message [" + content + "] -> " + destinationVertexID;
	}
}
