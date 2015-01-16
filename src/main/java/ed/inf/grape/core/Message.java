package ed.inf.grape.core;

import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = -6215563083195145662L;

	private int messageSourceVertexID;

	private int messageTargetVertexID;

	private String message;

	public Message(String message) {
		super();
		this.message = message;
	}

	public int getSource() {
		return this.messageSourceVertexID;
	}

	public int getTarget() {
		return this.messageTargetVertexID;
	}

	@Override
	public String toString() {
		return "Message [" + message + "] " + messageSourceVertexID + " -> "
				+ messageTargetVertexID;
	}
}
