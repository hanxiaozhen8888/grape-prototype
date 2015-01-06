package ed.inf.grape.core;

import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = -6215563083195145662L;

	private String message;

	public Message(String message) {
		super();
		this.message = message;
	}

	@Override
	public String toString() {
		return "Message [message=" + message + "]";
	}

}
