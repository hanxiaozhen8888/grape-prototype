package ed.inf.grape.graph;

import java.io.Serializable;

import org.jgrapht.graph.DefaultEdge;

public class edge extends DefaultEdge implements Serializable {

	private static final long serialVersionUID = 1L;

	public edge() {
	}

//	public boolean equals(Object o) {
//		if (this == o) {
//			return true;
//		} else if (!(o instanceof edge) || o == null) {
//			return false;
//		} else if (this.getSource().toString()
//				.equals(((DefaultEdge) o).getSource().toString())
//				&& this.getTarget().toString()
//						.equals(((DefaultEdge) o).getTarget().toString())) {
//			return true;
//		} else {
//			return false;
//		}
//	}

//	public int hashCode() {
//		int result;
//		result = (this.getSource().toString() == null ? 0 : this.getSource()
//				.toString().hashCode());
//		result = 37
//				* result
//				+ (this.getTarget().toString() == null ? 0 : this.getTarget()
//						.toString().hashCode());
//		return result;
//	}

//	public String getSource() {
//		String source = this.getSource();
//		return source;
//	}
//
//	public String getTarget() {
//		String target = this.getTarget();
//		return target;
//	}
}