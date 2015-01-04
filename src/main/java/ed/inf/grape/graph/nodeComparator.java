package ed.inf.grape.graph;

import java.util.Comparator;

/**
 * sort in descending order
 * 
 * @author s0944873
 *
 */

public class nodeComparator implements Comparator<String> {
	public cg_graph gg;

	public nodeComparator(cg_graph ggs) {
		gg = ggs;
	}

	public int compare(String a, String b) {
		if ((gg.inDegreeOf(a) + gg.outDegreeOf(a)) > (gg.inDegreeOf(b) + gg
				.outDegreeOf(b))) {

			return -1;
		}
		if ((gg.inDegreeOf(a) + gg.outDegreeOf(a)) < (gg.inDegreeOf(b) + gg
				.outDegreeOf(b))) {
			return 1;
		}
		return 0;
	}
}
