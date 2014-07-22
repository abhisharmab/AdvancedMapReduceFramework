/**
 * 
 */
package abhi.anagram;

import abhi.mapreduce.Partitioner;

/**
 * @author abhisheksharma
 *
 * This class is the partitioner which the user uses to partition the things.
 * 
 */
public class AnaPartitioner implements Partitioner<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public int getPartition(String key, int numofReducers) {
		return Math.abs(key.hashCode()) % numofReducers;	
	}

}
