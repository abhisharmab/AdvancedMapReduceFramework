/**
 * 
 */
package abhi.wordcount;

import abhi.mapreduce.Partitioner;

/**
 * @author abhisheksharma
 *
 * This implements the Partitioner Interface to get the Partition Fucntions
 * 
 */
public class WordCountPartitioner implements Partitioner<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public int getPartition(String key, int numofReducers) {
		return Math.abs(key.hashCode()) % numofReducers;	
	}

}
