/**
 * 
 */
package abhi.wordcount;

import abhi.mapreduce.Partitioner;

/**
 * @author abhisheksharma
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
