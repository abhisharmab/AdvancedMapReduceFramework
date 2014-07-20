/**
 * 
 */
package abhi.wordcount;

import java.io.IOException;
import java.util.Iterator;

import abhi.mapreduce.OutputCollector;
import abhi.mapreduce.Reducer;

/**
 * @author abhisheksharma
 *
 */
public class WordCountReducer extends Reducer<String, String, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(String key, Iterator<String> values,
			OutputCollector<String, String> output) throws IOException {
		
		long addition = 0; 
		
		while(values.hasNext())
		{
			addition += Long.parseLong(values.next());
		}
		
		output.collect(key, Long.toString(addition));
		
	}


}
