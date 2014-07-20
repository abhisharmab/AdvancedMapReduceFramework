/**
 * 
 */
package abhi.wordcount;

import java.io.IOException;

import abhi.mapreduce.Mapper;
import abhi.mapreduce.OutputCollector;

/**
 * @author abhisheksharma
 *
 */
public class WordCountMapper extends Mapper<String, String, String, String>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void map(String key, String value, OutputCollector<String, String> outputCollector)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] words = line.split(" ");
		
		for(String word : words)
		{
			outputCollector.collect(word, Long.toString(1));
		}

	}

}
