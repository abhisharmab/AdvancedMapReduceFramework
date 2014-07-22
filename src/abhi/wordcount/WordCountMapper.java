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
 *
 * The Map Function of the Word Count Example.
 */
public class WordCountMapper extends Mapper<String, String, String, String>
{

	private static final long serialVersionUID = 1L;

	@Override
	public void map(String key, String value, OutputCollector<String, String> outputCollector)
			throws IOException, InterruptedException {
		
		String data = value.toString(); //Converted the Data Into String
		String[] wordsArray = data.split(" "); //User is saying that the file is space de-limited
		
		for(String word : wordsArray)
		{
			outputCollector.collect(word, Long.toString(1));
		}

	}

}
