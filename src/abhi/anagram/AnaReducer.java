/**
 * 
 */
package abhi.anagram;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import abhi.mapreduce.OutputCollector;
import abhi.mapreduce.Reducer;

/**
 * @author abhisheksharma
 *
 */
public class AnaReducer extends Reducer<String, String, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(String key, Iterator<String> values,
			OutputCollector<String, String> output) throws IOException {
			
        String outputString = "";
        while(values.hasNext())
        {
                String anagram = values.next();
                outputString = output + anagram.toString() + "~";
        }
        StringTokenizer outputTokenizer = new StringTokenizer(outputString,"~");
        if(outputTokenizer.countTokens() >= 2)
        {
        	outputString = outputString.replace("~", ",");
            output.collect(key, outputString);
        }
		
	}

}
