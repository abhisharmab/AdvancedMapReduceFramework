/**
 * 
 */
package abhi.wordcount;

import abhi.mapreduce.OutputFormat;

/**
 * @author abhisheksharma
 *
 */
public class WordCountOutputFormat extends OutputFormat<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String format(String key, String value) {
		// TODO Auto-generated method stub
		return key + "\t" + value + "\n";
	}


}
