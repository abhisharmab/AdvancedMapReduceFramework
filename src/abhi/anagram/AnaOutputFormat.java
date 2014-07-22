/**
 * 
 */
package abhi.anagram;

import abhi.mapreduce.OutputFormat;

/**
 * @author abhisheksharma
 *
 * Extend the OutputFormat of the MR framework. This is format of how to split to the file.
 *
 */
public class AnaOutputFormat extends OutputFormat<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String format(String key, String value) {
		
		return key + "\t" + value + "\n";
		//We write key values on new lines separated by tabs
	}


}
