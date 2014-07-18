/**
 * 
 */
package abhi.mapreduce;

import java.io.Serializable;

/**
 * @author abhisheksharma
 *
 * OutputFormat describes the output-specification for a Map-Reduce job.
 * 
 * Two main functions:
 * 1. Provided with the key and value this method should return a String that must be written to the Output of the Map-Reduce Job
 * 2. Check if the OUTPUT directory already exists
 */
public abstract class OutputFormat<KOUT, VOUT>  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public abstract String format(KOUT key, VOUT value);
	

	
}
