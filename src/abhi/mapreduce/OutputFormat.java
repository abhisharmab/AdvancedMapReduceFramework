/**
 * 
 */
package abhi.mapreduce;

/**
 * @author abhisheksharma
 *
 * OutputFormat describes the output-specification for a Map-Reduce job.
 * 
 * Provided with the key and value this method should return a String that must be written to the Output of the Map-Reduce Job
 */
public abstract class OutputFormat<KOUT, VOUT> {

	public abstract String format(KOUT key, VOUT value);
}
