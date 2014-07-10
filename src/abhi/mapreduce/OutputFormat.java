/**
 * 
 */
package abhi.mapreduce;

/**
 * @author abhisheksharma
 *
 * OutputFormat describes the output-specification for a Map-Reduce job.
 * 
 * Two main functions:
 * 1. Provided with the key and value this method should return a String that must be written to the Output of the Map-Reduce Job
 * 2. Check if the OUTPUT directory already exists
 */
public abstract class OutputFormat<KOUT, VOUT> {

	public abstract String format(KOUT key, VOUT value);
	
	//TODO: Abhi
	//Implement code to check if the OUTPUT directory already exists. If yes then we cannot run the JOB 
	//This is as feature of Hadoop
	public void checkifOutputDirectoryExists()
	{
		
	}
	
}
