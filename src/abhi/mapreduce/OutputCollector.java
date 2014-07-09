/**
 * 
 */
package abhi.mapreduce;

import java.io.IOException;

/**
 * @author abhisheksharma
 * This abstract class defines the collect Method that will be used both by Reducer and Mappers to collect the Output after processing
 * An abstract class that will be extends by the Mapper and ReducerCollector
 * 
 *@param KOUT
 *@param VOUT
 */
public abstract class OutputCollector<KOUT, VOUT> {
	
	protected String outputDirectory; 
	
	public OutputCollector(String outdirectory)
	{
		this.outputDirectory = outdirectory;
	}
	
	protected abstract void collect(KOUT key, VOUT value) throws IOException;

}
