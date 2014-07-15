/**
 * 
 */
package abhi.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * @author abhisheksharma
 * 
 *This class is the abstract implementation of the Reduce Class which defines policy of usage
 *The Application Programmer must provide the appropriate implementation for Reduce based on the kind of job
 *
 * * Application Programmer will provide the precise implementations for these based on the KIND of Job
 * @param <V2>
 * @param <K2>
 * @param <K3>
 * @param <V3>
 */
public abstract class Reducer<K2,V2,K3,V3> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	//Called once at the beginning to setup the Reduce Task or perform some pre-processing as per Application Need
	protected void setup() throws IOException, InterruptedException {};
	
	//Called once at the end (Possibly for some cleanup and housekeeping work)
	protected void cleanUp() throws IOException, InterruptedException{};

	// The Reduce functionality that needs to be overwritten by the Application Programmer 
	// Provide the reduce functionality
	public abstract void reduce(K2 key, Iterator<V2> values, OutputCollector<K3, V3> output)
			throws IOException;
}
