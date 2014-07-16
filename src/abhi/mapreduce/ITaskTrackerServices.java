/**
 * 
 */
package abhi.mapreduce;

/**
 * @author abhisheksharma
 * 
 * This interface defines the services that the TaskTracker (running on each node on the Cluster)
 * must provide to JobTracker,  Basically it defines all the methods that handle communication with JobTrackers
 * Also this will act the Reference of the TaskTracker in the Registry.
 * 
 * TaskTRacker would implement these methods and provide the appropriate services to the caller of these services.
 *
 */

public interface ITaskTrackerServices {

	//This is an execution Signal
	public boolean executeTask();
}
