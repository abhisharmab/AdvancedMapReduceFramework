/**
 * 
 */
package abhi.mapreduce;

import java.rmi.RemoteException;

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

	//This is an execution Signal send to the TaskTracker by the JobTracker
	public boolean executeTask() throws RemoteException; 
	
	//This is for each of the worker on the TaskManager side to update status of the TaskTracker
	public void updateFieldAgentStatus(Object status) throws RemoteException; 
}
