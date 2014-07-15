/**
 * 
 */
package abhi.mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;


/**
 * @author abhisheksharma
 *
 *This interface defines the services that the JobTracker must provide to JobClient, 
 *TaskTracker etc. Basically it defines all the methods that handle communication with JobTrackers
 *
 *JobTracker would implement these methods and provide the appropriate services to the caller.
 *
 */
public interface IJobTrackerServices extends Remote{

	  public int requestJobID() throws RemoteException;

	  //JobClient calls this Method to SubmitaJob
	  public boolean submitJob(JobConf jconf, Object targetCode) throws RemoteException;
	  
	  //TaskManager calls this to Update JobTracker about its status. 
	  //This is pretty much the status of each node
	  public void updateTaskManagerStatus(Object status) throws RemoteException;
}
