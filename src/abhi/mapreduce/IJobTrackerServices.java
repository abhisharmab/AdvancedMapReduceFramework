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

	  public boolean submitJob(JobConf jconf) throws RemoteException;
	  
	  public void updateTaskManagerStatus(Object status) throws RemoteException;
}
