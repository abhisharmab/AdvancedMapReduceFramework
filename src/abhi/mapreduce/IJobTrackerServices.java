/**
 * 
 */
package abhi.mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;



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
	  public boolean submitJob(JobConf jconf) throws RemoteException;
	  
	  //TaskManager calls this to Update JobTracker about its status. 
	  //This is pretty much the status of each node
	  public void updateTaskManagerStatus(Object status) throws RemoteException;
	  
	  //This is for the Reduce Tasks on the same machine to check the status of the Mapper
	  //Reducers must start once the Map Job have spilt the respective files.
	  public SystemConstants.MapJobsStatus reportMapStatus(int taskID) throws RemoteException;
	  
	  //This if for the Reduce Tasks. After all the Mappers are done we want to pull in 
	  // the information for the Mappers to perform the Reduce.
	  public List<TaskProgress> getCompletedMapTasks(int reducerTaskID) throws RemoteException;
	  
	  //For the client to get Live Updates about the job that was just submitted
	  public JobInfo getLiveStatusofJob(int jobID) throws RemoteException;

	  //For the reducer to the get the HOST from where the Job Intitiated
	  public String getJobOriginHostNamebyTaskID(int taskID) throws RemoteException;

}
