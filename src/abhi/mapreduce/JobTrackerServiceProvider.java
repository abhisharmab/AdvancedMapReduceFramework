/**
 * 
 */
package abhi.mapreduce;

import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

import abhi.mapreduce.SystemConstants.MapJobsStatus;

/**
 * @author abhisheksharma
 *
 *
 * REMOTE REF FOR THE JOBTRACKER as well.
 * This is a remote reference for the JobTracker which lives in the RMI registry. 
 * Everyone who needs to communicate with the JobTracker will get a reference of this and talk to the JobTrackers
 * 
 * Basically this is the world-facing entity of the JobTracker and then delegates events and calls to
 * the JobTracker for the heavy-lifting. 
 * 
 * Primary callers of the services here are :
 * JobClient: 
 * 	1. To Submit a JOB 
 *  2. Get updates about a running map/reduce Job that the user has requested to start 
 *  
 *  
 *  Task Tracker:
 *  	1. To basically update about its progress to the Task Tracker (heartbeat)
 *  	2. Report anything special or any needed information etx.
 *
 */
public class JobTrackerServiceProvider extends UnicastRemoteObject implements IJobTrackerServices {

	private JobTracker jobTracker;
	
	protected JobTrackerServiceProvider() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see abhi.mapreduce.IJobTrackerServices#requestJobID()
	 */
	@Override
	public int requestJobID() throws RemoteException {
		return this.jobTracker.nextJobId();
	}

	/* (non-Javadoc)
	 * @see abhi.mapreduce.IJobTrackerServices#submitJob(abhi.mapreduce.JobConf)
	 */
	@Override
	public boolean submitJob(JobConf jconf, Object targetCode) throws RemoteException {
		if(jconf == null)
			return false;
		JobInfo jobInfo = new JobInfo(jconf);
		
		this.jobTracker.submitJob(jobInfo);
		
	
		return false;
	}

	/* (non-Javadoc)
	 * @see abhi.mapreduce.IJobTrackerServices#updateTaskManagerStatus(java.lang.Object)
	 */
	@Override
	public void updateTaskManagerStatus(Object hb) throws RemoteException {
		if(hb instanceof TrackerHeartBeat)
		{
			
			//Phase 1 of Update
			TrackerHeartBeat heartBeat = (TrackerHeartBeat) hb;
			
			//Check if the JobTracker already has this 
			TaskTrackerInfo taskTrackerInfo = this.jobTracker.getTaskTracker(heartBeat.getTaskTrackerName());
			
			//If not then check-in this TaskTRacker with the JObTracker
			if(taskTrackerInfo == null)
			{
				try {
					//TODO:Abhi to Fix this
					Registry registry = LocateRegistry.getRegistry(heartBeat.getRmiHostName(), 1099);
					TaskTrackerServices taskTrackerServiceReference =  (TaskTrackerServices) registry.lookup(heartBeat.getTaskTrackerServiceName());
					
					//Create a Fresh TaskTracker Info Object
					taskTrackerInfo = new TaskTrackerInfo(heartBeat.getTaskTrackerName(), taskTrackerServiceReference, heartBeat.getMapperSlotsAvailable(), heartBeat.getReducerSlotsAvailable());
					taskTrackerInfo.setTimestamp(System.currentTimeMillis());
				
					this.jobTracker.checkInTaskTracker(taskTrackerInfo);
				
				} catch (NotBoundException e) {
					System.err.println("Could not get reference to the TaskTracker");
				}
				
			}
			else
			{
				taskTrackerInfo.setNumOfMaps(heartBeat.getMapperSlotsAvailable());
				taskTrackerInfo.setNumOfReduces(heartBeat.getReducerSlotsAvailable());
				taskTrackerInfo.setTimestamp(System.currentTimeMillis());
			}
			
			
			//Phase 2 of Update

			
		}
	}

	@Override
	public MapJobsStatus reportMapStatus(int taskID) throws RemoteException {
		return this.jobTracker.checkMapPhaseStatus(taskID);
	}

}
