/**
 * 
 */
package abhi.mapreduce;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;

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
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see abhi.mapreduce.IJobTrackerServices#submitJob(abhi.mapreduce.JobConf)
	 */
	@Override
	public boolean submitJob(JobConf jconf, Object targetCode) throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see abhi.mapreduce.IJobTrackerServices#updateTaskManagerStatus(java.lang.Object)
	 */
	@Override
	public void updateTaskManagerStatus(Object status) throws RemoteException {
		// TODO Auto-generated method stub

	}

}
