/**
 * 
 */
package abhi.mapreduce;

import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

	protected JobTrackerServiceProvider(JobTracker jobTracker) throws RemoteException {
		this.jobTracker = jobTracker;
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
	public boolean submitJob(JobConf jconf) throws RemoteException {
		if(jconf == null)
			return false;
		JobInfo jobInfo = new JobInfo(jconf);

		this.jobTracker.submitJob(jobInfo);

		//Call this to assign Tasks
		//this.jobTracker.assignTasks();

		return true;
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
			TaskTrackerInfo taskTrackerInfo = this.jobTracker.getTaskTracker(heartBeat.getTaskTrackerServiceName());

			//If not then check-in this TaskTRacker with the JObTracker
			if(taskTrackerInfo == null)
			{
				try {
					//TODO:Abhi to Fix this
					Registry registry = LocateRegistry.getRegistry(heartBeat.getRmiHostName(), 1099);
					ITaskTrackerServices taskTrackerServiceReference =  (ITaskTrackerServices) registry.lookup(heartBeat.getTaskTrackerServiceName());

					//Create a Fresh TaskTracker Info Object
					taskTrackerInfo = new TaskTrackerInfo(heartBeat.getTaskTrackerServiceName(), taskTrackerServiceReference, heartBeat.getMapperSlotsAvailable(), heartBeat.getReducerSlotsAvailable());
					taskTrackerInfo.setTimestamp(System.currentTimeMillis());

					this.jobTracker.checkInTaskTracker(taskTrackerInfo);

				} catch (NotBoundException e ) {
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
			//Make sure the Task Exists on the JobTracker Side 
			//Update the TaskMetaData on the JObTRackers
			//Based on the status of the task take the appropriate action. 
			List<TaskProgress> progressList = heartBeat.getStatusofAllTasks();
			
			Map<Integer, ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>> mapTaskList = this.jobTracker.getMapperTasks();
			Map<Integer, ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>> reduceTaskList = this.jobTracker.getReducerTasks();

			for(TaskProgress taskProgress : progressList)
			{
				TaskMetaData taskMetaData = null; 

				if(mapTaskList.containsKey(taskProgress.getTaskID()))
				{
					System.out.println("we are getting the taskMetaData     MAP");
					
					taskMetaData = Collections.list(mapTaskList.get(taskProgress.getTaskID()).keys()).get(0);
					System.out.println(taskMetaData.getJobID());
					System.out.println(taskMetaData.getReducer());
					System.out.println(taskMetaData.isTaskDone());
					
					System.out.println("We got this from the MAP");
					System.out.println("map said === " + taskProgress.getStatus().toString());
				}
				else if (reduceTaskList.containsKey(taskProgress.getTaskID()))
				{
					System.out.println("we are getting the taskMetaData    REDUCE");
					taskMetaData = Collections.list(reduceTaskList.get(taskProgress.getTaskID()).keys()).get(0);

				}

				if(taskMetaData == null)
					continue;

				//In the TaskProgress set whoever is running the Latest Task
				taskProgress.setTaskTrackerName(heartBeat.getTaskTrackerServiceName());
				
				taskMetaData.setTaskProgress(taskProgress);

				//Get the jobInfo
				JobInfo jobInfo = this.jobTracker.getJobInfobyId(taskMetaData.getJobID());
				jobInfo.updateTaskProgress(taskProgress);

				if(taskProgress.getStatus() == SystemConstants.TaskStatus.INPROGRESS)
				{
					//Do Nothing. Update Data Structures and let the things run.
					continue;
				}
				else if(taskProgress.getStatus() == SystemConstants.TaskStatus.SUCCEEDED)
				{
					System.out.println("		else if(taskProgress.getStatus() == SystemConstants.TaskStatus.SUCCEEDED)");
					//Remove the Task from the TaskTracker List since its done.
					taskTrackerInfo.removeTask(taskProgress.getTaskID());

					//Double check if this was the last task. Who knows maybe the Job is Done.
					if(jobInfo.isJobDone())
						jobInfo.setJobStatus(SystemConstants.JobStatus.SUCCEEDED);

				}
				else if(taskProgress.getStatus() == SystemConstants.TaskStatus.FAILED)
				{
					taskMetaData.increaseAttempts();
					if(taskMetaData.getAttempts() <= TaskMetaData.MAXIMUM_TRIES)
					{
						this.jobTracker.queueUpFailedTask(taskMetaData);
					}
					else
					{
						jobInfo.setJobStatus(SystemConstants.JobStatus.FAILED);
					}
				}
			}
		}
	}

	@Override
	public MapJobsStatus reportMapStatus(int taskID) throws RemoteException {
		return this.jobTracker.checkMapPhaseStatus(taskID);
	}

	@Override
	public String getJobOriginHostNamebyTaskID(int taskID)
	{
		return this.jobTracker.getJobOriginHostNamebyTaskID(taskID);
	}
	
	
	@Override
	public List<TaskProgress> getCompletedMapTasks(int reducerTaskID)
			throws RemoteException {
		return this.jobTracker.getCompletedMapTasks(reducerTaskID);
	}


	@Override
	public JobInfo getLiveStatusofJob(int jobID) {
		return this.jobTracker.getJobInfobyId(jobID);		
	}

}
