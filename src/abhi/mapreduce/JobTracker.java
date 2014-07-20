/**
 * 
 */
package abhi.mapreduce;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import abhi.adfs.InputFileInfo;
import abhi.adfs.NameNodeMaster;


/**
 * @author abhisheksharma
 *
 * This class represents the JobTracker. JobTracker is the Main guy running the show for the MapReduce Tasks. 
 * The primary functions of the JobTracker are just like the JobTracker is Hadoop. 
 * 
 * Some of the core functions are: 
 * 1. Listen to requests from JobClient and address the JObs. 
 * 2. Trigger the starting,stopping or killing the Map Reduce Task on the TaskTrackers
 * 3. Maintain Meta-data information about the task-status of each of the running Jobs
 * 4. Load-Balance the tasks running on each of the Node in the cluster
 * 5. Maintain a list of all the nodes and their respective status. Make sure they are alive.
 * 6. Periodically update JobClient about the particular running job.
 */
public class JobTracker implements IDefineSchedulingStrategy{

	//The counter that will incrementing as we add more Jobs
	private int jobIDCounter;

	//The counter that will incrementing as we add more Tasks
	private int taskIDCounter;

	//This is the face of the JobTracker exposed to the rest of the world via RMIRegsitry
	private JobTrackerServiceProvider jtServiceProvider;

	//This is a cache of all the Task Tracker Reference from the RMI Registry 
	//As we need the taskTrackers we will fetch the reference once and then keep it locally until there is a problem 
	private Map<String, TaskTrackerInfo> taskTrackers;

	//All the Jobs Information that has ever been requested to be performed by the JobTracker
	private Map<Integer, JobInfo> jobs;

	//List of all the MapTasks in the System
	//private Map<Integer, TaskMetaData> mapTasks;

	//List of all the ReduceTasks in the System
	//private Map<Integer, TaskMetaData> reduceTasks;

	// the map task queue lined up for execution
	//private Map<String,TaskMetaData> queueofMapTasks;

	// the reduce task queue lined up for execution
	//private Map<String, TaskMetaData> queueofReduceTasks;
	
	/*New Strategy*/
	private Map<Integer, ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>> mapperTasks;
	private Map<Integer, ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>> reducerTasks;
	
	//The Data Structures below that will be used for Scheduling Tasks
	private ConcurrentHashMap<TaskMetaData, MapperPriorityQueue> mapTaskQueue;
	private ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue> reduceTaskQueue;
	
	/*New Strategy*/
	
	// the nameNodeReference for finding out the file-splits
	private NameNodeMaster nameNodeReference;

	public JobTracker() throws RemoteException
	{
		try 
		{
			//Register itself to the RMI Registry
			this.jtServiceProvider = new JobTrackerServiceProvider(this);
			Naming.rebind(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME), this.jtServiceProvider);

			 
			int nameNodeRegistryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_PORT));
			Registry nameNodermiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_HOST),nameNodeRegistryPort);
			this.nameNodeReference = (NameNodeMaster) nameNodermiRegistry.lookup(SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME));
			

			//Initialize the Data Structures
			this.jobIDCounter = 1;
			this.taskIDCounter = 1; 
			
			//Job collections
			this.jobs = Collections.synchronizedMap(new HashMap<Integer, JobInfo>());

			
			//New Strategy 
			this.mapperTasks =  Collections.synchronizedMap(new HashMap<Integer,ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>>());
			this.reducerTasks =  Collections.synchronizedMap(new HashMap<Integer,ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>>());
			

			//New Strategy
			this.mapTaskQueue = new ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>();
			this.reduceTaskQueue = new ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>();
			
			//Scheduler Strategy 
		    ScheduledExecutorService schExecutor = Executors.newScheduledThreadPool(1);
		    Thread thread = new Thread(new Runnable() {
		      public void run() {
		        makeStrategy();
		      }
		    });
		    
		    thread.setDaemon(true);
		    schExecutor.scheduleAtFixedRate(thread, 0, 5, TimeUnit.SECONDS);
		     

		} catch (RemoteException | MalformedURLException | NotBoundException e) {
			System.err.println("Could not Register to the RMI Registry");
			e.printStackTrace();
		} 
		
	}
	
	//Get a particular Job Info
	public JobInfo getJobInfobyId(int jobID)
	{
		return this.jobs.get(jobID);
	}
	

	//Methods for JobTracker to assign new TaskIDs and JobIDs
	public int nextJobId()
	{
		return ++this.jobIDCounter;
	}

	public int nextTaskId()
	{
		return ++this.taskIDCounter;
	}


	//We need to make sure we check-in all the TaskTrackers that send us heart-beat
	//If we already have added them just ignore otherwise add it to the TaskTrackerInfo List
	public void checkInTaskTracker(TaskTrackerInfo taskTrackerInfo)
	{
		if(!this.taskTrackers.containsKey(taskTrackerInfo.getTaskTrackerName()))
		{
			this.taskTrackers.put(taskTrackerInfo.getTaskTrackerName(), taskTrackerInfo);
		}
	}

	//Check_Out a Task Tracker because maybe its Dead
	public void checkOutTaskTracker(String name) {
		if (name == null)
			return;

		if (this.taskTrackers.containsKey(name)) {
			this.taskTrackers.remove(name);

		}
	}

   //Get all the TaskTRackers. This will be needed for Scheduling Strategy
	public Map<String, TaskTrackerInfo> getTaskTrackers() {
		return Collections.unmodifiableMap(this.taskTrackers);
	}


	//Retrieve a specific task tracker
	public TaskTrackerInfo getTaskTracker(String id) {
		if (this.taskTrackers.containsKey(id)) {
			return this.taskTrackers.get(id);
		} else {
			return null;
		}
	}


	//Called to Queue-Up A Task
	public void queueUpFailedTask(TaskMetaData taskMetaData)
	{
		if(taskMetaData.isMapperTask())
		{
			ArrayList<MapperPriorityQueue> temp = Collections.list(this.mapperTasks.get(taskMetaData.getTaskID()).elements());
			if(temp.get(0)!= null)
				this.mapTaskQueue.put(taskMetaData, temp.get(0));
		} 
		else
		{
			ArrayList<ReducerPriorityQueue> temp = Collections.list(this.reducerTasks.get(taskMetaData.getTaskID()).elements());
			if(temp.get(0)!= null)
				this.reduceTaskQueue.put(taskMetaData, temp.get(0));
		}
	}
	
	//Re-Queue and Existing Task if the TaskTracker Fails. We will have some Alternative Task-Trackers to Re-Do the Task.
	public void reQueueExisitingTask(int taskID)
	{
		if(this.mapperTasks.containsKey(taskID))
		{
			this.mapTaskQueue.put(Collections.list(this.mapperTasks.get(taskID).keys()).get(0), Collections.list(this.mapperTasks.get(taskID).elements()).get(0));
		}
		else
		{
			this.reduceTaskQueue.put(Collections.list(this.reducerTasks.get(taskID).keys()).get(0), Collections.list(this.reducerTasks.get(taskID).elements()).get(0));
		}
	}
	
	public static void main(String[] args) 
	{
		try {
			JobTracker jt = new JobTracker();
			
		    //TaskTracker Fault Tolerance Thread
		    ScheduledExecutorService faultyTaskTrackers = Executors.newScheduledThreadPool(1);
		    TaskTrackerFaultTolerance faultTolerance = new TaskTrackerFaultTolerance(jt);
		    faultyTaskTrackers.scheduleAtFixedRate(faultTolerance, 10, 10,TimeUnit.SECONDS);
		} 
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
	public void makeStrategy() 
	{
		//Try to schedule Mapper Tasks
		for(Entry<TaskMetaData, MapperPriorityQueue> entry: this.mapTaskQueue.entrySet())
		{
			TaskTrackerInfo taskTrackerInfo = null;
			if(entry.getValue().peek().getNumOfMaps() > 0)
			{
				boolean result = false;
				try
				{
					//Get the relevant TaskTracker from the Queue who can Execute this Job
					 taskTrackerInfo = entry.getValue().poll();
					
					//Send the Task to the Appropriate TaskTracker
					result = taskTrackerInfo.getTaskTrackerReference().executeTask(entry.getKey());
				}
				catch(Exception e)
				{
					System.out.println("Could not execute Mapper Task");
				}
				
				if(result)
				{
					entry.getKey().getTaskProgress().setStatus(SystemConstants.TaskStatus.INPROGRESS);
					synchronized(this.mapTaskQueue)
					{
						this.mapTaskQueue.remove(entry);
					}
				}
				else
				{
					if(taskTrackerInfo != null)
					{
						entry.getValue().add(taskTrackerInfo);
						synchronized(this.mapTaskQueue)
						{
							this.mapTaskQueue.remove(entry);
							this.mapTaskQueue.put(entry.getKey(), entry.getValue());
						}
					}
				}
			}
		}
		
		//Try to schedule the Reducer Tasks
		for(Entry<TaskMetaData, ReducerPriorityQueue> entry: this.reduceTaskQueue.entrySet())
		{
			TaskTrackerInfo taskTrackerInfo = null;
			if(entry.getValue().peek().getNumOfReduces() > 0)
			{
				boolean result = false;
				try
				{
					//Get the relevant TaskTracker from the Queue who can Execute this Job
					taskTrackerInfo = entry.getValue().poll();
					result = taskTrackerInfo.getTaskTrackerReference().executeTask(entry.getKey());
				}
				catch(Exception e)
				{
					System.out.println("Could not execute Reduce Task");
				}
				if(result)
				{
					entry.getKey().getTaskProgress().setStatus(SystemConstants.TaskStatus.INPROGRESS);
					synchronized(this.reduceTaskQueue)
					{
						this.reduceTaskQueue.remove(entry);
					}
				}
				else
				{
					if(taskTrackerInfo != null)
					{
						entry.getValue().add(taskTrackerInfo);
						
						synchronized(this.reduceTaskQueue)
						{
							this.reduceTaskQueue.remove(entry);
							this.reduceTaskQueue.put(entry.getKey(), entry.getValue());
						}
					}
				}
			}
		}

	}


	//TODO:Abhi
	public void submitJob(JobInfo jobInfo) {
		//1. Talk to the NameNode and get the Chunk Information 
		 	//1.1 As the appropriate Slave to move the JAR to all the NODES
		try 
		{
			if(this.nameNodeReference.checkFileExistence(jobInfo.getJobConf().getInputPath()))
			{
				InputFileInfo inputFileInfo = this.nameNodeReference.getInputFileInfo(jobInfo.getJobConf().getInputPath());
				HashMap<String, List<String>> partitionInfo = inputFileInfo.getTranspose();
				
				//Instantiate the Priority Queue for the MapperTask and ReducerTasks(This List keeps a Heap of Where POssibly we might run it)
				MapperPriorityQueue mQ = new MapperPriorityQueue(10);
				ReducerPriorityQueue rQ = new ReducerPriorityQueue(10);
				
				Map<Integer, ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>> mTasks = new HashMap<Integer, ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>>();				
				Map<Integer, ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>> rTasks = new HashMap<Integer, ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>>();
				
				for(Map.Entry<String, List<String>> partitionEntry: partitionInfo.entrySet())
				{
					int taskID = this.nextTaskId();
					String chunkInputName = partitionEntry.getKey();
					
				
					//Get the List of the Possible Deployments
					List<String> possibleDeploymentNodes = partitionEntry.getValue();
					
					for(String dataNode : possibleDeploymentNodes)
					{
						String taskTrackerName = "TaskTracker_" + dataNode.split("_")[1].toString();
						
						if(this.taskTrackers.containsKey(taskTrackerName))
						{
							//Possible list of Slaves the TaskCould Run on Based on the File Chunks
							mQ.add(this.taskTrackers.get(taskTrackerName));
							rQ.add(this.taskTrackers.get(taskTrackerName)); 
						}
					}
					
					//Build a Map Task Meta Data need to execute a Task
					TaskProgress taskProgress = new TaskProgress(taskID,SystemConstants.TaskType.MAPPER);
					TaskMetaData mapTask = new TaskMetaData(jobInfo.getJobID(),taskID, SystemConstants.TaskType.MAPPER,
											taskProgress, chunkInputName, String.valueOf(jobInfo.getJobID()),
											jobInfo.getJobConf().getInputFormatClassName(), jobInfo.getJobConf().getOutputFormatClassName(),
											jobInfo.getJobConf().getMapperClassName(),null,jobInfo.getJobConf().getPartitionerClassName(),jobInfo.getJobConf().getReducerNum(),0);
					
					//Add this Task to the Map Queue 
					this.mapTaskQueue.put(mapTask, mQ);
					
					//Add this Task to the MTasks
					ConcurrentHashMap<TaskMetaData, MapperPriorityQueue> temp = new ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>();
					temp.put(mapTask, mQ);
					mTasks.put(taskID, temp);
					
					//Also the jobInfo that this Task in Under you
					jobInfo.progressofallTasks.put(taskID, taskProgress);

				}
				
				for(int i=0; i < jobInfo.getJobConf().getReducerNum() ; i++)
				{
					int taskID = this.nextTaskId();
					TaskProgress taskProgress = new TaskProgress(taskID,SystemConstants.TaskType.REDUCER);
					
					TaskMetaData reduceTask = new TaskMetaData(jobInfo.getJobID(),taskID, SystemConstants.TaskType.REDUCER,
							taskProgress, null, String.valueOf(jobInfo.getJobID()),
							jobInfo.getJobConf().getInputFormatClassName(), jobInfo.getJobConf().getOutputFormatClassName(),
							null,jobInfo.getJobConf().getReducerClassName(),null,0,i);
					
					//Add this Task to the Map Queue 
					this.reduceTaskQueue.put(reduceTask, rQ);
					
					//Add this Task to the RTasks	
					ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue> temp = new ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>();
					temp.put(reduceTask, rQ);
					rTasks.put(taskID,temp);
					
					//Also the jobInfo that this Task in Under you
					jobInfo.progressofallTasks.put(taskID, taskProgress);
				}
				
				//Add it to all the other DataStructures in the JobTracker
				this.mapperTasks.putAll(mTasks);
				this.reducerTasks.putAll(rTasks);
				
				this.jobs.put(jobInfo.getJobID(), jobInfo);
				jobInfo.setJobStatus(SystemConstants.JobStatus.INPROGRESS);
			}
		} 
		catch (RemoteException e) {
			// TODO Auto-generated catch block
			System.err.println("Could not contact NameNodeReference");
			e.printStackTrace();
		}
		
	}

	
	//This function is to the check the status of the Map Phase for a particular Job
	public SystemConstants.MapJobsStatus checkMapPhaseStatus(int taskID) {
		TaskMetaData task = Collections.list(this.reducerTasks.get(taskID).keys()).get(0);

		if (task == null) {
			return SystemConstants.MapJobsStatus.INPROGRESS;
		}

		JobInfo job = this.jobs.get(task.getJobID());

		if (job == null || job.getJobStatus() == SystemConstants.JobStatus.FAILED) {
			return SystemConstants.MapJobsStatus.FAILED;
		}

		List<TaskProgress> mapTasksProgress = Collections.list(job.getProgressofallTasks().elements());
		for (TaskProgress mtaskProgress : mapTasksProgress) {
			if (this.mapperTasks.containsKey(mtaskProgress.getTaskID()) && 
					!Collections.list(this.mapperTasks.get(mtaskProgress.getTaskID()).keys()).get(0).isTaskDone())
				return SystemConstants.MapJobsStatus.INPROGRESS;
		}

		// if all map tasks finished, then return FINISHED
		return SystemConstants.MapJobsStatus.SUCCEEDED;
	}
	
	//To return HOSTNAME which is the Origin of the Job
	public String getJobOriginHostNamebyTaskID(int taskID)
	{
		TaskMetaData task = Collections.list(this.reducerTasks.get(taskID).keys()).get(0);
		return this.jobs.get(task.getJobID()).getJobConf().getJobRequestOriginHostName();
	}
	
	// This will return the list of TaskProcess that is related to the reducer.
	public List<TaskProgress> getCompletedMapTasks(int reducerTaskID){
		TaskMetaData task =  Collections.list(this.reducerTasks.get(reducerTaskID).keys()).get(0);
		JobInfo job = this.jobs.get(task.getJobID());
		List<TaskProgress> mapTasksProgress = Collections.list(job.getProgressofallTasks().elements());
		for (TaskProgress mtaskProgress : mapTasksProgress) {
			if (!Collections.list(this.mapperTasks.get(mtaskProgress.getTaskID()).keys()).get(0).isTaskDone()){
				System.out.println("There are map tasks are not complete.");
				return null;
			}
				
		}
		return mapTasksProgress;
	}

	/**
	 * @return the mapTaskQueue
	 */
	public ConcurrentHashMap<TaskMetaData, MapperPriorityQueue> getMapTaskQueue() {
		return mapTaskQueue;
	}

	/**
	 * @param mapTaskQueue the mapTaskQueue to set
	 */
	public void setMapTaskQueue(ConcurrentHashMap<TaskMetaData, MapperPriorityQueue> mapTaskQueue) {
		this.mapTaskQueue = mapTaskQueue;
	}

	/**
	 * @return the reduceTaskQueue
	 */
	public ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue> getReduceTaskQueue() {
		return reduceTaskQueue;
	}

	/**
	 * @param reduceTaskQueue the reduceTaskQueue to set
	 */
	public void setReduceTaskQueue(ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue> reduceTaskQueue) {
		this.reduceTaskQueue = reduceTaskQueue;
	}

	/**
	 * @return the reducerTasks
	 */
	public Map<Integer, ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>> getReducerTasks() {
		return reducerTasks;
	}

	/**
	 * @param reducerTasks the reducerTasks to set
	 */
	public void setReducerTasks(Map<Integer, ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>> reducerTasks) {
		this.reducerTasks = reducerTasks;
	}

	/**
	 * @return the mapperTasks
	 */
	public Map<Integer, ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>> getMapperTasks() {
		return mapperTasks;
	}

	/**
	 * @param mapperTasks the mapperTasks to set
	 */
	public void setMapperTasks(Map<Integer, ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>> mapperTasks) {
		this.mapperTasks = mapperTasks;
	}



}

