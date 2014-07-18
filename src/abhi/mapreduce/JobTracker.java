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

import abhi.adfs.NameNodeManager;


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
	private NameNodeManager nameNodeReference;

	public JobTracker() throws RemoteException
	{
		try 
		{
			//Register itself to the RMI Registry
			this.jtServiceProvider = new JobTrackerServiceProvider();
			Naming.rebind(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME), this.jtServiceProvider);

			//TODO: Abhi. Get the RemoteReference of the Name Node Registry 
			/*int nameNodeRegistryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_PORT));
			Registry nameNodermiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.NAMENODE_REGISTRY_HOST),nameNodeRegistryPort);
			this.nameNodeReference = (NameNodeManager) nameNodermiRegistry.lookup(SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME));
			*/

			//Initialize the Data Structures
			this.jobIDCounter = 1;
			this.taskIDCounter = 1; 
			
			//Job collections
			this.jobs = Collections.synchronizedMap(new HashMap<Integer, JobInfo>());

			//List of all the Tasks in the Systems.
			//this.mapTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMetaData>());
			//this.reduceTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMetaData>());
			
			
			//New Strategy 
			this.mapperTasks =  Collections.synchronizedMap(new HashMap<Integer,ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>>());
			this.reducerTasks =  Collections.synchronizedMap(new HashMap<Integer,ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>>());
			
			//Basically these are queued Map and Reduce Tasks which are Yet to be Picked up
			//this.queueofMapTasks = new HashMap<String, TaskMetaData>();
			//this.queueofReduceTasks = new HashMap<String, TaskMetaData>();
			
			//New Strategy
			this.mapTaskQueue = new ConcurrentHashMap<TaskMetaData, MapperPriorityQueue>();
			this.reduceTaskQueue = new ConcurrentHashMap<TaskMetaData, ReducerPriorityQueue>();
			
			//Scheduler Strategy 
		    ScheduledExecutorService schExecutor = Executors.newScheduledThreadPool(3);
		    Thread thread = new Thread(new Runnable() {
		      public void run() {
		        makeStrategy();
		      }
		    });
		    thread.setDaemon(true);
		    schExecutor.scheduleAtFixedRate(thread, 0, 2, TimeUnit.SECONDS);
		    //Scheduler Strategy 

		} catch (RemoteException | MalformedURLException e) {
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

	
	//TODO: Abhi this strategy is not going to work. This code is wrong.
	//Get the next MapperTask in Line to be Processed
	
	//Fuck Abhi
	/*
	public TaskMetaData getNextMapperTaskinLineforNode(String taskTrackerName)
	{
		while(!this.mapTasks.isEmpty())
		{
			TaskMetaData task = this.queueofMapTasks.get(taskTrackerName);
			if(this.jobs.get(task.getJobID()).getJobStatus() == SystemConstants.JobStatus.FAILED)
			{
				task.getTaskProgress().setStatus(SystemConstants.TaskStatus.FAILED);
			}
			else
			{
				//TODO: Abhi Make sure you synchronizedly delete the Entry from the Map(Queue) before returning
				return task;
			}
		}
		return null;
	}
	*/

	//Fuck Abhi
	/*
	//Get the next ReducerTask in-line to be Processed
	public TaskMetaData getNextReducerTaskinLineforNode(String taskTrackerName)
	{
		while(!this.reduceTasks.isEmpty())
		{
			TaskMetaData task = this.queueofReduceTasks.get(taskTrackerName);
			if(this.jobs.get(task.getJobID()).getJobStatus() == SystemConstants.JobStatus.FAILED)
			{
				task.getTaskProgress().setStatus(SystemConstants.TaskStatus.FAILED);
			}
			else
			{
				//TODO: Abhi Make sure you synchronizedly delete the Entry from the Map(Queue) before returning
				return task;
			}
		}
		return null;
	}
	*/

	//We need to make sure we check-in all the TaskTrackers that send us heart-beat
	//If we already have added them just ignore otherwise add it to the TaskTrackerInfo List
	public void checkInTaskTracker(TaskTrackerInfo taskTrackerInfo)
	{
		if(!this.taskTrackers.containsKey(taskTrackerInfo.getTaskTrackerName()))
		{
			this.taskTrackers.put(taskTrackerInfo.getTaskTrackerName(), taskTrackerInfo);
		}
	}

	//Check_Out a Task Tracker coz maybe its Dead
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

	/*
	//This method actual picks up the Map and Reduce tasks choosen as per Strategy and asked the TaskTracker to Run it
	public void assignTasks()
	{
		Map<Integer, String> strategy = null;

		// use the system's scheduler to generate the scheduling schemes
		synchronized (this.taskTrackers) {strategy = makeStrategy();}

		if (strategy == null)
			return;

		for (Entry<Integer, String> entry : strategy.entrySet()) {
			Integer taskid = entry.getKey();

			TaskMetaData task = null;

			if (this.mapTasks.containsKey(taskid)) {
				task = this.mapTasks.get(taskid);
			}

			if (this.reduceTasks.containsKey(taskid)) {
				task = this.reduceTasks.get(taskid);
			}

			if (task == null)
				continue;

			// find the specific task tracker
			TaskTrackerInfo targetTasktracker = this.taskTrackers.get(entry.getValue());

			// assign the task to the task-tracker
			boolean result = false;
			try 
			{
				//TODO:Abhi -- Check this code. Written late at Night
				result = targetTasktracker.getTaskTrackerReference().executeTask();
				//The Execute Method Needs to be changed
			} catch (Exception e) {
				result = false;
			}
			if (result) {
				// if this task has been submitted to a task-tracker successfully
				task.getTaskProgress().setStatus(SystemConstants.TaskStatus.INPROGRESS);
			} else {
				// if this task is failed to be submitted, place it back on the Map
				if (task.isMapperTask()) 
				{
					this.queueofMapTasks.put(entry.getValue(), task);
				} else 
				{
					this.queueofMapTasks.put(entry.getValue(), task);
				}
			}
		}
	}*/

	//Called to Queue-Up A Task
	public void queueUpTask(TaskMetaData taskMetaData)
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
	
	public static void main(String[] args) 
	{
		try {
			JobTracker jt = new JobTracker();
		} catch (RemoteException e) {
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
			if(entry.getValue().peek().getNumOfMaps() > 0)
			{
				boolean result = false;
				try
				{
					//Get the relevant TaskTracker from the Queue who can Execute this Job
					TaskTrackerInfo taskTrackerInfo = entry.getValue().poll();
					
					//Send the Task to the Appropriate TaskTracker
					result = taskTrackerInfo.getTaskTrackerReference().executeTask(entry.getKey());
				}
				catch(Exception e)
				{
					System.out.println("Could not execute Mapper Task");
				}
				if(result)
					entry.getKey().getTaskProgress().setStatus(SystemConstants.TaskStatus.INPROGRESS);
				else
				{
					this.mapTaskQueue.put(entry.getKey(), entry.getValue());
				}
			}
		}
		
		//Try to schedule the Reducer Tasks
		for(Entry<TaskMetaData, ReducerPriorityQueue> entry: this.reduceTaskQueue.entrySet())
		{
			if(entry.getValue().peek().getNumOfReduces() > 0)
			{
				boolean result = false;
				try
				{
					//Get the relevant TaskTracker from the Queue who can Execute this Job
					TaskTrackerInfo taskTrackerInfo = entry.getValue().poll();
					result = taskTrackerInfo.getTaskTrackerReference().executeTask(entry.getKey());
				}
				catch(Exception e)
				{
					System.out.println("Could not execute Reduce Task");
				}
				if(result)
					entry.getKey().getTaskProgress().setStatus(SystemConstants.TaskStatus.INPROGRESS);
				else
				{
					this.reduceTaskQueue.put(entry.getKey(), entry.getValue());
				}
			}
		}

	}


	//TODO:Abhi
	public void submitJob(JobInfo jobInfo) {
		//1. Talk to the NameNode and get the Chunk Information 
		 	//1.1 As the appropriate Slave to move the JAR to all the NODES
		
		//2. Construct fresh objects of MapTask and ReduceTasks (TaskMetaData basically)
			// DKREW : When making the ReduceTask(TaskMetaData) add in the partition Number
			// if the user wants 3 partition
			// TeskMetaData.setPartitionNumber(1)..........
		//3. Add it to the Maps appropriate for them to be taken up for scheduling 
		//4. Add this JOb into the Jobs Data Structure 
		//5. Set the status of the Job In-Progress
		//6. Assign the and distributed the Tasks
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

