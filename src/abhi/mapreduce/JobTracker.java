/**
 * 
 */
package abhi.mapreduce;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;






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
public class JobTracker {

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
	private Map<Integer, TaskMetaData> mapTasks;

	//List of all the ReduceTasks in the System
	private Map<Integer, TaskMetaData> reduceTasks;

	// the map task queue lined up for execution
	private Queue<TaskMetaData> queueofMapTasks;

	// the reduce task queue lined up for execution
	private Queue<TaskMetaData> queueofReduceTasks;


	public JobTracker() throws RemoteException
	{
		try 
		{
			//Register itself to the RMI Registry
			this.jtServiceProvider = new JobTrackerServiceProvider();
			Naming.rebind(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME), this.jtServiceProvider);

			//Initialize the Data Structures
			this.jobIDCounter = 1;
			this.taskIDCounter = 1; 

			this.mapTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMetaData>());
			this.reduceTasks = Collections.synchronizedMap(new HashMap<Integer, TaskMetaData>());
			this.jobs = Collections.synchronizedMap(new HashMap<Integer, JobInfo>());


			//Priority Queues for the Map and Reduce Task
			this.queueofMapTasks = (Queue<TaskMetaData>) (new PriorityQueue<TaskMetaData>(10,
					new Comparator<TaskMetaData>() {

				@Override
				public int compare(TaskMetaData o1, TaskMetaData o2) {
					return o1.getJobID() - o2.getJobID();
				}

			}));

			this.queueofReduceTasks = (Queue<TaskMetaData>) (new PriorityQueue<TaskMetaData>(10,
					new Comparator<TaskMetaData>() {

				@Override
				public int compare(TaskMetaData o1, TaskMetaData o2) {
					return o1.getJobID() - o2.getJobID();
				}

			}));

		} catch (RemoteException | MalformedURLException e) {
			System.err.println("Could not Register to the RMI Registry");
			e.printStackTrace();
		}

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
	
	
	  //Check_Out a Task Tracker coz maybe its Dead
	  public void checkOutTaskTracker(String name) {
	    if (name == null)
	      return;

	    if (this.taskTrackers.containsKey(name)) {
	      this.taskTrackers.remove(name);

	    }
	  }
	
	  /**
	   * get the whole list of task trackers
	   * 
	   * @return
	   */
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



	public static void main(String[] args) 
	{
		try {
			JobTracker jt = new JobTracker();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
