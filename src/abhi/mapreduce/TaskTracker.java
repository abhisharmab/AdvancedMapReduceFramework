/**
 * 
 */
package abhi.mapreduce;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;



/**
 * @author abhisheksharma
 * This class is the TaskTracker. 
 * 
 * TaskTracker runs on each of the node in the cluster and is sort of the local task manager on each of the machine 
 * Task Tracker is responsible for starting the appropriate map and reduce tasks as requested by the JobTracker 
 * 
 * Primary functions of the TaskTracker are: 
 * 1. Actually run the Tasks no the machine 
 * 2. Gather the status of all the accomodate Jobs on the Machine 
 * 		-Gather the Status 
 * 3. Send heart-beat to the JobTracker about it being alive periodically, 
 * 		-Send the Status out 
 *
 */

//Reference for Daemon Thread Monitoring
//http://www.journaldev.com/1072/java-daemon-thread-example


public class TaskTracker {

	private String taskTrackerName;

	private final int hb_period = 4;

	private IJobTrackerServices jobTrackerServices;

	private HashMap<Integer, TaskProgress> statusofAllTasks;

	//The synchronized function needs a Wrapper. Since it works only on Objects
	public Integer countofRunningMapperFieldAgents; 

	//The synchronized function needs a Wrapper. Since it works only on Objects
	public  Integer countofRunningReducerFieldAgents;

	public int mapperSlotCapacity;
	public int reducerSlotCapacity;

	//TODO:Abhi make this configurable
	private final long aliveThreshold = 8000;

	public TaskTracker()
	{
		//Get the JobTrackerServiceProvider Reference
		int registryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.JOBTRACKER_REGISTRY_PORT));
		Registry rmiRegistry;
		try {
			rmiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.JOBTRACKER_REGISTRY_HOST),registryPort);
			this.setJobTrackerServices((IJobTrackerServices) rmiRegistry.lookup(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME)));
		} catch (RemoteException | NotBoundException e) {
			System.err.println("Could bind to the JobTracker Registry Error Occured");
			e.printStackTrace();
		}

		//Set the taskTrackerName
		try {
			this.taskTrackerName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Register the TaskTrackerServices in the RMI Registry
		try 
		{
			TaskTrackerServices tServices = new TaskTrackerServices(this);
			Naming.rebind("TaskTracker_" + this.taskTrackerName, tServices);
		} catch (RemoteException | MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.mapperSlotCapacity = Integer.parseInt(SystemConstants.getConfig(SystemConstants.MAX_MAPPER_SLOTS));
		this.reducerSlotCapacity = Integer.parseInt(SystemConstants.getConfig(SystemConstants.MAX_REDUCER_SLOTS));
		this.countofRunningMapperFieldAgents = new Integer(0);
		this.countofRunningReducerFieldAgents = new Integer(0);

		this.statusofAllTasks = new HashMap<Integer,TaskProgress>();

	}

//	//Called to actually start a Mapper or Reducer Task
//	public void executeTask() throws RemoteException
//	{
//	
//	}
//	
	
	//Each of the Spawned Off Mapper or Reducer Field Agent Will Call Upon this Function to Update their Status
	//Each field agent will have a reference of the main co-ordinating Boss that is the TaskTracker
	public void updateFieldAgentStatus(TaskProgress progress) throws RemoteException
	{
		synchronized(this.statusofAllTasks)
		{
			if(this.statusofAllTasks.containsKey(progress.getTaskID()))
			{
				this.statusofAllTasks.remove(progress.getTaskID());
			}
			
			this.statusofAllTasks.put(progress.getTaskID(), progress);
		}
	}

	public static void main(String[] args) {
		TaskTracker taskTracker = new TaskTracker();
		taskTracker.fireUp();
	}

	private void fireUp() {
		Thread trackerTaskHealthMonitor = new Thread(new Runnable() {
			@Override
			public void run() {
				synchronized (statusofAllTasks) {
					for (TaskProgress taskProgress : statusofAllTasks.values()) {
						if ((System.currentTimeMillis() - taskProgress.getLatestUpdateTimeStamp() > aliveThreshold)
								&& taskProgress.getStatus() != SystemConstants.TaskStatus.SUCCEEDED) 
						{
							taskProgress.setStatus(SystemConstants.TaskStatus.FAILED);
						}
					}
				}

			}
		}
				);


		Thread trackerTaskStatusUpdater = new Thread(new Runnable() {
			@Override
			public void run() {
				List<TaskProgress> currentTaskList;
				synchronized (statusofAllTasks) {
					currentTaskList = new ArrayList<TaskProgress>(statusofAllTasks.values());
					
					ArrayList<Integer> toDelete = new ArrayList<Integer>();
					for (int id : statusofAllTasks.keySet()) {
						if (statusofAllTasks.get(id).getStatus() != SystemConstants.TaskStatus.INPROGRESS) {
							toDelete.add(id); //Add to a list to delete

							//Mark Slots as Available
							if (statusofAllTasks.get(id).getTaskType() == SystemConstants.TaskType.MAPPER)
								synchronized (countofRunningMapperFieldAgents) {
									countofRunningMapperFieldAgents--;
								}
							else
								synchronized (countofRunningReducerFieldAgents) {
									countofRunningReducerFieldAgents--;
								}
						}
					}

			          //Delete the entry from the Task Status. 
					 // We do not need to maintain status of these tasks anymore
			          for (int id : toDelete) {
			        	  statusofAllTasks.remove(id);
			          }
					
			          //Send the update information to the JobTracker

			          TrackerHeartBeat hb = null;
			          synchronized (countofRunningMapperFieldAgents) {
			            synchronized (countofRunningReducerFieldAgents) {
			            	hb = new TrackerHeartBeat(taskTrackerName,
			            			mapperSlotCapacity - countofRunningMapperFieldAgents,
			                        reducerSlotCapacity  - countofRunningReducerFieldAgents,
			                        currentTaskList, taskTrackerName);
			            }
			          }
			          /* send update package */
			          if (hb != null)
			            try {
			            	jobTrackerServices.updateTaskManagerStatus(hb);
			            } catch (RemoteException e) {
			              e.printStackTrace();
			            }
				}
			}
		});

		trackerTaskHealthMonitor.setDaemon(true);
		trackerTaskStatusUpdater.setDaemon(true);

	    ScheduledExecutorService schExecService = Executors.newScheduledThreadPool(2);
	    ScheduledFuture<?> healthChecker = schExecService.scheduleAtFixedRate(trackerTaskHealthMonitor, 0,
	            hb_period, TimeUnit.SECONDS);
	    ScheduledFuture<?> statusUpdater = schExecService.scheduleAtFixedRate(trackerTaskStatusUpdater, 0,
	    		hb_period, TimeUnit.SECONDS);
	}

 

	/**
	 * @return the taskTrackerName
	 */
	public String getTaskTrackerName() {
		return taskTrackerName;
	}
	/**
	 * @param taskTrackerName the taskTrackerName to set
	 */
	public void setTaskTrackerName(String taskTrackerName) {
		this.taskTrackerName = taskTrackerName;
	}

	/**
	 * @return the statusofAllTasks
	 */
	public HashMap<Integer, TaskProgress> getStatusofAllTasks() {
		return statusofAllTasks;
	}
	/**
	 * @param statusofAllTasks the statusofAllTasks to set
	 */
	public void setStatusofAllTasks(HashMap<Integer, TaskProgress> statusofAllTasks) {
		this.statusofAllTasks = statusofAllTasks;
	}
	/**
	 * @return the countofRunningMapperFieldAgents
	 */
	public int getCountofRunningMapperFieldAgents() {
		return countofRunningMapperFieldAgents;
	}
	/**
	 * @param countofRunningMapperFieldAgents the countofRunningMapperFieldAgents to set
	 */
	public void setCountofRunningMapperFieldAgents(
			int countofRunningMapperFieldAgents) {
		this.countofRunningMapperFieldAgents = countofRunningMapperFieldAgents;
	}

	/**
	 * @return the countofRunningReducerFieldAgents
	 */
	public int getCountofRunningReducerFieldAgents() {
		return countofRunningReducerFieldAgents;
	}
	/**
	 * @param countofRunningReducerFieldAgents the countofRunningReducerFieldAgents to set
	 */
	public void setCountofRunningReducerFieldAgents(
			int countofRunningReducerFieldAgents) {
		this.countofRunningReducerFieldAgents = countofRunningReducerFieldAgents;
	}




	/**
	 * @return the aliveThreshold
	 */
	public long getAliveThreshold() {
		return aliveThreshold;
	}

	public IJobTrackerServices getJobTrackerServices() {
		return jobTrackerServices;
	}

	public void setJobTrackerServices(IJobTrackerServices jobTrackerServices) {
		this.jobTrackerServices = jobTrackerServices;
	}





}
