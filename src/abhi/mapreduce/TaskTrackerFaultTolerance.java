/**
 * 
 */
package abhi.mapreduce;

import java.util.Map;
import java.util.Set;

/**
 * @author abhisheksharma
 *
 * This is a thread that will be running by the job-tracker to check of the TaskTRackers are alive or NOT.
 * 
 * The main function of this thread is to check if the TaskTracker is DEAD and if yes try to re-schedule the tasks 
 * that were on the Task Tracker
 * 
 *
 */
public class TaskTrackerFaultTolerance implements Runnable {

	/**
	 * 
	 */
	
	private JobTracker jobTracker; 
	
	public TaskTrackerFaultTolerance(JobTracker jobTracker) {
		this.jobTracker = jobTracker;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
	
	   Map<String, TaskTrackerInfo> taskTrackers = this.jobTracker.getTaskTrackers();

	   for (Map.Entry<String, TaskTrackerInfo> entry : taskTrackers.entrySet()) 
	   {
		   TaskTrackerInfo tInfo = entry.getValue();
		   
		   if(!tInfo.isMachineAlive())
		   {
			   System.out.println("Task Tracker: " + tInfo.getTaskTrackerName() + "Diead. Trying to re-organize the tasks");
			   
			    if (this.jobTracker.getTaskTrackers().containsKey(tInfo)) 
			    {
			    	this.jobTracker.getTaskTrackers().remove(tInfo);
			    }
			    
		        //Re-assign the Tasks
		        Set<Integer> currentRunningTasks = tInfo.getTasksRunning();
		        for (Integer taskID : currentRunningTasks) 
		        {
		          this.jobTracker.reQueueExisitingTask(taskID);
		        }
		   }

	   }
	}

	/**
	 * @return the jobTracker
	 */
	public JobTracker getJobTracker() {
		return jobTracker;
	}

	/**
	 * @param jobTracker the jobTracker to set
	 */
	public void setJobTracker(JobTracker jobTracker) {
		this.jobTracker = jobTracker;
	}

}
