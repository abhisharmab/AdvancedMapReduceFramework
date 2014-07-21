/**
 * 
 */
package abhi.mapreduce;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author abhisheksharma
 *
 * This represents the information of the TaskTrackers on the JobTracker
 * 
 * Basically it contains some meta-data information about each of the Task Trackers that are running in the 
 * system
 *
 */
public class TaskTrackerInfo implements Comparable<TaskTrackerInfo>, Serializable{

	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	//Threshold for a SLave Node to be considered as alive
	  private final int THRESHOLD_ALIVE_CYCLE = 30000; //milliseconds 

	  // the unique name of task tracker
	  private String taskTrackerName;

	  private static int numOfMaps;

	  private static int numOfReduces;

	  private long timestamp;

	  private Set<Integer> tasksRunning;
	  
	  private ITaskTrackerServices taskTrackerReference;

	  public TaskTrackerInfo(String name, ITaskTrackerServices taskTrackerReference, int pendingMapSlots, int pendingReduceSlots) {
	    this.taskTrackerName = name;
	    this.tasksRunning = new HashSet<Integer>();
	    this.taskTrackerReference = taskTrackerReference;

	    this.numOfMaps = pendingMapSlots;
	   
	    
	    this.numOfReduces = pendingReduceSlots;
	  }

	  public String getTaskTrackerName() {
	    return taskTrackerName;
	  }

	  public long getTimestamp() {
	    return this.timestamp;
	  }

	  public void setTimestamp(long ctime) {
	    this.timestamp = ctime;
	  }

	  public void removeTask(int id) {
	    if (this.tasksRunning.contains(id)) {
	      this.tasksRunning.remove(id);
	    }
	  }

	  public boolean isMachineAlive() {
	    return (System.currentTimeMillis() - this.timestamp <= THRESHOLD_ALIVE_CYCLE);
	  }

	/**
	 * @return the numOfMaps
	 */
	public int getNumOfMaps() {
		return numOfMaps;
	}

	/**
	 * @param numOfMaps the numOfMaps to set
	 */
	public void setNumOfMaps(int numOfMaps) {
		TaskTrackerInfo.numOfMaps = numOfMaps;
	}

	/**
	 * @return the numOfReduces
	 */
	public int getNumOfReduces() {
		return numOfReduces;
	}

	/**
	 * @param numOfReduces the numOfReduces to set
	 */
	public void setNumOfReduces(int numOfReduces) {
		TaskTrackerInfo.numOfReduces = numOfReduces;
	}

	/**
	 * @return the taskTrackerReference
	 */
	public ITaskTrackerServices getTaskTrackerReference() {
		return taskTrackerReference;
	}

	/**
	 * @param taskTrackerReference the taskTrackerReference to set
	 */
	public void setTaskTrackerReference(ITaskTrackerServices taskTrackerReference) {
		this.taskTrackerReference = taskTrackerReference;
	}

	/**
	 * @return the taskRunning
	 */
	public Set<Integer> getTasksRunning() {
		return tasksRunning;
	}

	/**
	 * @param taskRunning the taskRunning to set
	 */
	public void setTasksRunning(Set<Integer> taskRunning) {
		this.tasksRunning = taskRunning;
	}

	@Override
	public int compareTo(TaskTrackerInfo o) {
		// TODO Auto-generated method stub
		return 0;
	}

}