/**
 * 
 */
package abhi.mapreduce;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author abhisheksharma
 *
 *This class represents the TaskTracker Heartbeat Payload. 
 *This message will sent to the JobTracker will all the information about the TaskTracker
 *
 */
public class TrackerHeartBeat implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String taskTrackerName; 
	
	private int mapperSlotsAvailable;
	
	private int reducerSlotsAvailable;
	
	private List<TaskProgress> statusofAllTasks;
	
	private String taskTrackerServiceName;

	private String rmiHostName; 
	
	public TrackerHeartBeat(String name, int mapSlots, int reduceSlots, List<TaskProgress> statusofTasks, String rmiHostName)
	{
		this.taskTrackerName = name;
		this.mapperSlotsAvailable = mapSlots;
		this.reducerSlotsAvailable = reduceSlots;
		this.setTaskTrackerServiceName("TaskTracker_" + this.taskTrackerName);
		this.statusofAllTasks = statusofTasks;
		try {
			this.rmiHostName = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	 * @return the mapperSlotsAvailable
	 */
	public int getMapperSlotsAvailable() {
		return mapperSlotsAvailable;
	}

	/**
	 * @param mapperSlotsAvailable the mapperSlotsAvailable to set
	 */
	public void setMapperSlotsAvailable(int mapperSlotsAvailable) {
		this.mapperSlotsAvailable = mapperSlotsAvailable;
	}

	/**
	 * @return the reducerSlotsAvailable
	 */
	public int getReducerSlotsAvailable() {
		return reducerSlotsAvailable;
	}

	/**
	 * @param reducerSlotsAvailable the reducerSlotsAvailable to set
	 */
	public void setReducerSlotsAvailable(int reducerSlotsAvailable) {
		this.reducerSlotsAvailable = reducerSlotsAvailable;
	}

	/**
	 * @return the statusofAllTasks
	 */
	public List<TaskProgress> getStatusofAllTasks() {
		return statusofAllTasks;
	}

	/**
	 * @param statusofAllTasks the statusofAllTasks to set
	 */
	public void setStatusofAllTasks(List<TaskProgress> statusofAllTasks) {
		this.statusofAllTasks = statusofAllTasks;
	}

	/**
	 * @return the taskTrackerServiceName
	 */
	public String getTaskTrackerServiceName() {
		return taskTrackerServiceName;
	}
	/**
	 * @param taskTrackerServiceName the taskTrackerServiceName to set
	 */
	public void setTaskTrackerServiceName(String taskTrackerServiceName) {
		this.taskTrackerServiceName = taskTrackerServiceName;
	}
	/**
	 * @return the rmiHostName
	 */
	public String getRmiHostName() {
		return rmiHostName;
	}
	/**
	 * @param rmiHostName the rmiHostName to set
	 */
	public void setRmiHostName(String rmiHostName) {
		this.rmiHostName = rmiHostName;
	}
	
	
	
	
}
