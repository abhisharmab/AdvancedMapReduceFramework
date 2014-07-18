/**
 * 
 */
package abhi.mapreduce;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author abhisheksharma
 *
 */
public class TaskTrackerServices extends UnicastRemoteObject implements ITaskTrackerServices{

	private TaskTracker taskTrackerReference; 
	
	protected TaskTrackerServices(TaskTracker taskTracker) throws RemoteException {
		this.taskTrackerReference = taskTracker;
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean executeTask() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void updateFieldAgentStatus(Object status) throws RemoteException {
		if(status instanceof TaskProgress)
		{
			TaskProgress progress = (TaskProgress) status;
			this.taskTrackerReference.updateFieldAgentStatus(progress);
		}
		
	}

}
