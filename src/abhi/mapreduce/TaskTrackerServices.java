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

	protected TaskTrackerServices() throws RemoteException {
		super();
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

}
