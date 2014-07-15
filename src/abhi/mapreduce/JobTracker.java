/**
 * 
 */
package abhi.mapreduce;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;


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
	
	//This is the face of the JobTracker exposed to the rest of the world via RMIRegsitry
	private JobTrackerServiceProvider jtServiceProvider;
	
	 public JobTracker()
	 {
		try 
		{
			//Register itself to the RMI Registry
			this.jtServiceProvider = new JobTrackerServiceProvider();
			Naming.rebind(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME), this.jtServiceProvider);
			
		} catch (RemoteException | MalformedURLException e) {
			System.err.println("Could not Register to the RMI Registry");
			e.printStackTrace();
		}
		 
	 }
	
	
	  public static void main(String[] args) 
	  {
		JobTracker jb = new JobTracker();
	  }

}
