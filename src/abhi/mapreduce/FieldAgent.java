/**
 * 
 */
package abhi.mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import abhi.adfs.NameNodeSlave;

/**
 * @author abhisheksharma
 *
 * This is the base class for this Main Worker. This is the guy that will be doing the heavy lifting and actually going to do some wokrk. 
 * 
 * 
 *
 */
public abstract class FieldAgent {

	
	  protected int taskID; //Task ID

	  protected String outputFile;

	  protected String inputFile;
	  
	  protected TaskProgress taskProgress;
	  
	  protected ITaskTrackerServices taskServiceProviderReference; //Task Service Provider Reference
	  
	  protected NameNodeSlave nameNodeSlaveReference;

	  protected List<String> createdFiles; // List of created Files 
	  

	public List<String> getCreatedFiles() {
		if(createdFiles == null){
			createdFiles = new ArrayList<String>();
		}
		return createdFiles;
	}

	public void setCreatedFiles(List<String> createdFiles) {
		this.createdFiles = createdFiles;
	}

	public FieldAgent(int taskID, String infile, String outfile,
	          SystemConstants.TaskType type) {

	    this.taskID = taskID;
	    this.outputFile = outfile;
	    this.inputFile = infile;
	    this.taskProgress = new TaskProgress(this.taskID, type);

	    String registryHostName = null;
	    try {
	      registryHostName = InetAddress.getLocalHost().getHostAddress();
	    } catch (UnknownHostException e1) {
	      e1.printStackTrace();
	    }

	    try {
	      Registry reg = LocateRegistry.getRegistry(registryHostName, 1099);
	      taskServiceProviderReference = (ITaskTrackerServices) reg.lookup("TaskTracker_"+ registryHostName);
	      nameNodeSlaveReference = (NameNodeSlave) reg.lookup(SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE) + "_" + registryHostName);
	    } catch (RemoteException e) {
	      e.printStackTrace();
	    } catch (NotBoundException e) {
	      e.printStackTrace();
	    }

	  }

	  /**
	   * abstract method used to run task
	   */
	  public abstract void run();


	  protected abstract float getPercentage();
	  
	  public void pushStatusToTaskTracker() {
	    /* periodically send status progress to task tracker */
		  
	    //ScheduledExecutorService schExec = Executors.newScheduledThreadPool(1);
		  
	    Thread thread = new Thread(new Runnable() {
	      public void run() {
	    	  while(true)
	    	  {
	    		  updateStatus();
	    		  try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	  }
	      }
	    });
	    thread.start();
	    
	    //thread.setDaemon(true);
	    //schExec.scheduleAtFixedRate(thread, 0, 2, TimeUnit.SECONDS);
	  }

	  
	  public void updateStatus() {
	    synchronized (taskProgress) {
	      
	    	/* if already succeed, stop. if not, send in-progress status */
	      if (taskProgress.getStatus() != SystemConstants.TaskStatus.SUCCEEDED) {
	        try {

	        	taskProgress.setPercentageCompleted(this.getPercentage());

	        	taskProgress.setStatus(SystemConstants.TaskStatus.INPROGRESS);

	        	taskProgress.setLatestUpdateTimeStamp(System.currentTimeMillis());

	          //Update Task Tracker
	          taskServiceProviderReference.updateFieldAgentStatus(taskProgress);

	        } catch (RemoteException e) {
	          e.printStackTrace();
	        }
	      }
	    }
	  }


	  public void updateStatusSucceeded() {
	    /* lock progress before change it */
		  synchronized (taskProgress) {
	      try {
	        	taskProgress.setPercentageCompleted(this.getPercentage());

	        	taskProgress.setStatus(SystemConstants.TaskStatus.SUCCEEDED);
	        	taskProgress.setCreatedFileNames(getCreatedFiles());

	        /* set the current time stamp */
	    	  taskProgress.setLatestUpdateTimeStamp(System.currentTimeMillis());

	    	  //Update Task Tracker
	          taskServiceProviderReference.updateFieldAgentStatus(taskProgress);
	      } catch (RemoteException e) {
	        e.printStackTrace();
	      }
	    }
	  }


	}
