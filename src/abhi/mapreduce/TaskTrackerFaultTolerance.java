/**
 * 
 */
package abhi.mapreduce;

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
		// TODO Auto-generated method stub

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
