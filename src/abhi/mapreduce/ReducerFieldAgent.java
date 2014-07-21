/**
 * 
 */
package abhi.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Timer;

import javax.swing.border.EtchedBorder;

import abhi.adfs.NameNodeMaster;
import abhi.adfs.NameNodeMasterImpl;
import abhi.adfs.NameNodeSlave;
import abhi.mapreduce.SystemConstants.MapJobsStatus;

/**
 * @author abhisheksharma, dkrew
 *
 */
public class ReducerFieldAgent extends FieldAgent{

	private ReducerOutputCollector outputCollector;

	private Reducer reducer;
	
	private OutputFormat outputFormat;

	private IJobTrackerServices jobTrackerServiceProvider;


	private int partitionedNumber;
	
	// This is for reporting the progress
	private double totalEntryNumber;
	private double processedEntryNumber;
	
	
	public ReducerFieldAgent(int taskID, String outfile, String reducer, 
			String outputFormat, int partitionedNumber)
	{


		super(taskID, null, outfile, SystemConstants.TaskType.REDUCER);
		this.partitionedNumber = partitionedNumber;
		try {

			this.reducer = (Reducer) Class.forName(reducer).newInstance();

			
			System.out.println("--------------"+outputFormat);
			System.out.println("--------------"+outputFile);
			this.outputFormat = (OutputFormat) Class.forName(outputFormat)
					.newInstance();
			
			this.outputCollector = 
					new ReducerOutputCollector(this.outputFile, "\t", this.outputFormat, partitionedNumber);
		} catch (Exception e) {
			e.printStackTrace();
			/* exception happens, shut down jvm */
			System.exit(0);
		}
		
		
		totalEntryNumber = 0;
		processedEntryNumber = 0;
		
		//This will later be updated. If it is not updated. We cannot use it.
		System.out.println("contructor done");
	}

	private void sendResultToJobClient(){

		
       try
        {
    	   
	   		String slaveName = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
	   		String hostName = getJobTrackerServiceProvider().getJobOriginHostNamebyTaskID(taskID);
	   		// Building the lookup Name
	   		String lookup_name = "rmi://" +hostName + ":"+ 1099+ "/"+slaveName+"_"+hostName;
	   		System.out.println("Building a look up make for the Slave  : " + lookup_name);
   		
   		
    	   // This is the origin nameNodeSlave
    		NameNodeSlave originSlave = (NameNodeSlave) Naming.lookup(lookup_name);
    		System.out.println("NameNodeSlave has been looked up.");
    		
    		for(String filename : getCreatedFiles()){
    			// This is the local Reference
    			String data = this.nameNodeSlaveReference.retrieveFromLocalDataNode(filename);
    			
    			// Sending the results to the origin
    			originSlave.saveFileToLocalDataNode(filename, data);
    		}
    	} catch (Exception e){
    		System.out.println("Manager: Exception thrown looking up " + "NameNodeSlave");
    	}
		
	}

	private HashMap<String, List<String>> packageData(List<String> data){
		
		HashMap<String, List<String>> packageData = new HashMap<String, List<String>>();
		
		
		for(String fileData : data){
			
			System.out.println();
			Scanner scan = new Scanner(fileData);
			scan.useDelimiter("\\t|\\n");
			
			// The data is constructed by "key \t value \n"
			while(scan.hasNext()){
				String key = scan.next(); 
				String value = scan.next();
				
				if(packageData.containsKey(key)){
					packageData.get(key).add(value);
				} else {
					List<String> values = new ArrayList<String>();
					values.add(value);
					packageData.put(key, values);
				}
				
				
			}
		}
		
		return packageData;
	}
	
	// We are gathering all the data here for the reducer
	private List<String> shuffleData(){
		List<TaskProgress> mapTaskList;
		try {
			// Get the map task list whitch are completed.
			mapTaskList = getJobTrackerServiceProvider().getCompletedMapTasks(taskID);
			
			if( mapTaskList == null){
				System.out.println("Failed to grab the completed Map Tasks.");
				return null;
			} else {
				System.out.println("Retrieved the Completed Map Tasks.");
				
			}
		
			// Using the TaskTracker to get the Slave and building a list for retrieval 
			HashMap<NameNodeSlave, List<String>> slavesWithFiles = new HashMap<NameNodeSlave, List<String>>();
			for(TaskProgress task : mapTaskList){
				NameNodeSlave slave = lookupNameNodeSlave(task.getTaskTrackerName());
				List<String> fileNames = task.getCreatedFileNames();
				slavesWithFiles.put(slave, fileNames);
				
				for(String name : fileNames){
					System.out.println("Slave   " + slave.toString());
					System.out.println("file =====  " + name);
				}
				
			}
			
			List<String> data = new ArrayList<String>();
			//Look into all slaves and get the correct partitioned results of Mapper
			for(Entry<NameNodeSlave, List<String>> entry : slavesWithFiles.entrySet()){
				//Check for alive on the slave
				if(entry.getKey().ping()){
					String fileName = getFileNameWithParitionNumber(entry.getValue(), partitionedNumber);
					if(fileName != null){
						NameNodeSlave slaveNode= entry.getKey();
						// This is where we retrieve the data from different nodes.
						String fileData = slaveNode.retrieveFromLocalDataNode(fileName);
						data.add(fileData);
						
					} else {
						System.out.println("Partitioned File does not exist");
					}
				} else {
					System.out.println("Error in getting data from the Slave");
				}
				
				
			}
			
			return data;
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error in accessing the Job Tracker.");
		}
		
		return null;

	}
	
	// Within the FileList we are getting the correct file part that is assigned to the reducer
	private String getFileNameWithParitionNumber(List<String> fileNames, int partitionedNumber){
		String indicator = "part_"+ partitionedNumber;
		
		System.out.println("indicator   " + indicator);
		for(String name : fileNames){
			System.out.println("FFFFILE NAME " + name);
			if(name.contains(indicator)){
				return name;
			}
		}
		
		return null;
		
	}
	
	// We are looking up the name of the NameNodeSlave using the taskTrackerName
	private NameNodeSlave lookupNameNodeSlave(String taskTrackerName){
		
		String slaveName = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
		
		// Parsing out the location information
		int end_index = taskTrackerName.length();
		int start_index = taskTrackerName.indexOf("_")+1;
		String location = taskTrackerName.substring(start_index, end_index);
		
		// Building the lookup Name
		String lookup_name = "rmi://" +location + ":"+ 1099+ "/"+slaveName+"_"+location;
		System.out.println("Building a look up make for the Slave  : " + lookup_name);
		
       try
        {
    		NameNodeSlave nameNodeSlave = (NameNodeSlave) Naming.lookup(lookup_name);
    		System.out.println("NameNodeSlave has been looked up.");
    		return nameNodeSlave;
    	} catch (Exception e){
    		System.out.println("Manager: Exception thrown looking up " + "NameNodeSlave");
    		return null;
    		
    	}
		
		
		
	}
	public static void main(String[] args) {
		
		if (args.length !=5) {
			System.out.println("Illegal arguments");
		}
		int taskID = Integer.parseInt(args[0]);
		try {
			PrintStream out = new PrintStream(new FileOutputStream(new File(SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY)+ "reducer_tasklog"+ taskID)));
			System.setErr(out);
			System.setOut(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		}
		String outputFile = args[1];
		String reducer = args[2];
		String outputFormat = args[3];
		int partitionedNumber = Integer.parseInt(args[4]);
		
		
	
		
		ReducerFieldAgent fieldAgent= new ReducerFieldAgent(taskID, outputFile,reducer, outputFormat, partitionedNumber);
		fieldAgent.run();
	}

	@Override
	protected float getPercentage() {
		// TODO Auto-generated method stub
		if(totalEntryNumber == 0){
			return 0;
		}
		return (float) (processedEntryNumber/totalEntryNumber*100);
	}
	
	// Looking up the job Tracker
	private boolean lookUpJobTracker(){
		//Get the JobTrackerServiceProvider Reference
		int registryPort = Integer.parseInt(SystemConstants.getConfig(SystemConstants.JOBTRACKER_REGISTRY_PORT));
		Registry rmiRegistry;
		try {
			rmiRegistry = LocateRegistry.getRegistry(SystemConstants.getConfig(SystemConstants.JOBTRACKER_REGISTRY_HOST),registryPort);
			this.setJobTrackerServiceProvider((IJobTrackerServices) rmiRegistry.lookup(SystemConstants.getConfig(SystemConstants.JOBTRACKER_SERVICE_NAME)));
			return true;
		} catch (RemoteException | NotBoundException e) {
			System.err.println("Could bind to the JobTracker Registry Error Occured");
			return false;
		}
	}

	public IJobTrackerServices getJobTrackerServiceProvider() {
		return jobTrackerServiceProvider;
	}

	public void setJobTrackerServiceProvider(IJobTrackerServices jobTrackerServiceProvider) {
		this.jobTrackerServiceProvider = jobTrackerServiceProvider;
	}

	@Override
	public void run() {

			

			try
			{
				
				pushStatusToTaskTracker();

				//called once
				reducer.setup();
				
				
				// Shuffle
				int count = 0;
				if(lookUpJobTracker()){
					MapJobsStatus status;
					status = getJobTrackerServiceProvider().reportMapStatus(taskID);
					while(!status.equals(MapJobsStatus.SUCCEEDED)){
						status = getJobTrackerServiceProvider().reportMapStatus(taskID);
						Thread.sleep(1000);
					}	
				} else {
					System.out.println("ERROR : Cannot look up the Job tracker.");
					this.outputCollector.close();
					System.exit(0);
				}
				
				
				System.out.println("All map task have been completed.");
				System.out.println("Starting the Shuffle process");
				List<String> data = shuffleData();
				if(data == null){
					System.out.println("Error has been occured while accessing the Data from the Mappers.");
					// let the job tracker know??????
					this.outputCollector.close();
					System.exit(0);
				} else {
					System.out.println("Got all data from the Mapper.");
				}
				
				
				
				// Packaging the Data
				System.out.println("Combining the data.");
				HashMap<String, List<String>> packagedData = packageData(data);
				if(packagedData.isEmpty()){
					System.out.println("There are errors in packaging the Data.");
					this.outputCollector.close();
					System.exit(0);
				} else {
					System.out.println("Data packaged.");
					totalEntryNumber = packagedData.size();
				}

				
				
				// Reduce
				try {
					Iterator iter = packagedData.entrySet().iterator();
					while (iter.hasNext()) {

						Entry<String, List<String>> entry = (Entry<String, List<String>>) iter.next();
						reducer.reduce(entry.getKey(), entry.getValue().iterator(), this.outputCollector);
						
						// This is for tracking the progress
						processedEntryNumber++;
					}
					/* close the files */
					this.outputCollector.close();
					
					
					String name = this.outputCollector.getOutputFileName().substring(SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY).length() + 1);
					this.nameNodeSlaveReference.registerToLocalDataNode(name);
					getCreatedFiles().add(name);
				}/* if runtime exception happens in user's code, exit jvm */
				catch (RuntimeException e) {
					e.printStackTrace();
					System.exit(0);
				}

				// Send the file to the Job Client location
				sendResultToJobClient();
				
				//clean-up at end
				reducer.cleanUp();
				
				//Shows task is Done
				this.updateStatusSucceeded();
				
				Thread.sleep(4000);
			}
			catch(IOException | InterruptedException e)
			{
				System.exit(0);
			}

			
			
			System.exit(0);
		
	}


}
