package abhi.adfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author Douglas Rew
 * This is manage the file partition of the input file.
 * Addition this will be used to do the replication if a DataNode dies.
 */

public class InputFileInfo  implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2521084379974491121L;
	private String fileName;
	
	// Key will be the DataNodeName and the Values will be the fileNames
	private HashMap<String, List<String>> partitions;
	private HashMap<String, List<String>> deadNodes;
	
	// This will be the total partition number of the file
	private Integer paritionNumber;
	
	// This will be the boolean value to check easy whether the inputfile is valid
	private boolean valid;
	
	
	// This method is used to validate the distribution of the files.
	// It will use the partition number to check the files
	public boolean validateFiles(){
		
		// Building all file list for checking.
		List<String> allFileList = new ArrayList<String>();
		for(Entry<String,List<String>> entry : getPartitions().entrySet()){
			allFileList.addAll(entry.getValue());
		}
		
		boolean checking = true;
		for(int i = 1; i <+ paritionNumber; i ++){
			// This is how to file is being names in parts.
			String name = fileName+"_" +i;
			if( !allFileList.contains(name)){
				checking = false;
				break;
			}
		}
		
		// When validation pass we clean up the deadNodes
		if( checking){
			System.out.println("Validation Passed!");
			getDeadNodes().clear();
			valid = true;
		} else {
			System.out.println("Error while validatation.");
			System.out.println("Filename " + fileName + " cannot be used.");
			valid = false;
		}
		
		return valid;
		
		
		
		
	}
	
	// This method is to retrieve the DataNode which the file exist.
	public String fileExistInDataNode(String fileName){
		for(Entry<String,List<String>> entry : getPartitions().entrySet()){
			if(entry.getValue().contains(fileName)){
				return entry.getKey();
			}
		}
		return null;
	}
	
	// This is used to determine whether this InputFileInfo has been 
	// partitioned in the provided dataNode
	public boolean isPartitionedInDataNode(String dataNode){
		return getPartitions().containsKey(dataNode);
	}
	
	// This is used to return the file list from the detected dataNodes.
	public List<String> filesFromDeadDataNode(String dataNodeName){
		// We set the valid to false
		// Because if we call this method it means that there is a dead DataNode
		setValid(false);
		for(Entry<String,List<String>> entry : getPartitions().entrySet()){
			if(entry.getKey().equals(dataNodeName)){
				getDeadNodes().put(entry.getKey(),entry.getValue());
			}
		}
		
		// Clean up the partitions list  
		for(String key : getDeadNodes().keySet()){
			getPartitions().remove(key);
		}
			
		
		return getDeadNodes().get(dataNodeName);
	}
	
	public HashMap<String, List<String>> getPartitions() {
		if(partitions == null){
			partitions = new HashMap<String, List<String>>();
		}
		return partitions;
	}

	public void setPartitions(HashMap<String, List<String>> partitions) {
		this.partitions = partitions;
	}
	
	public InputFileInfo(String fileName){
		this.setFileName(fileName);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}


	public Integer getParitionNumber() {
		return paritionNumber;
	}

	public void setParitionNumber(Integer paritionNumber) {
		this.paritionNumber = paritionNumber;
	}
	
	// This method is used to add in partition file info with the DataNodeName 
	public void addFileParitionInfo(String dataNodeName, String partitionFileName){
	 
		if(getPartitions().containsKey(dataNodeName)){
			
			if(!getPartitions().get(dataNodeName).contains(partitionFileName)){
				getPartitions().get(dataNodeName).add(partitionFileName);
			}
		} else { 
			
			List<String> fileNames = new ArrayList<String>();
			fileNames.add(partitionFileName);
			getPartitions().put(dataNodeName, fileNames);
		} 
		
		
		// Debug
//		for(Entry<String, List<String>> entry : getPartitions().entrySet()){
//			System.out.println(entry.getKey());
//			for(String name : entry.getValue()){
//				System.out.println("----"+name);
//			}
//			
//		}
	}

	// This method is used in the Job tracker while submission of a job.
	// This is just a simple transpose of the Partition.
	public HashMap<String, List<String>> getTranspose(){
		HashMap<String, List<String>> transpose = new HashMap<String, List<String>>();
		for(Entry<String, List<String>> entry : getPartitions().entrySet()){
			for(String fileName : entry.getValue()){
				if(!transpose.containsKey(fileName)){
					List<String> dataNodeNameList = new ArrayList<String>();
					dataNodeNameList.add(entry.getKey());
					transpose.put(fileName, dataNodeNameList);
				} else {
					transpose.get(fileName).add(entry.getKey());
				}
			}
		}
		
		return transpose;
	}
	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}
	public HashMap<String, List<String>> getDeadNodes() {
		if(deadNodes == null){
			deadNodes = new HashMap<String, List<String>>();
		}
		return deadNodes;
		
	}
	public void setDeadNodes(HashMap<String, List<String>> deadNodes) {
		this.deadNodes = deadNodes;
	}
}
