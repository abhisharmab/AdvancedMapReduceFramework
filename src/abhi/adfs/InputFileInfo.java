package abhi.adfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class InputFileInfo  implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2521084379974491121L;
	private String fileName;
	private HashMap<String, List<String>> partitions;
	private HashMap<String, List<String>> deadNodes;
	private Integer paritionNumber;
	private boolean valid;
	
	
	public String fileExistInDataNode(String fileName){
		for(Entry<String,List<String>> entry : getPartitions().entrySet()){
			if(entry.getValue().contains(fileName)){
				return entry.getKey();
			}
		}
		return null;
	}
	
	public boolean isPartitionedInDataNode(String dataNode){
		return getPartitions().containsKey(dataNode);
	}
	
	public List<String> dataNodeDead(String dataNodeName){
		setValid(false);
		for(Entry<String,List<String>> entry : getPartitions().entrySet()){
			if(entry.getKey().equals(dataNodeName)){
				getDeadNodes().put(entry.getKey(),entry.getValue());
			}
		}
		
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
		for(Entry<String, List<String>> entry : getPartitions().entrySet()){
			System.out.println(entry.getKey());
			for(String name : entry.getValue()){
				System.out.println("----"+name);
			}
			
		}
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
