/**
 * 
 */
package abhi.adfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import abhi.mapreduce.SystemConstants;

/**
 * @author Douglas Rew
 *
 */
public class NameNodeMasterImpl extends UnicastRemoteObject implements NameNodeMaster{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2917579042804203973L;
	private static String master_Name;
	
	
	private static String ipAddress = null;
	private static String portNumber = null;
	
	// This will keep track of the file information.
	private static List<InputFileInfo> list_fileInfo;
	private static ConcurrentHashMap <String, DataNode> list_dataNode;
	
	private static Integer rotationIndex;
	
	public NameNodeMasterImpl() throws RemoteException {
		super();
		list_dataNode = new ConcurrentHashMap <String, DataNode>();
		setList_fileInfo(new ArrayList<InputFileInfo>());
		rotationIndex = 1;
		// TODO Auto-generated constructor stub
	}
	
	public boolean registerToMaster(String slave_name) 
			throws RemoteException {

		registerDataNode(slave_name);
		return true;
		

	}
//
//	@Override
//	public boolean registerFileToMaster(String slave_name, String file_name)
//			throws RemoteException {
//		// TODO Auto-generated method stub
//		if(list_distributed_files.containsKey(slave_name)){
//			if(list_distributed_files.get(slave_name).contains(file_name)){
//				System.out.println("This file is already registered Filename : " + file_name + " in slave : " + slave_name);
//				// Might need to throw exception
//				return false;
//			} else {
//				list_distributed_files.get(slave_name).add(file_name);
//				return true;
//			}
//			
//		} else {
//			return false;	
//		}
//		
//	}
	
	public static void main(String args[])
    {


        try
        {
    		portNumber = SystemConstants.getConfig(SystemConstants.REGISTRY_PORT);
            ipAddress = SystemConstants.getConfig(SystemConstants.REGISTRY_HOST);
            
        	master_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SERVICE_NAME);
            System.out.println("Registering NameNodeMaster as : " + master_Name);
            NameNodeMasterImpl master = new NameNodeMasterImpl();
            //Registry registry = LocateRegistry.getRegistry(ipAddress, Integer.parseInt(portNumber));
            Naming.rebind(master_Name, master);
            System.out.println("NameNodeMaster: Ready...");
            
            
            // This is for periodical ping on to the dataNodes that are registered.
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (true) {
                        try {
                        	checkDataNodes();
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            });
        }
        catch (Exception e)
        {
            System.out.println("Server: Failed to register NameNodeMaster: " + e);
        }
    }

	@Override
	public void print() throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("This is me!!!!!");
	}

	void registerDataNode(String dataNodeName){
		
        try {
        	System.out.println("Looked up datanNode : " + dataNodeName);
			DataNode dataNode = (DataNode) Naming.lookup(dataNodeName);
			
			
			list_dataNode.put(dataNodeName, dataNode);
			
			System.out.println(list_dataNode);
			
			
			
			dataNode.print();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}

	@Override
	public void registerFileInformation(InputFileInfo fileInfo)
			throws RemoteException {
		
		
		if(list_fileInfo.isEmpty()){
			list_fileInfo.add(fileInfo);	
		} else {
			if(!checkFileExistance(fileInfo.getFileName())){
				list_fileInfo.add(fileInfo); 
			}
		}
		

		
		
		
		System.out.println("registerFileInformation    : " + fileInfo.getFileName());

		
		for(InputFileInfo info : list_fileInfo){
			System.out.println("-------"+info.getFileName());
		}
		
		
	}

	public static List<InputFileInfo> getList_fileInfo() {
		return list_fileInfo;
	}

	public static void setList_fileInfo(List<InputFileInfo> list_fileInfo) {
		NameNodeMasterImpl.list_fileInfo = list_fileInfo;
	}

	@Override
	public List<String> getDataNodes() throws RemoteException {
		
		System.out.println("getDataNodes()");
		System.out.println(list_dataNode.toString());
		List<String> dataNodes = new ArrayList<String>(list_dataNode.keySet());
		return dataNodes;
	}
	
	public static void replicateFiles(String dataNodeName){
		System.out.println("This Data node is dead  " + dataNodeName);
		System.out.println("Starting the replication process.");
		
		List<InputFileInfo> infoList = needValidations(dataNodeName);
		
		for(InputFileInfo info : infoList){
			System.out.println("Info   " +info.getFileName());
			for(String file : info.dataNodeDead(dataNodeName)){
				System.out.println("This is the missing file  " + file);
				String dataNode = info.fileExistInDataNode(file);
				System.out.println("this is where the existing file is. " + dataNode);
				Boolean found = Boolean.FALSE;
				Integer counter = 0;
				while(!found ){
					Entry<String, DataNode> entry = getNextDataNodeEntry();
					
					try {
						if(!entry.getValue().isExist(file)){
							found = Boolean.TRUE;
							// Got the data from an existing Node 
							DataNode node = list_dataNode.get(dataNode);
							
							
							System.out.println("I amm about to retrieve   " + file);
							System.out.println("from    " + dataNode);
							for(String fileaaa : node.getFileList()){
								System.out.println("-------"+fileaaa);
							}
							String data = node.retrieve(file);
							
							// Get the new Node and submit the file
							DataNode newNode = entry.getValue();
							newNode.submit(file, data);
							
							// update the info
							info.addFileParitionInfo(entry.getKey(), file);
						} else {
							// This is a limitation of the replication retry process.
							// If the counter is over the size of the dataNode
							// There is no need for replication. All files exist in the filesystem.
							counter++;
							if(counter >  list_dataNode.size()){
								found = Boolean.TRUE;
							}
						}
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		}
		
		
		
		
	}
	public static List<InputFileInfo> needValidations(String dataNode){
		List<InputFileInfo> list = new ArrayList<InputFileInfo>();
		for(InputFileInfo info : list_fileInfo){
			if(info.isPartitionedInDataNode(dataNode)){
				list.add(info);
			}
		}
		
		return list;
	}


	@Override
	public boolean checkFileExistance(String fileName) throws RemoteException {
		
//		System.out.println("checkFileExistance   " + fileName);
//		
//		for(InputFileInfo info : list_fileInfo){
//			System.out.println("Info name  " + info.getFileName());
//		}
		
		
		for(InputFileInfo info : list_fileInfo){
			System.out.println("Info name  " + info.getFileName());
			if(info.getFileName().equals(fileName)){
				return info.isValid();
			}
				
		}
		return false;
		
	}

	@Override
	public InputFileInfo getInputFileInfo(String fileName)
			throws RemoteException {
		for(InputFileInfo info : list_fileInfo){
			if( info.getFileName().equals(fileName)){
				return info;
			}
		}
		return null;
	}

	@Override
	public void removeInputFileInfo(String fileName)
			throws RemoteException {
		Iterator<InputFileInfo> iter = list_fileInfo.iterator();
		while(iter.hasNext()){
			InputFileInfo info = iter.next();
			if(info.getFileName().equals(fileName)){
				cleanUpDataNodes(info);
				iter.remove();
			}
		}
		
	}
	
	public void cleanUpDataNodes(InputFileInfo info){
		for(Entry<String, List<String>> entry : info.getPartitions().entrySet()){
			DataNode dn = list_dataNode.get(entry.getKey());
			try {
				if(dn.ping()){
					System.out.println("Cleaning up all partitioned files.");
					for(String filename : entry.getValue()){
						if(dn.remove(filename)){
							System.out.println("File : " + filename + " has been removed.");
							
						} else {
							System.out.println("Error has been occured while removing file : " + filename);
						}
						
					}
					
				} else {
					System.out.println("DataNode : " + entry.getKey() + " is dead.");
					list_dataNode.remove(entry.getKey());
					
					// Should add in code that will go through the inputfileinfo and clean up and
					// and save partitioned files
					
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public boolean ping() throws RemoteException {
		// TODO Auto-generated method stub
		return true;
	}
	
	public static Entry<String,DataNode> getNextDataNodeEntry(){
		rotationIndex++;
		Integer index = rotationIndex % list_dataNode.size(); 
		System.out.println("rotationIndex  " + rotationIndex);
		System.out.println("list_dataNode.size()  " + list_dataNode.size());
		System.out.println("index  " + index);
		List<String> keys = new ArrayList<String>(list_dataNode.keySet());
		Map.Entry<String, DataNode> entry = 
				new AbstractMap.SimpleEntry<String,DataNode>(keys.get(index), list_dataNode.get(keys.get(index)));
		return entry;
		
	}
	
	public static void checkDataNodes(){
		
		Iterator<Entry<String, DataNode>> iter = list_dataNode.entrySet().iterator();
		while(iter.hasNext()){
			@SuppressWarnings("unchecked")
			Entry<String, DataNode> dn = (Entry<String, DataNode>) iter.next();
		
			try{
				if(dn.getValue().ping()){
					// Debug 
					System.out.println(dn.getKey() + " is alive");
					continue;
				} 
			} catch(RemoteException e){
				System.out.println("There are dead dataNode this should be addressed");
				String deadNode = dn.getKey();
				iter.remove();
				replicateFiles(deadNode);
			}
			
			
		}
				
	}
}
