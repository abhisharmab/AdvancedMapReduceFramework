package abhi.adfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Scanner;
import java.util.Map.Entry;

import abhi.mapreduce.SystemConstants;

public class Testing_Spliter {

	public static void main(String[] args) {
		
		 Scanner s = new Scanner("hello\t world \n hello\t world");
		 s.useDelimiter("\\t|\\n");
		 while(s.hasNext()){
		          System.out.println(s.next().trim());

		 }
		 
		 
//		String temp;
//		try {
//			temp = InetAddress.getLocalHost().getHostName();
//			System.out.println(temp);
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	
		
		
		
//		
//		File file = new File("acts.jar");
//		byte buffer[] = new byte[(int)file.length()];
//		try {
//		     BufferedInputStream input = new
//		       BufferedInputStream(new FileInputStream("acts.jar"));
//		     input.read(buffer,0,buffer.length);
//		     input.close();
//		     
//		   //  System.out.println(new String(buffer));
//		} catch (Exception e){
//			System.out.println("wfwf");
//		}
//		
//		
//		File file2 = new File("acts2.jar");
//		BufferedOutputStream output;
//		try {
//			output = new
//				 BufferedOutputStream(new FileOutputStream(file2.getName()));
//	        output.write(buffer,0,buffer.length);
//	        output.flush();
//	        output.close();
//		} catch (FileNotFoundException e2) {
//			// TODO Auto-generated catch block
//			e2.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//
//		try {
//			System.out.println(InetAddress.getLocalHost().getHostAddress());
//		} catch (UnknownHostException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		
//		// TODO Auto-generated method stub
//		Scanner scan = new Scanner(System.in);
//		String filename = scan.nextLine();
//		
//		 
//		FileSpliter spliter;
//		try {
//			spliter = new FileSpliter(filename, (double) (1024*1024*0.01));
//			String data = spliter.getNextBlock();
//			Integer numbering = 0;
//			while (data != null){
//				
//				numbering++;
//				String num = numbering.toString(); 
//				submit("testing_"+num, data);
//				data = new String();
//				data = spliter.getNextBlock();
//				
//				
//				System.out.println("------------------------------------------------------");
//			}
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		
		
		
		

		
		
		
		
		
		
		
		
		
		
	}

	
	
	public static boolean submit(String filename, String data) throws RemoteException {
		// TODO Auto-generated method stub
		
		BufferedWriter writer = null;
		File file = new File(filename);
		
		
		try {
			if( file.createNewFile()){
				System.out.println("creating new");
				System.out.println(data);
				
				writer = new BufferedWriter(new FileWriter(file));
				writer.write(data);
				System.out.println("File " + filename + " has been created.");
				writer.close();
			} else {
				System.out.println("There are duplicate file names");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e.toString());
			return false;
		} finally {
		
			try {
				if(writer !=null ){
					writer.close();
					return true;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println(e.toString());
				return false;
			}
			
		}
		return false;
	}
}
