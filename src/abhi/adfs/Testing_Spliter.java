package abhi.adfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Scanner;
import java.util.Map.Entry;

public class Testing_Spliter {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner scan = new Scanner(System.in);
		String filename = scan.nextLine();
		
		
		FileSpliter spliter;
		try {
			spliter = new FileSpliter(filename, (long) (1024*1024*0.01));
			String data = spliter.getNextBlock();
			Integer numbering = 0;
			while (data != null){
				
				numbering++;
				String num = numbering.toString(); 
				submit("testing_"+num, data);
				data = new String();
				data = spliter.getNextBlock();
				
				
				System.out.println("------------------------------------------------------");
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		

		
		
		
		
		
		
		
		
		
		
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
