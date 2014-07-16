/**
 * 
 */
package abhi.adfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Scanner;

/**
 * @author Douglas Rew
 *
 */
public class FileSpliter {

	private Double fileSize;
	private Integer paritionSize;
	private String fileName;
	private static Scanner scan;
	
	public FileSpliter(String fileName, Double file_size) throws FileNotFoundException{
		this.fileName = fileName;
		this.fileSize = file_size;
		paritionSize = 0;
		scan = new Scanner(new File(fileName));
	}
	
	public String getNextBlock() throws IOException{
		if(scan.hasNextLine()){
			StringBuilder data = new StringBuilder();
			while (data.length() < fileSize.intValue() && scan.hasNextLine()){
				String temp = scan.nextLine();
				data.append("\n"+temp);
			}
			
			paritionSize++; 
			return data.toString();
		} else {
			return null;
		}
	}
	
	public void close() throws IOException{
		scan.close();
	}

	public Integer getParitionSize() {
		return paritionSize;
	}

	public void setParitionSize(Integer paritionSize) {
		this.paritionSize = paritionSize;
	}
	
	
}
