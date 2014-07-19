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
 * This is used the break up the file in the parts depending on the configuration.
 */
public class FileSpliter {

	// This will be the limitation of the fileSize
	private Double fileSize;
	// This will be the partition count
	private Integer paritionSize;
	// File Name
	private String fileName;
	
	// This is used to read in the data
	private static Scanner scan;
	
	public FileSpliter(String fileName, Double file_size) throws FileNotFoundException{
		this.fileName = fileName;
		this.fileSize = file_size;
		paritionSize = 0;
		scan = new Scanner(new File(fileName));
	}
	
	// This method will return the next file block.
	// It will build up a string and return when the size of the string
	// is lower that the configured file size
	public String getNextBlock() throws IOException{
		if(scan.hasNextLine()){
			StringBuilder data = new StringBuilder();
			// Keep on building the data until it is end of the file
			// or it goes over the file size
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
	
	// This will close the scanner
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
