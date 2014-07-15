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

	private Long fileSize;
	private Integer paritionSize;
	private String fileName;
	private Scanner scan;
	
	public FileSpliter(String fileName, Long fileSize) throws FileNotFoundException{
		this.fileName = fileName;
		this.fileSize = fileSize;
		scan = new Scanner(new File(fileName));
	}
	
	public String getNextBlock() throws IOException{
		if(scan.hasNextLine()){
			StringBuilder data = new StringBuilder();
			while (data.length() < fileSize.intValue() && scan.hasNextLine()){
				String temp = scan.nextLine();
				data.append("\n"+temp);
			}
			
			setParitionSize(getParitionSize() + 1);
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