/**
 * 
 */
package abhi.mapreduce;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;


/**
 * @author abhisheksharma
 *
 * This is the actually just an Iterator class ON the Map Reduce Side. 
 * The application programmer will extend this class to over-ride some of the fucntions. 
 * 
 * As the part of this functions we actually read the file chunks
 *
 */
public abstract class InputFormat implements Iterator<KeyValueConstruct> {
	  
	  protected String filename;

	  protected long offset;

	  protected long fileSize;

	  protected RandomAccessFile raf;
	  

	  //Constructor for the INput Format
	  protected InputFormat(String filename) throws IOException {
	    this.filename = filename;
	    this.offset = 0;
	    this.raf = new RandomAccessFile(filename, "r");
	    this.fileSize = this.raf.length();
	    
	    this.raf.seek(offset);
	  }

	  protected boolean hasByte() throws IOException {
	    if (this.raf.getFilePointer() < (this.offset + this.fileSize))
	      return true;
	    return false;
	  }
	}
