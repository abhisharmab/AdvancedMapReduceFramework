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
import java.util.ArrayList;
import java.util.Collections;


/**
 * @author abhisheksharma
 *
 */
public class MapperFieldAgent extends FieldAgent {

	private long offset;

	private int fileSize;

	private int reducerNum;

	private MapperOutputCollector outputCollector;

	private Mapper mapper;
	
	//How to parse the Key-Value Pair
	private InputFormat inputFormat;

	public MapperFieldAgent(int taskID, String infile, String outfile, String mapper, 
			String partitioner, String inputFormat, int numReducer)
	{

		super(taskID, infile, outfile, SystemConstants.TaskType.MAPPER);

		//Create the directory for the Job if it doesn't exist
		File dir = new File(outfile);
		
		if(!dir.exists())
		{
			dir.mkdir();
		} 
		
		this.offset = 0;
		this.reducerNum = numReducer;
		try {

			this.mapper = (Mapper) Class.forName(mapper).newInstance();

			Partitioner part = (Partitioner) Class.forName(partitioner).newInstance();

			
			System.out.println("--------------" + inputFormat);
			System.out.println("===============" + outputFile);
			System.out.println("--------------" + inputFile);
			
			String strippedName = this.inputFile.substring(SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY).length() + 1);
			
			this.outputCollector = new MapperOutputCollector(this.outputFile, strippedName, part, reducerNum, "\t");

			this.inputFormat = (InputFormat) Class.forName(inputFormat)
					.getConstructor(String.class)
					.newInstance(this.inputFile);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		System.out.println("contructor done");
	}

	@Override
	public void run() {

		try
		{
			pushStatusToTaskTracker();

			//called once
			mapper.setup();

			try {
				while (this.inputFormat.hasNext()) {
					KeyValueConstruct record = this.inputFormat.next();
					mapper.map(record.key, record.value, this.outputCollector);
				}
				/* close the files */
				this.outputCollector.closeAll();
			}
			catch (RuntimeException e) {
				e.printStackTrace();
				System.exit(0);
			}

			//clean-up at end
			mapper.cleanUp();
			
			this.sort();
			
			//Shows task is Done
			this.updateStatusSucceeded();
			
			Thread.sleep(3000);
			
			
		}
		catch(IOException | InterruptedException e)
		{
			System.out.println("Error Occured");

			e.printStackTrace();

		}

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("I am ending!!!!");
		System.exit(0);
	}

	//Reference: http://stackoverflow.com/questions/11647889/sorting-the-mapkey-value-in-descending-order-based-on-the-value
	  private void sort() {
		    /* for each partition */
		    for (int i = 0; i < this.reducerNum; i++) {
		      ArrayList<KeyValueConstruct> list = new ArrayList<KeyValueConstruct>();
		      String filename = outputCollector.outputDirectory + System.getProperty("file.separator")
		              + outputCollector.outputFileNamePrefix + "_part_"+ i;
		      
		      
		      File file = new File(filename);
		      /* read file for each partition, wrap to records, store to list */
		      BufferedReader br = null;
		      try {
		        br = new BufferedReader(new FileReader(file));
		        String line;
		        while ((line = br.readLine()) != null) {
		          int ind = line.indexOf('\t');
		          String key = line.substring(0, ind);
		          String value = line.substring(ind + 1);
		          KeyValueConstruct record = new KeyValueConstruct(key, value);
		          list.add(record);
		        }
		      } catch (FileNotFoundException e) {
		        e.printStackTrace();
		      } catch (IOException e) {
		        e.printStackTrace();
		      } finally {
		        if (br != null)
		          try {
		            br.close();
		          } catch (IOException e) {
		            e.printStackTrace();
		            System.exit(0);     
		          }
		      }
		      /* delete the file */
		      file.delete();
		      
		      /* sort the records */
		      Collections.sort(list);
		      /* write the sorted records to file */
		      BufferedWriter bw = null;
		      try {
		        bw = new BufferedWriter(new FileWriter(file, true));
		        for (KeyValueConstruct r : list) {
		          bw.write(r.key + outputCollector.separator + r.value);
		          bw.newLine();
		        }
		      } catch (IOException e) {
		        e.printStackTrace();
		      } finally {
		        if (bw != null)
		          try {
		            bw.close();
		            
		            String name = filename.substring(SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY).length() + 1);
		            getCreatedFiles().add(name);
		            this.nameNodeSlaveReference.registerToLocalDataNode(name);
		          } catch (IOException e) {
		            e.printStackTrace();
		            System.exit(0);     
		          }
		      }
		    }
		  }

	@Override
	protected float getPercentage() {
		try {
			
			return (float) (this.inputFormat.raf.getFilePointer() - this.inputFormat.offset) / this.inputFormat.fileSize;
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return 0;
	}

	public static void main(String[] args) {
		if (args.length !=7) {
			System.out.println("Illegal arguments");
		}
		int taskID = Integer.parseInt(args[0]);
		PrintStream out = null;
		try {
			 out = new PrintStream(new FileOutputStream(new File(SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY)+ "mapper_tasklog"+ taskID)));
			System.setErr(out);
			System.setOut(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		}
		String inputFile = args[1];
		String outputFile = args[2];
		String mapper = args[3];
		String partitioner = args[4];
		String inputFormat = args[5];
		int reducerNum = Integer.parseInt(args[6]);

		MapperFieldAgent mFA = new MapperFieldAgent(taskID, inputFile, outputFile, mapper, partitioner, inputFormat, reducerNum);
		mFA.run();
	}

}
