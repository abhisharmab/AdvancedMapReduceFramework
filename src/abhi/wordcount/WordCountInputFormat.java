/**
 * 
 */
package abhi.wordcount;

import java.io.IOException;

import abhi.mapreduce.InputFormat;
import abhi.mapreduce.KeyValueConstruct;

/**
 * @author abhisheksharma
 *
 */
public class WordCountInputFormat extends InputFormat
{

<<<<<<< HEAD
	protected WordCountInputFormat(String filename) throws IOException {
=======

	public WordCountInputFormat(String filename) throws IOException {
>>>>>>> ebf1c897050254bf5119095ac6686b377e6e309b
		super(filename);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean hasNext() {
		try {
			return this.hasByte();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public KeyValueConstruct next() {

		try
		{
			String line = this.raf.readLine();
			String key = Integer.toString(line.length());
			String value = line;

			return new KeyValueConstruct(key, value);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void remove() {

	}
}
