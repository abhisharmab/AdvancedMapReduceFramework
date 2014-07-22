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
 * Extending the InputFormat of the MR framework to provide the Iterator
 *
 */
public class WordCountInputFormat extends InputFormat
{
	public WordCountInputFormat(String filename) throws IOException {
		super(filename);
	}

	@Override
	public boolean hasNext() {
		try {
			return this.hasByte();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override //Function to construct the KeyValue
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
