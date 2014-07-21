/**
 * 
 */
package abhi.anagram;

import java.io.IOException;
import java.util.Arrays;

import abhi.mapreduce.Mapper;
import abhi.mapreduce.OutputCollector;

/**
 * @author abhisheksharma
 *
 */
//Reference https://code.google.com/p/hadoop-map-reduce-examples/source/browse/trunk/hadoop-examples/src/com/hadoop/examples/anagrams/AnagramMapper.java
public class AnaMapper extends Mapper<String, String, String, String> {


		private static final long serialVersionUID = 1L;

		public void map(String key, String value, OutputCollector<String, String> outputCollector)
				throws IOException, InterruptedException {
			
			String originalWord = value.toString(); //Converted the Data Into String
			char[] wordChars = originalWord.toCharArray(); //User is saying that the file is space de-limited
			
			Arrays.sort(wordChars);
            String sortedWord = new String(wordChars);

            outputCollector.collect(sortedWord, originalWord);


		}
}
