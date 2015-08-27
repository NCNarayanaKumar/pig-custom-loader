package com.company.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.company.hadoop.StartStopInputFormat.StartStopRecordReader;
/**
 * StartStopCustomLoader - This custom pig loader loads the content enclosed between the
 * start and end tags as a tuple.
 *  
 * @author ramisetty
 *
 */
public class StartStopCustomLoader extends LoadFunc {

	public static String START_TAG_KEY;
	public static String END_TAG_KEY;

	private StartStopRecordReader reader;

	private TupleFactory tupleFactory;

	/**
	 * Pig Loaders only take string parameters. The only
	 * interaction the user has with the Loader from the script.
	 * 
	 * @param indexesAsStrings
	 */
	public StartStopCustomLoader(String start, String end) {
		START_TAG_KEY = start;
		END_TAG_KEY = end;
		tupleFactory = TupleFactory.getInstance();
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new StartStopInputFormat(START_TAG_KEY, END_TAG_KEY);

	}

	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		try {
			boolean notDone = reader.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) reader.getCurrentValue();
			tuple = tupleFactory.newTuple(value.toString());
			
		} catch (InterruptedException e) {
			// add more information to the runtime exception condition.
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

		return tuple;

	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit pigSplit)
			throws IOException {
		this.reader = (StartStopRecordReader) reader; // note that for this Loader, we don't care about
								// the PigSplit.
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location); // the location is assumed
														// to be comma separated
														// paths.

	}

}