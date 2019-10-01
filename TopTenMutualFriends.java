import java.util.*;
import java.util.Map.Entry;

import javax.print.DocFlavor.STRING;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import MutualFriend.Map;
//import MutualFriend.Reduce;

public class TopTenMutualFriends {

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text keypair = new Text();
		
		public void map(LongWritable key, Text val, Context con)throws IOException, InterruptedException
		{
			// Get each record
			String[] record = val.toString().split("\t");
			if(record.length == 2)
			{
				String person = record[0];
				//Create an arraylist of friends
				// without new ArrayList, type mismatch
				// record[1] contains the friends separated by ','
				//	Arrays.asList returns a fixed size list backed by the specified arrays
				//List<String> friends = (Arrays.asList(record[1].split(",")));
				List<String> friends =(Arrays.asList(record[1].split(",")));
				//for each friend in friends
				//for each friend in friends
				for(String friend : friends)
				{
					//String pair_of_friends = "";
					if(Integer.parseInt(person) < Integer.parseInt(friend))
							{
								keypair.set(person + "," +friend);
							}
					else
					{
						keypair.set(friend + "," + person);
					}
					// Create an array list that does not have the current friend. 
					// Initially copy the friend list to the array list and then remove the current friend.
					//ArrayList<String> list_sans_curr_friend =new ArrayList<String>(friends);
					//removing
					//list_sans_curr_friend.remove(friend);
					//Convert this arraylist to a string  since the type for list is Text
					//String sans_curr_frnd = String.join(",", list_sans_curr_friend);
					//set pair of friends  as key
					//keypair.set(pair_of_friends);
					//Set the list value
					//listvalue.set(sans_curr_frnd);
					//Write it to the context
					con.write(keypair, new Text((record[1])));
				}
			}
		}
	}// First Map class
	
	//First Reducer class
	public static class FirstReducer extends Reducer<Text,Text,Text,IntWritable>
	{
		public void reduce(Text key, Iterable<Text> val, Context con)throws IOException, InterruptedException
		{
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			//Keep a 
			int mutualFriendCount = 0;
			//StringBuilder result = new StringBuilder();
			for(Text friends : val)
			{
				List<String> friendlist = Arrays.asList(friends.toString().split(","));
				for(String friend : friendlist)
				{
					if(map.containsKey(friend))
						mutualFriendCount += 1;
					else
						map.put(friend,1);
				}
			}
			//After appending all commas remove the last comma
			con.write(key, new IntWritable(mutualFriendCount));	
		}
	}//Reducer 1 class
	
	//Pass result through secnd Mapper class
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		private final static IntWritable valone= new IntWritable(1);

		//private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text val, Context con)throws IOException, InterruptedException
		{
			con.write(valone, val);
		}
	}
	
	// Second reducer
	
	public static class Secondreducer extends Reducer<IntWritable,Text, Text, IntWritable>
	{
		public void reduce(IntWritable key,Iterable<Text> val, Reducer<IntWritable, Text, Text, IntWritable>.Context con)throws IOException, InterruptedException
		{
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			int count = 1;
			for(Text record: val)
			{
				String[] part = record.toString().split("\t");
				if(part.length ==2)
				{
					map.put(part[0], Integer.parseInt(part[1]));
				}
			}//for
			
			// Using the Value Comparator object for sorting
			// Parametrized constructer is called sending the map
			ValueComparator bvc = new ValueComparator(map);
			// Using the treemap datastructure for ease in sorting
			TreeMap<String, Integer> mapsorted = new TreeMap<String, Integer>(bvc);
			mapsorted.putAll(map);
			// checking for the top ten mutual friends
			for(Entry<String, Integer> entry : mapsorted.entrySet())
			{
				if(count <= 10)
				{
					con.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
				}
				else
					break;
				// Count to check for 10
				count++;
			}
			
		}
	}
	// Value comparator class
	
	public static class ValueComparator implements Comparator<String>
	{
		
			HashMap<String, Integer> base;
			// Parametrized constructor
			public ValueComparator(HashMap<String, Integer> base)
			{
				this.base = base;
			}
			//compare if greater than the other 
			public int compare(String s1, String s2)
			{
				if(base.get(s1) >= base.get(s2))
					return -1;
				else
					return 1;
			}
	}
		
	// Main class set up configuration and arguments
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args 
		// Has to be 3 arguments
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopTenFriends <inputfile HDFS path> <outputfile1 HDFS path> <outputfile2 HDFS path>");
			System.exit(2);
		}
		// Annotation to supress the warnings
		@SuppressWarnings("deprecation")
		// Create the job
		Job job = new Job(conf, "TopTepFriends Phase 1");
		// Call the class
		job.setJarByClass(TopTenMutualFriends.class);
		// Call mapper class
		job.setMapperClass(Map1.class);
		// Call first reducer
		job.setReducerClass(FirstReducer.class);
		// Describe the output class for map key
		job.setMapOutputKeyClass(Text.class);
		// Describe the output class for map value
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output1 (this output file will contain count of the mutual friends between two friends)
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean mapreduce = job.waitForCompletion(true);
		// If first map reduce successful start second map reduce step. steps are similar to that of the first map reduce 
		if (mapreduce) {
			Configuration conf1 = new Configuration();
			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf1, "TopTenFriends Phase 2");
			job1.setJarByClass(TopTenMutualFriends.class);
			job1.setMapperClass(Map2.class);
			job1.setReducerClass(Secondreducer.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Text.class);
			
			job1.setOutputKeyClass(Text.class);
			
			job1.setOutputValueClass(IntWritable.class);
		
			//sending output of MapReduce phase1 as an input to MapReduce phase2 
			FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
			
			// set the HDFS path for the output2 (this output file will contain top ten count of the mutual friends between two friends)
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

			System.exit(job1.waitForCompletion(true) ? 0 : 1);

		}

	}
}
	
	

