/*
To run the hadoop program use
1) javac -cp /usr/local/hadoopUser/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.0.jar:/usr/local/hadoopUser/hadoop-2.9.0/share/hadoop/common/hadoop-common-2.9.0.jar:/usr/local/hadoopUser/hadoop-2.9.0/share/hadoop/common/lib/commons-cli-1.2.jar -d /Users/bhawesh/Desktop/divya/assign_db/classfolder /Users/bhawesh/Desktop/divya/assign_db/assign1.java
2) jar -cvf /Users/bhawesh/Desktop/divya/assign_db/assign1.jar -C /Users/bhawesh/Desktop/divya/assign_db/classfolder .
3) bin/hadoop jar /Users/bhawesh/Desktop/divya/assign_db/assign1.jar assign1 /user/divya/assign1inp/in.txt /user/divya/output
*/

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class assign1
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split(" ");
            String tid=str[0];
            context.write(new Text(tid), new Text(value));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
   {
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
        int total_cost=0;
        int no_items=0;
        String tid =  key.toString();
         for (Text val : values)
         {
            String [] str = val.toString().split(" ", -3);
            no_items= str.length-1;
            for(int i=1;i<str.length;i++)
              total_cost = total_cost + Integer.parseInt(str[i]);
         }
        String result = tid+">"+String.valueOf(total_cost)+","+String.valueOf(no_items);
         if(no_items>=1 & no_items<=10)
          {
            tid="bin1";
          }
          else if(no_items>=11 & no_items<=20)
          {
            tid="bin2";
          }
          else if(no_items>=21 & no_items<=30)
          {
            tid="bin3";
          }
          else if(no_items>=31 & no_items<=40)
          {
            tid="bin4";
          }
          else if(no_items>=41 & no_items<=50)
          {
            tid="bin5";
          }
         context.write(new Text(tid), new Text(result));
      }
   }
   
   
   public static void main(String[] args) throws Exception
    {    
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "word count");
      job.setJarByClass(assign1.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(MapClass.class);
      job.setNumReduceTasks(5);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
      job.setReducerClass(ReduceClass.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
   }
}