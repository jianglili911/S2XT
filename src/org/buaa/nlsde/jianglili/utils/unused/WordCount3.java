package org.buaa.nlsde.jianglili.utils.unused;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


/**
 * Created by jianglili on 2016/1/30.
 */
public class WordCount3 {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one=new IntWritable(1);
        private Text  word=new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException{
            String line=value.toString();
            StringTokenizer tokenizer=new StringTokenizer(line);
            while (tokenizer.hasMoreElements()){

                word.set(tokenizer.nextToken());
                output.collect(word,one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key,Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter)throws IOException{
            int sum=0;
            while (values.hasNext()){
                sum+=values.next().get();
            }
            output.collect(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf(WordCount3.class);
        conf.setJobName("wordcount");


        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        String[] args2={"input","output"};
        args=args2;

        delete(conf,args[1]);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

    private static void delete(Configuration conf, String dirPath) throws IOException {
        FileSystem fs=FileSystem.get(conf);
        Path  targetPath=new Path(dirPath);
        if(fs.exists(targetPath)){
            boolean delResult=fs.delete(targetPath,true);
            if(delResult){
                System.out.println(targetPath + "has been  deleted successfully.");
            } else{
                System.out.println(targetPath + "deletion failed.");
            }
        }
    }

}
