package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

public class HighestRatedMovieCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration c= new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        if(files.length<2){
            System.err.println("Usage: Movie <userIdMovieName path>   <output path>");
            System.exit(-1);
        }
        Path input = new Path(files[0]);
        Path outputJob1 = new Path(files[0]+"-tmp");
        Path output = new Path(files[1]);
        Configuration configuration=new Configuration();
        Job job1= Job.getInstance(configuration, "MovieCountbyName");
        job1.setJarByClass(HighestRatedMovieCount.class);
        job1.setMapperClass(MapForMovieCountByUserId.class);
        job1.setReducerClass(ReducerForCountByMovie.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, outputJob1);
        if(!job1.waitForCompletion(true)){
            System.exit(1);
        }
        Configuration c4=new Configuration();
        Job job2= Job.getInstance(c4, "MovieCountGroupFinal");
        job2.setJarByClass(HighestRatedMovieCount.class);
        job2.setMapperClass(MapToCountMovieName.class);
        job2.setReducerClass(ReduceForCountMovieName.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, outputJob1);
        FileOutputFormat.setOutputPath(job2, output);
        boolean resultjob = job2.waitForCompletion(true);
        System.out.println("Status de fin du job de grouping de films par nombre d'utilisateurs : " + resultjob);
        System.exit(resultjob ? 0 : 1);
    }
    public static class MapForMovieCountByUserId extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnsForLine = line.split("\t");
            String movieName= columnsForLine[1];
            con.write(new Text(movieName), new IntWritable(1));

        }
    }
    public static class ReducerForCountByMovie extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text movieName, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

            int sum=0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(movieName, new IntWritable(sum));
        }
    }
    public static class MapToCountMovieName extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnsForLine = line.split("\t");
            Text movieName = new Text(columnsForLine[0]);
            int  movieCount =Integer.parseInt( columnsForLine[1]);
            IntWritable outputKey = new IntWritable(movieCount);
            con.write(outputKey, movieName);
        }
    }
    public static class ReduceForCountMovieName extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable movieCount, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            StringBuilder movieNames = new StringBuilder();
            for (Text value : values) {
                movieNames.append(value.toString()).append(" ");

            }
            con.write(movieCount, new Text(movieNames.toString()));
        }
    }
}
