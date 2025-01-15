package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.logging.Logger;

public class HighestRatedMovieNamePerUserId {
    static Logger log = Logger.getLogger(HighestRatedMovieNamePerUserId.class.getName());
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if(files.length<3){
            System.err.println("Usage: Movie <ratings path> <movie path>  <output path>");
            System.exit(-1);
        }
        Path rating = new Path(files[0]);
        Path movie = new Path(files[1]);
        Path tempOutput = new Path(files[2]+"-tmp");
        Path outputJob = new Path(files[2]);

        Job job1 = Job.getInstance(configuration, "highestRatedMovieIdPerUser");
        job1.setJarByClass(HighestRatedMovieNamePerUserId.class);
        job1.setMapperClass(MapForMovieHighestRating.class);
        job1.setReducerClass(ReducerForHighestRating.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, rating);
        FileOutputFormat.setOutputPath(job1, tempOutput);
        if(!job1.waitForCompletion(true)){
            System.exit(1);
        }
        Configuration c2=new Configuration();
        Job job2 = Job.getInstance(c2, "highestRatedMovieNamePerUser");


        MultipleInputs.addInputPath(job2, tempOutput, TextInputFormat.class, MapForMovieIdHighestRating.class);
        MultipleInputs.addInputPath(job2, movie, TextInputFormat.class, MapForMovieIdName.class);
        job2.setJarByClass(HighestRatedMovieNamePerUserId.class);
        job2.setReducerClass(ReducerForHighestRatingName.class);
        // j'ai décidé de renvoyer des string ici puisqu' aucune opération mathématique  ne sera fait sur les userId ou movieId
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, outputJob);
        boolean resultjob2 = job2.waitForCompletion(true);
        System.out.println("Status de fin du job de récupération du film le mieux noté par utilisateur : " + resultjob2);
        System.exit(resultjob2 ? 0 : 1);


    }
    public static class MapForMovieHighestRating extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.contains("userId")){
                return;
            }
            String[] columnsForLine=  line.split(",");
            String userId = columnsForLine[0];
            String rating = columnsForLine[2];
            String movieId=columnsForLine[1];
            Text outputValue = new Text(rating+"|"+movieId);
            Text outputKey = new Text(userId);
            con.write(outputKey, outputValue);
        }
    }
    public static class ReducerForHighestRating extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text userId, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            double maxRating = 0;
            Text movieIdForMaxRating =new Text();

            for (Text value : values) {
                double rating = Double.parseDouble(value.toString().split("\\|")[0]);

                if(rating > maxRating){
                    maxRating = rating;
                    movieIdForMaxRating =  new Text(value.toString().split("\\|")[1]);
                }
            }
            con.write(userId, movieIdForMaxRating);
        }
    }
    public static class MapForMovieIdHighestRating extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnsForLine = line.split("\t");
            String userId = columnsForLine[0];
            String movieId= columnsForLine[1];

            Text outputValue = new Text(userId);
            Text outputKey = new Text(movieId);
            con.write(outputKey, outputValue);
        }
    }
    public static class MapForMovieIdName extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.contains("movieId")){
                return;
            }
            String[] columnsForLine=  line.split(",");
            String movieId = columnsForLine[0];
            String title = columnsForLine[1];
            Text outputValue = new Text(title+"|name");
            Text outputKey = new Text(movieId);
            con.write(outputKey, outputValue);
        }
    }
    public static class ReducerForHighestRatingName extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text movieId, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            String movieName = null;

            for (Text value : values) {
                if (value.toString().contains("|name")) {
                    movieName = value.toString().split("\\|name")[0];
                    break;
                }
            }

            if (movieName == null) {
                log.warning("Missing movie name for movieId: " + movieId);
                return;
            }

            for (Text value : values) {
                if (!value.toString().contains("|name")) {
                    con.write(value, new Text(movieName));
                }
            }
        }
    }

}
