/*
package org.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Movie {


    static Logger log = Logger.getLogger(Movie.class.getName());

    public static void main(String[] args) throws Exception {
*/
/*        Configuration c= new Configuration();
        //highest rated movie per user
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        if(files.length<2){
            System.err.println("Usage: Movie <input path> <output path>");
            System.exit(-1);
        }
        Path input = new Path(files[0]);
      Path output = new Path(files[1]);
        Job j = Job.getInstance(c, "highestRatedMoviePerUser");
        j.setJarByClass(Movie.class);
        j.setMapperClass(MapForMovieHighestRating.class);
        j.setReducerClass(ReducerForHighestRating.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        boolean resultjob = j.waitForCompletion(true);
        System.out.println("status de fin du job" + resultjob);
        System.exit(resultjob ? 0 : 1);*//*

        Configuration c= new Configuration();
        //highest rated movie per user
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        if(files.length<3){
            System.err.println("Usage: Movie <ratings path> <movie path>  <output path>");
            System.exit(-1);
        }
        Path rating = new Path(files[0]);
        Path movie = new Path(files[1]);
        Path outputJob2 = new Path(files[2]+"-tmp"+"-tmp");
        Path outputJob3 = new Path(files[2]+"-tmp"+"-tmp"+"-tmp");
        Path outputJob = new Path(files[2]);
        Path outputJob1 = new Path(files[2]+"-tmp");
        Job job1 = Job.getInstance(c, "highestRatedMovieIdPerUser");
        job1.setJarByClass(Movie.class);
        job1.setMapperClass(MapForMovieHighestRating.class);
        job1.setReducerClass(ReducerForHighestRating.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, rating);
        FileOutputFormat.setOutputPath(job1, outputJob1);
if(!job1.waitForCompletion(true)){
    System.exit(1);
}
Configuration c2=new Configuration();
        Job job2 = Job.getInstance(c2, "highestRatedMovieNamePerUser");


        MultipleInputs.addInputPath(job2, outputJob1, TextInputFormat.class, MapForMovieIdHighestRating.class);
        MultipleInputs.addInputPath(job2, movie, TextInputFormat.class, MapForMovieIdName.class);
        job2.setJarByClass(Movie.class);
        job2.setReducerClass(ReducerForHighestRatingName.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, outputJob2);
        boolean resultjob2 = job2.waitForCompletion(true);
        System.out.println("status de fin du job : " + resultjob2);
        if(!resultjob2){
            System.exit(1);
        }
        Configuration c3=new Configuration();
        Job job3= Job.getInstance(c3, "moviecountbyName");
        job3.setJarByClass(Movie.class);
        job3.setMapperClass(MapForMovieCountByUserId.class);
        job3.setReducerClass(ReducerForCountByMovie.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, outputJob2);
        FileOutputFormat.setOutputPath(job3, outputJob3);
        if(!job3.waitForCompletion(true)){
            System.exit(1);
        }
        Configuration c4=new Configuration();
        Job job4= Job.getInstance(c4, "movieCountFinal");
        job4.setJarByClass(Movie.class);
        job4.setMapperClass(MapToCountMovieName.class);
        job4.setReducerClass(ReduceForCountMovieName.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, outputJob3);
        FileOutputFormat.setOutputPath(job4, outputJob);
        boolean resultjob4 = job4.waitForCompletion(true);
        System.out.println("status de fin du job : " + resultjob4);
        System.exit(resultjob4 ? 0 : 1);



//
//        Job j = Job.getInstance(c, "moviecountbygenre");
//        j.setJarByClass(Movie.class);
//        j.setMapperClass(MapForMovieCount.class);
//        j.setReducerClass(ReduceForWordCount.class);
//        j.setOutputKeyClass(Text.class);
//        j.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(j, input);
//        Configuration c = new Configuration();
//        // Utiliser Arrays.copyOfRange pour obtenir les arguments à partir de l'indice 0 jusqu'à args.length - 2
//        String[] filesName = Arrays.copyOfRange(args, 0, args.length - 1);
//        String genre = args[args.length - 1];
//         c.set("genre", genre);
//        String[] files = new GenericOptionsParser(c, filesName).getRemainingArgs();
//        if (files.length < 2) {
//            System.err.println("Usage: Movie <input path> <output path>");
//            System.exit(-1);
//        }
//        Path input = new Path(files[0]);
//        Path output = new Path(files[1]);
//
//        Job j = Job.getInstance(c, "moviecountbygenre");
//        j.setJarByClass(Movie.class);
//        j.setMapperClass(MapForMovieCount.class);
//        j.setReducerClass(ReduceForWordCount.class);
//        j.setOutputKeyClass(Text.class);
//        j.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(j, input);
//      //  MultiFileInputFormat.addInputPath(j, input, TextInputFormat.class, );
//        //faire un mapper par fichier de multiple format regarder la code pour gerer si'l sagit de la partie 1 ou 2 du join
//        FileOutputFormat.setOutputPath(j, output);
//
//        boolean resultjob = j.waitForCompletion(true);
//        System.out.println("status de fin du job" + resultjob);
//        System.exit(resultjob ? 0 : 1);
    }

    */
/*//*
/Count the number of movies by genre
    public static class MapForMovieCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
          String[] columnsForLine=  line.split(",");
            String genreString = columnsForLine[2];
            String[] genres = genreString.split("\\|");
            for (String genre : genres) {

                Text outputKey = new Text(genre.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);
            }

        }
    }*//*


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


 */
/*   //count the number of movies for an input genre
    public static class MapForMovieCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static  String genre;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            genre = context.getConfiguration().get("genre");
        }
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnsForLine=  line.split(",");
            String genreString = columnsForLine[2];
            String[] genres = genreString.split("\\|");
            for (String genreIn : genres) {

                Text outputKey = new Text(genreIn.toUpperCase().trim());
                if(outputKey.toString().equals(genre)) {
                    IntWritable outputValue = new IntWritable(1);
                    con.write(outputKey, outputValue);
                }
            }

        }
    }*//*


    //count the number of movies for an input genre
    public static class MapForMovieCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static  String genre;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            genre = context.getConfiguration().get("genre");
        }
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnsForLine=  line.split(",");
            String genreString = columnsForLine[2];
            String[] genres = genreString.split("\\|");
            for (String genreIn : genres) {

                Text outputKey = new Text(genreIn.toUpperCase().trim());
                if(outputKey.toString().equals(genre)) {
                    IntWritable outputValue = new IntWritable(1);
                    con.write(outputKey, outputValue);
                }
            }

        }
    }


    //count the number of movies for an input genre
    public static class MapForMovieCountByUserId extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columnsForLine = line.split("\t");
            String movieName= columnsForLine[1];
            con.write(new Text(movieName), new IntWritable(1));

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

    public static class ReducerForHighestRatingName extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text movieId, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            String movieName = null;

            // Première boucle pour trouver le nom du film
            for (Text value : values) {
                if (value.toString().contains("|name")) {
                    movieName = value.toString().split("\\|name")[0];
                    break; // Une seule valeur devrait contenir "|name"
                }
            }

            // Si aucun nom de film n'est trouvé
            if (movieName == null) {
                con.getCounter("Reducer", "MissingMovieName").increment(1);
                return;
            }

            // Deuxième boucle pour écrire les autres valeurs avec le nom du film
            for (Text value : values) {
                if (!value.toString().contains("|name")) {
                    con.write(value, new Text(movieName));
                }
            }
        }
    }


    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text genre, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(genre, new IntWritable(sum)); // Écrire la somme totale pour le mot
        }
    }
}
*/
