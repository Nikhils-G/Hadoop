import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MovieNameMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row of csv
        if (key.get() == 0 && value.toString().contains("movieId")) {
            return;
        }

        String line = value.toString();
        String[] moviePieces = line.split(",");

        if (moviePieces.length >= 2) {
            // ratingPieces is: [movieId,title,genres], want (movieId, title)
            int movieId = Integer.parseInt(moviePieces[0]);
            String title = moviePieces[1];

            // Convert to Hadoop types (mark this as the "title" value)
            IntWritable mapKey = new IntWritable(movieId);
            Text mapValue = new Text("title\t" + title);

            // Output intermediate key,value pair
            context.write(mapKey, mapValue);
        }
    }
}


MovieRatingMapper.java
package org.codewithmuktha;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row of csv
        if (key.get() == 0 && value.toString().contains("userId")) {
            return;
        }

        String line = value.toString();
        String[] ratingPieces = line.split(",");

        if (ratingPieces.length >= 2) {
            // ratingPieces is: [userId, movieId, rating, timestamp]
            int movieId = Integer.parseInt(ratingPieces[1]);
            double rating = Double.parseDouble(ratingPieces[2]);

            // Convert to Hadoop types
            IntWritable mapKey = new IntWritable(movieId);
            DoubleWritable mapValue = new DoubleWritable(rating);

            // Output intermediate key,value pair
            context.write(mapKey, mapValue);
        }
    }
}
MovieReducer.java
package org.codewithmuktha;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException{

        double avgRating = 0;
        int numValues = 0;

        // Add up all the ratings
        for (DoubleWritable value: values) {
            avgRating += value.get();
            numValues++;
        }

        // Divide by the number of ratings to get the average for this movie
        avgRating /= numValues;
        DoubleWritable average = new DoubleWritable(avgRating);

        // Output the average rating for this movie
        context.write(key, average);
    }
}

MovieRating.java
package org.codewithmuktha;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatings {

    public static void main(String[] args) throws Exception {
        // Check that the input and output path have been provided
        if (args.length != 2) {
            System.err.println("Syntax: MovieRatings <input path> <output path>");
            System.exit(-1);
        }

        // Create an instance of the MapReduce job
        Job job = new Job();
        job.setJarByClass(MovieRatings.class);
        job.setJobName("Average movie rating");

        // Set input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set mapper and reducer classes
        job.setMapperClass(MovieRatingMapper.class);
        job.setReducerClass(MovieRatingReducer.class);

        // Set output key/value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Run the job and then exit the program
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
MovieNamesRating.java
package org.codewithmuktha;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class MovieNamesRatings {

    public static void main(String[] args) throws Exception {
        // Check that the input and output path have been provided
        if (args.length != 3) {
            System.err.println("Syntax: MovieNamesRatings <ratings input path> <titles input path> <output path>");
            System.exit(-1);
        }

        // Create an instance of the MapReduce job
        Job job = new Job();
        job.setJarByClass(MovieNamesRatings.class);
        job.setJobName("Average movie rating by movie title");

        // Set input and output locations
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieNameRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieNameMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Set reducer classes
        job.setReducerClass(MovieNameRatingReducer.class);

        // Set output key/value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Run the job and then exit the program
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

MovieNameRatingReducer.java
package org.codewithmuktha;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieNameRatingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        double avgRating = 0;
        int numValues = 0;
        String title = "";

        // Add up all the ratings
        for (Text value: values) {
            // Separate the value from the tag
            String parts[] = value.toString().split("\t");

            if (parts[0].equals("title")) {
                // Get the title
                title = parts[1];
            }
            else {
                // Get a rating for this title
                double ratingValue = Double.parseDouble(parts[1]);
                avgRating += ratingValue;
                numValues++;
            }
        }

        // Divide by the number of ratings to get the average for this movie
        avgRating /= numValues;

        String outputValue = title + "\n" + avgRating;
        Text output = new Text(outputValue);

        // Output movieId, title and avg rating
        context.write(key, output);
    }
}

Output (1 ,3)

Open terminal :
Hadoop jar target/FinalMovielens-1.0-SNAPSHOT.jar org.codewithmuktha.MovieRatings /MovielensData/ratings.csv /output1
