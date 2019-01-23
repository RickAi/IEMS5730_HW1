import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * author: yongbiaoai@gmail.com
 */
public class Main {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            if (otherArgs.length < 5) {
                System.out.println("hadoop jar xxx.jar Main [M input] [N input] [tmp] [output] [reducer count]");
                System.exit(1);
            }

            FileSystem fileSystem = FileSystem.get(conf);
            if (!fileSystem.exists(new Path(otherArgs[2]))) {
                // init class path
                Job matrixMultiplyJob = Job.getInstance();
                matrixMultiplyJob.setJarByClass(Main.class);

                // set up input and output
                MultipleInputs.addInputPath(matrixMultiplyJob, new Path(otherArgs[0]), TextInputFormat.class, Mappers.MatrixMMapper.class);
                MultipleInputs.addInputPath(matrixMultiplyJob, new Path(otherArgs[1]), TextInputFormat.class, Mappers.MatrixNMapper.class);
                FileOutputFormat.setOutputPath(matrixMultiplyJob, new Path(otherArgs[2]));

                // set up format
                matrixMultiplyJob.setReducerClass(Reducers.MatrixMultipleReducer.class);

                matrixMultiplyJob.setMapOutputKeyClass(LongWritable.class);
                matrixMultiplyJob.setMapOutputValueClass(MultiplyItemWritable.class);
                matrixMultiplyJob.setOutputKeyClass(MatrixCoordinatesWritable.class);
                matrixMultiplyJob.setOutputValueClass(DoubleWritable.class);

                matrixMultiplyJob.setOutputFormatClass(TextOutputFormat.class);

                // start job with sync mode
                if (matrixMultiplyJob.waitForCompletion(true)) {
                    System.out.println("MatrixMultiplyJob job success.");
                } else {
                    System.out.println("MatrixMultiplyJob job failed.");
                }
            } else {
                System.out.println("Skip MatrixMultiplyJob job, the tmp data already exists.");
            }

            // set up matrix sum job
            Job matrixSumJob = Job.getInstance();
            matrixSumJob.setJarByClass(Main.class);

            // set up config
            matrixSumJob.setMapperClass(Mappers.MatrixMapper.class);
            matrixSumJob.setReducerClass(Reducers.MatrixSumReducer.class);

            matrixSumJob.setMapOutputKeyClass(MatrixCoordinatesWritable.class);
            matrixSumJob.setMapOutputValueClass(DoubleWritable.class);
            matrixSumJob.setOutputKeyClass(MatrixCoordinatesWritable.class);
            matrixSumJob.setOutputValueClass(DoubleWritable.class);

            matrixSumJob.setInputFormatClass(TextInputFormat.class);
            matrixSumJob.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(matrixSumJob, new Path(otherArgs[2]));
            FileOutputFormat.setOutputPath(matrixSumJob, new Path(otherArgs[3]));

            matrixSumJob.setNumReduceTasks(Integer.valueOf(otherArgs[4]));

            // start with sync mode
            if (matrixSumJob.waitForCompletion(true)) {
                System.out.println("MatrixSumJob job success.");
            } else {
                System.out.println("MatrixSumJob job failed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
