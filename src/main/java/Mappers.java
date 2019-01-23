import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mappers {

    public static class MatrixMMapper extends Mapper<LongWritable, Text, LongWritable, MultiplyItemWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineTokens = line.split("\t");

            if (lineTokens.length != 3) {
                return;
            }

            LongWritable pivotValue = new LongWritable(Long.valueOf(lineTokens[1]));
            MultiplyItemWritable item = new MultiplyItemWritable(true, Long.valueOf(lineTokens[0]), Double.valueOf(lineTokens[2]));
            context.write(pivotValue, item);
        }
    }

    public static class MatrixNMapper extends Mapper<LongWritable, Text, LongWritable, MultiplyItemWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineTokens = line.split("\t");

            if (lineTokens.length != 3) {
                return;
            }

            LongWritable pivotValue = new LongWritable(Long.valueOf(lineTokens[0]));
            MultiplyItemWritable item = new MultiplyItemWritable(false, Long.valueOf(lineTokens[1]), Double.valueOf(lineTokens[2]));
            context.write(pivotValue, item);
        }
    }

    public static class MatrixMapper extends Mapper<LongWritable, Text, MatrixCoordinatesWritable, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineTokens = line.split("\t");

            if (lineTokens.length != 3) {
                return;
            }

            MatrixCoordinatesWritable coordinate = new MatrixCoordinatesWritable(Long.valueOf(lineTokens[0]), Long.valueOf(lineTokens[1]));
            context.write(coordinate, new DoubleWritable(Double.valueOf(lineTokens[2])));
        }
    }

}
