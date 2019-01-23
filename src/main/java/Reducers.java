import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reducers {

    public static class MatrixMultipleReducer extends Reducer<LongWritable, MultiplyItemWritable, MatrixCoordinatesWritable, DoubleWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<MultiplyItemWritable> values, Context context) throws IOException, InterruptedException {
            List<MultiplyItemWritable> itemMList = new ArrayList<MultiplyItemWritable>();
            List<MultiplyItemWritable> itemNList = new ArrayList<MultiplyItemWritable>();

            for (MultiplyItemWritable item : values) {
                if (item.isM.get()) {
                    itemMList.add(new MultiplyItemWritable(item));
                } else {
                    itemNList.add(new MultiplyItemWritable(item));
                }
            }

            for (int i = 0; i < itemMList.size(); i++) {
                for (int j = 0; j < itemNList.size(); j++) {
                    MatrixCoordinatesWritable coordinate = new MatrixCoordinatesWritable(itemMList.get(i).coordinate, itemNList.get(j).coordinate);
                    double multiply = itemMList.get(i).itemValue.get() * itemNList.get(j).itemValue.get();
                    context.write(coordinate, new DoubleWritable(multiply));
                }
            }
        }

    }

    public static class MatrixSumReducer extends Reducer<MatrixCoordinatesWritable, DoubleWritable, MatrixCoordinatesWritable, DoubleWritable> {

        @Override
        protected void reduce(MatrixCoordinatesWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable item : values) {
                sum += item.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

}
