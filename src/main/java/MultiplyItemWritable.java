import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MultiplyItemWritable implements Writable {

    BooleanWritable isM;
    LongWritable coordinate;
    DoubleWritable itemValue;

    public MultiplyItemWritable() {
        this.isM = new BooleanWritable();
        this.coordinate = new LongWritable();
        this.itemValue = new DoubleWritable();
    }

    public MultiplyItemWritable(MultiplyItemWritable otherItem) {
        this.isM = new BooleanWritable(otherItem.isM.get());
        this.coordinate = new LongWritable(otherItem.coordinate.get());
        this.itemValue = new DoubleWritable(otherItem.itemValue.get());
    }

    public MultiplyItemWritable(boolean isM, long coordinate, double itemValue) {
        this.isM = new BooleanWritable(isM);
        this.coordinate = new LongWritable(coordinate);
        this.itemValue = new DoubleWritable(itemValue);
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.isM.write(dataOutput);
        this.coordinate.write(dataOutput);
        this.itemValue.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.isM.readFields(dataInput);
        this.coordinate.readFields(dataInput);
        this.itemValue.readFields(dataInput);
    }

}
