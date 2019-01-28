import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixCoordinatesWritable implements WritableComparable<MatrixCoordinatesWritable> {

    LongWritable row;
    LongWritable column;

    public MatrixCoordinatesWritable() {
        this.row = new LongWritable();
        this.column = new LongWritable();
    }

    public MatrixCoordinatesWritable(long row, long column) {
        this.row = new LongWritable(row);
        this.column = new LongWritable(column);
    }

    public MatrixCoordinatesWritable(LongWritable row, LongWritable column) {
        this(row.get(), column.get());
    }

    public int compareTo(MatrixCoordinatesWritable other) {
        int result = compare(this.row.get(), other.row.get());
        if (result == 0) {
            result = compare(this.column.get(), other.column.get());
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        row.write(dataOutput);
        column.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        row.readFields(dataInput);
        column.readFields(dataInput);
    }

    private int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(row.get()).append("\t").append(column.get());
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return this.row.hashCode() * 13 + (this.column == null ? 0 : this.column.hashCode());
    }
}
