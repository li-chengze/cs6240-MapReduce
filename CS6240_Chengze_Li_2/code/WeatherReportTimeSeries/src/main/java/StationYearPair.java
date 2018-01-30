import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StationYearPair implements Writable, WritableComparable<StationYearPair>{

    private Text stationId = new Text();
    private IntWritable year = new IntWritable();

    public StationYearPair(){}

    public StationYearPair(String stationId, int year) {
        this.stationId.set(stationId);
        this.year.set(year);
    }

    public void setStationId(String stationId) {
        this.stationId.set(stationId);
    }

    public void setYear(int year) {
        this.year.set(year);
    }

    public Text getStationId() {
        return this.stationId;
    }

    public int getYear() {
        return this.year.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        stationId.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId.readFields(in);
        year.readFields(in);
    }

    @Override
    public int compareTo(StationYearPair that) {
        int result = this.stationId.compareTo(that.stationId);
        if (result == 0) {
            result = this.year.compareTo(that.year);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(stationId);
        sb.append("_");
        sb.append(year);

        return sb.toString();
    }
}
