package mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieBean implements WritableComparable<MovieBean> {
    private String movie;
    private String rate;
    private String timeStamp;
    private String userId;

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return movie + "\t" +
                rate + "\t" +
                timeStamp + "\t" +
                userId ;
    }


    public int compareTo(MovieBean movieBeen){
        return this.rate.compareTo(movieBeen.rate);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeUTF(rate);
        dataOutput.writeUTF(timeStamp);
        dataOutput.writeUTF(userId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.movie = dataInput.readUTF();
        this.rate = dataInput.readUTF();
        this.timeStamp = dataInput.readUTF();
        this.userId = dataInput.readUTF();
    }

}
