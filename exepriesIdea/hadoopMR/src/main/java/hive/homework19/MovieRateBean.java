package hive.homework19;

import java.util.Objects;

public class MovieRateBean {
    private String movie;
    private String rate;
    private String timeStamp;
    private String uid;

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

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieRateBean that = (MovieRateBean) o;
        return Objects.equals(movie, that.movie) &&
                Objects.equals(rate, that.rate) &&
                Objects.equals(timeStamp, that.timeStamp) &&
                Objects.equals(uid, that.uid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movie, rate, timeStamp, uid);
    }

    @Override
    public String toString() {
        return  movie + "\t" +
                 rate + "\t" +
               timeStamp + "\t" +
                uid ;
    }
}
