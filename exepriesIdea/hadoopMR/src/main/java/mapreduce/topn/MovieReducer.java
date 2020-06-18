package mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MovieReducer extends Reducer<Text,MovieBean,Text,MovieBean> {

    protected void reduce(Text key,Iterable<MovieBean> values,Context context) throws IOException, InterruptedException {

        List<MovieBean> movieBeans = new ArrayList<MovieBean>();

        Configuration configuration = context.getConfiguration();
       // System.out.println("------:" + configuration.get("topN"));
        int topN = configuration.getInt("topN",5);

        for (MovieBean m:values) {
            MovieBean newMovieBean = new MovieBean();
            newMovieBean.setMovie(m.getMovie());
            newMovieBean.setRate(m.getRate());
            newMovieBean.setTimeStamp(m.getTimeStamp());
            newMovieBean.setUserId(m.getUserId());
            movieBeans.add(newMovieBean);
        }

        Collections.sort(movieBeans);
        for (int i =0; i < topN;i++){
            context.write(key,movieBeans.get(i));
        }


    }
}
