import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankMR {

    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) return;

            String page = parts[0];
            double pr = (parts.length == 3) ? Double.parseDouble(parts[1]) : 1.0;
            String[] links = (parts.length == 3) ? parts[2].split(",") : parts[1].split(",");

            context.write(new Text(page), new Text("|" + String.join(",", links)));

            if (links.length > 0) {
                double contrib = pr / links.length;
                for (String link : links) {
                    context.write(new Text(link), new Text(String.valueOf(contrib)));
                }
            }
        }
    }

    public static class PRReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            String links = "";

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("|")) {
                    links = s.substring(1);
                } else {
                    sum += Double.parseDouble(s);
                }
            }

            double newPR = 0.15 + 0.85 * sum;
            context.write(key, new Text(String.format("%.10f", newPR) + "\t" + links));
        }
    }

    public static class FormatMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 2) {
                context.write(new Text("(" + parts[0] + "," + parts[1] + ")"), new Text(""));
            }
        }
    }

    public static class FormatReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, null);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "/user/hadoop/pagerank/input";
        String tempOutput = "/user/hadoop/pagerank/temp";
        int iterations = 10;

        for (int i = 0; i < iterations; i++) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "PageRank Iter " + i);
            job.setJarByClass(PageRankMR.class);
            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(i == 0 ? input : tempOutput + (i - 1)));
            FileOutputFormat.setOutputPath(job, new Path(tempOutput + i));

            if (!job.waitForCompletion(true)) System.exit(1);
        }

        Configuration formatConf = new Configuration();
        Job formatJob = Job.getInstance(formatConf, "Format Output");
        formatJob.setJarByClass(PageRankMR.class);
        formatJob.setMapperClass(FormatMapper.class);
        formatJob.setReducerClass(FormatReducer.class);
        formatJob.setOutputKeyClass(Text.class);
        formatJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(formatJob, new Path(tempOutput + (iterations - 1)));
        FileOutputFormat.setOutputPath(formatJob, new Path("/user/hadoop/pagerank/output"));

        System.exit(formatJob.waitForCompletion(true) ? 0 : 1);
    }
}