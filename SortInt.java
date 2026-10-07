cat > SortInt.java << 'EOF'
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortInt {
    public static class M extends Mapper<Object, Text, IntWritable, NullWritable> {
        public void map(Object key, Text value, Context ctx)
                throws IOException, InterruptedException {
            String s = value.toString().trim();
            if (!s.isEmpty()) {
                ctx.write(new IntWritable(Integer.parseInt(s)), NullWritable.get());
            }
        }
    }

    public static class R extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
        private int rank = 1;

        public void reduce(IntWritable key, Iterable<NullWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            for (NullWritable v : vals) {
                ctx.write(new IntWritable(rank++), key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort int");
        job.setJarByClass(SortInt.class);
        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1); // 保证全局有序
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
EOF