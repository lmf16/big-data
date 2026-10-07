cat > GrandParent.java << 'EOF'
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrandParent {
    public static class M extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context ctx)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("child")) return;
            String[] arr = line.split("\\s+");
            if (arr.length < 2) return;

            String child = arr[0];
            String parent = arr[1];

            ctx.write(new Text(parent), new Text("C:" + child));
            ctx.write(new Text(child), new Text("P:" + parent));
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            List<String> children = new ArrayList<>();
            List<String> parents = new ArrayList<>();

            for (Text t : vals) {
                String s = t.toString();
                if (s.startsWith("C:")) {
                    children.add(s.substring(2));
                } else if (s.startsWith("P:")) {
                    parents.add(s.substring(2));
                }
            }

            for (String c : children) {
                for (String p : parents) {
                    ctx.write(new Text(c), new Text(p));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "grand parent");
        job.setJarByClass(GrandParent.class);
        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
EOF