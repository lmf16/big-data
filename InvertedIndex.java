import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class InvertedIndex {

    // Mapper 阶段
    public static class InvIndexMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docId = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 从分布式缓存读取停词表
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                br.close();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获取当前文件的文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            docId.set(fileName);

            // 分词，过滤标点符号
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z]", " "));
            while (itr.hasMoreTokens()) {
                String term = itr.nextToken().toLowerCase();
                // 过滤停词
                if (!stopWords.contains(term)) {
                    word.set(term);
                    // 输出格式: <单词, 文件名>
                    context.write(word, docId);
                }
            }
        }
    }

    // Combiner 阶段：在 Map 端本地进行聚合，减少网络传输
    public static class InvIndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> docCountMap = new HashMap<>();
            for (Text val : values) {
                String doc = val.toString();
                docCountMap.put(doc, docCountMap.getOrDefault(doc, 0) + 1);
            }
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : docCountMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
            }
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    // Reducer 阶段：统计并格式化输出
    public static class InvIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 收集所有文档及词频，格式为 doc:count;
            Map<String, Integer> docCountMap = new HashMap<>();
            for (Text val : values) {
                String[] parts = val.toString().split(";");
                for (String part : parts) {
                    if (part.isEmpty()) continue;
                    String[] kv = part.split(":");
                    String doc = kv[0];
                    int count = Integer.parseInt(kv[1]);
                    docCountMap.put(doc, docCountMap.getOrDefault(doc, 0) + count);
                }
            }

            // 构建最终输出格式：<document01.txt,6>;<document02.txt,3>;<total,11>.
            StringBuilder sb = new StringBuilder();
            int total = 0;
            for (Map.Entry<String, Integer> entry : docCountMap.entrySet()) {
                sb.append("<").append(entry.getKey()).append(",").append(entry.getValue()).append(">;");
                total += entry.getValue();
            }
            sb.append("<total,").append(total).append(">.");
            
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    // Driver 主程序
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvIndexMapper.class);
        job.setCombinerClass(InvIndexCombiner.class);
        job.setReducerClass(InvIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将停词表加入分布式缓存
        job.addCacheFile(new URI(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}