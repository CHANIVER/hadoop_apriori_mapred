package cse;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class AprioriMR {
    private static final IntWritable one = new IntWritable(1);
    private static int minsup;
    private static int iteration;
    private static boolean isDone = false;

    public static class AprioriMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text itemset = new Text();
        private List<String> prevItemsets;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();

            minsup = conf.getInt("minsup", 0);
            iteration = conf.getInt("iteration", 0);

            // iteration이 1보다 클 때만 readPreviousItemsets 메서드를 호출합니다.
            if (iteration > 1) {
                URI[] uris = Job.getInstance(conf).getCacheFiles();
                prevItemsets = readPreviousItemsets(uris);
            }
        }

        private List<String> readPreviousItemsets(URI[] uris) throws IOException {
            List<String> prevItemsets = new ArrayList<>();
            for (URI uri : uris) {
                FileSystem fs = FileSystem.get(uri, new Configuration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] split = line.split("\t");
                    prevItemsets.add(split[0]);
                }

                reader.close();
            }

            return prevItemsets;
        }
        private List<List<String>> singletons(String[] items) {
            List<List<String>> singletonItemsets = new ArrayList<>();
            for (String item : items) {
                singletonItemsets.add(Collections.singletonList(item));
            }
            return singletonItemsets;
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(" ");
            List<String> itemsList = new ArrayList<>(Arrays.asList(items));
            List<List<String>> candidateItemsets;

            if (iteration == 1) {
                candidateItemsets = singletons(items);
            } else {
                candidateItemsets = generateCandidateItemsets(prevItemsets, items);
            }

            Set<String> uniqueCandidateItemsets = new HashSet<>();

            for (List<String> candidateItemset : candidateItemsets) {
                if (candidateItemset.size() == iteration && itemsList.containsAll(candidateItemset)) {
                    String itemsetString = String.join(" ", candidateItemset);
                    uniqueCandidateItemsets.add(itemsetString);
                }
            }

            for (String uniqueCandidateItemset : uniqueCandidateItemsets) {
                itemset.set(uniqueCandidateItemset);
                context.write(itemset, one);
                System.out.println("Mapper output: " + uniqueCandidateItemset);
            }
        }


        
        private List<List<String>> generateCandidateItemsets(List<String> prevItemsets, String[] items) {
            List<List<String>> candidateItemsets = new ArrayList<>();
            List<String> itemsList = new ArrayList<>(Arrays.asList(items));

            for (String prevItemset : prevItemsets) {
                List<String> prevItemsetList = new ArrayList<>(Arrays.asList(prevItemset.split(" ")));
                for (String item : itemsList) {
                    if (!prevItemsetList.contains(item)) {
                        List<String> newCandidate = new ArrayList<>(prevItemsetList);
                        newCandidate.add(item);
                        Collections.sort(newCandidate);  // 항상 동일한 순서를 유지하기 위해 정렬
                        if (!candidateItemsets.contains(newCandidate)) { // 중복 제거
                            candidateItemsets.add(newCandidate);
                        }
                    }
                }
            }

            return candidateItemsets;
        }



    }

    public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<Text, IntWritable> resultMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("minsup", 0); // minsup 값을 Configuration에서 읽어옴
            resultMap = new HashMap<>();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            System.out.println("Reducer key: " + key.toString() + ", sum: " + sum);
            //System.out.println("minsup: " + minsup + ", sum: " + sum + ", key: " + key.toString());
            if (sum >= minsup) {
                context.getCounter(AprioriMR.class.getName(), "numCandidates").increment(1);
                context.write(key, result);
                resultMap.put(new Text(key), new IntWritable(sum));
            }
        }

       
        

    }

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        minsup = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        conf.setInt("minsup", minsup);


        while (!isDone) {
            System.out.println("Iteration " + iteration + " starts.");
            conf.setInt("iteration", ++iteration);

            Job job = Job.getInstance(conf, "apriori");
            job.setJarByClass(AprioriMR.class);
            job.setMapperClass(AprioriMapper.class);
            job.setCombinerClass(AprioriReducer.class);
            job.setReducerClass(AprioriReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath, String.valueOf(iteration)));

            if (iteration > 1) {
                FileStatus[] statuses = fs.listStatus(new Path(outputPath, String.valueOf(iteration - 1)));
                for (FileStatus status : statuses) {
                    job.addCacheFile(status.getPath().toUri());
                }
            }

            if (job.waitForCompletion(true)) {
                long numCandidates = job.getCounters().findCounter(AprioriMR.class.getName(), "numCandidates").getValue();
                if (numCandidates == 0) {
                    isDone = true;
                    
                }
            }
        }
        
     // MapReduce 작업이 끝난 후에 각 출력 파일의 내용을 순회하고 합치는 작업을 수행합니다.
        Path resultFile = new Path(outputPath, "final_result");
        FSDataOutputStream out = fs.create(resultFile);

        for (int i = 1; i <= iteration; i++) {
            Path iterOutputPath = new Path(outputPath, String.valueOf(i));
            FileStatus[] statuses = fs.listStatus(iterOutputPath);
            for (FileStatus status : statuses) {
                if (status.getPath().getName().startsWith("part-r-")) {
                    FSDataInputStream in = fs.open(status.getPath());
                    IOUtils.copyBytes(in, out, conf, false); // 내용을 복사합니다.
                    in.close();
                }
            }
        }

        out.close();
    }
}
