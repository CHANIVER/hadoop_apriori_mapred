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
    private static final IntWritable one = new IntWritable(1); // MapReduce 작업에서 사용할 값 1을 초기화합니다.
    private static int minsup; // 지지도 임계값 미리 설정
    private static int iteration; // 반복횟수를 저장하는 변수
    private static boolean isDone = false; // 알고리즘의 완료 상태를 나타내는 플래그

    // Map 함수를 정의하는 AprioriMapper 클래스
    public static class AprioriMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text itemset = new Text(); // 키 값으로 사용할 itemset을 선언
        private List<String> prevItemsets; // 이전 반복에서 생성된 itemset을 저장하는 리스트

        // Mapper setup 메서드: Map 작업 시작 전에 한 번만 실행되는 메서드
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();

            // minsup와 iteration 값을 설정 파일에서 읽어옴
            minsup = conf.getInt("minsup", 0);
            iteration = conf.getInt("iteration", 0);

            // iteration이 1보다 클 때만 readPreviousItemsets 메서드를 호출. 이전 itemset을 읽어옴
            if (iteration > 1) {
                URI[] uris = Job.getInstance(conf).getCacheFiles();
                prevItemsets = readPreviousItemsets(uris);
            }
        }

        // 이전 반복에서 생성된 itemset을 읽어오는 메서드
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

        // 입력된 items 배열을 singleton으로 변환하는 메서드
        private List<List<String>> singletons(String[] items) {
            List<List<String>> singletonItemsets = new ArrayList<>();
            for (String item : items) {
                singletonItemsets.add(Collections.singletonList(item));
            }
            return singletonItemsets;
        }

        // Map 함수: 각 행마다 실행되는 함수
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(" ");
            List<String> itemsList = new ArrayList<>(Arrays.asList(items));
            List<List<String>> candidateItemsets;

            // 첫 번째 반복에서는 singleton 생성, 이후 반복에서는 이전 itemset을 활용해 candidate itemset 생성
            if (iteration == 1) {
                candidateItemsets = singletons(items);
            } else {
                candidateItemsets = generateCandidateItemsets(prevItemsets, items);
            }

            // 후보 itemsets을 저장하는 uniqueCandidateItemsets 선언
            Set<String> uniqueCandidateItemsets = new HashSet<>();

            // 생성된 candidate itemset을 통해 unique itemset을 생성
            for (List<String> candidateItemset : candidateItemsets) {
                if (candidateItemset.size() == iteration && itemsList.containsAll(candidateItemset)) {
                    String itemsetString = String.join(" ", candidateItemset);
                    uniqueCandidateItemsets.add(itemsetString);
                }
            }

            // 생성된 unique itemset을 context에 쓰기
            for (String uniqueCandidateItemset : uniqueCandidateItemsets) {
                itemset.set(uniqueCandidateItemset);
                context.write(itemset, one);
                System.out.println("Mapper output: " + uniqueCandidateItemset);
            }
        }

        // 이전 itemsets과 현재 아이템 배열을 이용하여 후보 itemsets을 생성하는 메서드
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

    // Reduce 함수를 정의하는 AprioriReducer 클래스
    public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable(); // 결과 값을 저장할 변수 선언
        private Map<Text, IntWritable> resultMap; // 결과를 저장할 맵 선언

        // Reducer setup 메서드: Reduce 작업 시작 전에 한 번만 실행되는 메서드
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("minsup", 0); // minsup 값을 설정 파일에서 읽어옴
            resultMap = new HashMap<>();
        }

        // Reduce 함수: 같은 key를 가진 value들을 합치는 함수
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            System.out.println("Reducer key: " + key.toString() + ", sum: " + sum);
            // 지지도 임계값 이상일 경우에만 결과를 context에 쓰고 결과 맵에 추가
            if (sum >= minsup) {
                context.getCounter(AprioriMR.class.getName(), "numCandidates").increment(1);
                context.write(key, result);
                resultMap.put(new Text(key), new IntWritable(sum));
            }
        }
    }

    // 메인 함수
    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]); // 입력 경로
        Path outputPath = new Path(args[1]); // 출력 경로
        minsup = Integer.parseInt(args[2]); // 지지도 임계값

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
                    isDone = true; // 후보가 없으면 알고리즘 종료
                }
            }
        }
        
     // MapReduce 작업이 끝난 후에 각 출력 파일의 내용을 순회하고 합치는 작업을 수행합니다.
        Path resultFile = new Path(outputPath, "final_result");
        FSDataOutputStream out = fs.create(resultFile);

        for (int i = 1; i <= iteration
        for (int i = 1; i <= iteration; i++) {
            Path iterOutputPath = new Path(outputPath, String.valueOf(i));
            FileStatus[] statuses = fs.listStatus(iterOutputPath);
            for (FileStatus status : statuses) {
                // 출력 파일 중 'part-r-'로 시작하는 파일만 선택합니다.
                if (status.getPath().getName().startsWith("part-r-")) {
                    FSDataInputStream in = fs.open(status.getPath());
                    // 선택한 파일의 내용을 최종 결과 파일로 복사합니다.
                    IOUtils.copyBytes(in, out, conf, false); 
                    in.close();
                }
            }
        }

        // 모든 복사 작업이 끝나면 출력 스트림을 닫습니다.
        out.close();
    }
}
