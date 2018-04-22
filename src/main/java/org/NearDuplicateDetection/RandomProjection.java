package org.NearDuplicateDetection;

import java.io.*;
import java.util.*;
import java.lang.*;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;
import org.NearDuplicateDetection.GenerteRandomVectors;


public class RandomProjection extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RandomProjection.class);

    private static class MyMapper extends Mapper<LongWritable, Text, ArrayListOfIntsWritable, Text> {
        private int vecLen = 400;
        private int sigLen = 100;
        private int permNo = 10;

        private long randSeed = 1123456;
        private long seeds[];
        private ArrayList<ArrayList<Double>> randomVectors = new ArrayList<ArrayList<Double>>();

        private static final ArrayListOfIntsWritable SIGNATURE = new ArrayListOfIntsWritable();
        private static final Text SENTENCE = new Text();

        @Override
        public void setup(Context context) {
            vecLen = context.getConfiguration().getInt("vecLen", 400);
            sigLen = context.getConfiguration().getInt("sigLen", 100);
            permNo = context.getConfiguration().getInt("permNo", 10);
            randSeed = context.getConfiguration().getLong("randSeed", 112345);

            seeds = new long[sigLen];
            Random r = new Random(randSeed);
            for (int i = 0; i < sigLen; i++) {
                seeds[i] = r.nextLong();
            }
            randomVectors = new GenerteRandomVectors(vecLen, sigLen, seeds).getRandomVectors();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            String docId = Integer.toString(Float.valueOf(tokens[0]).intValue());
            ArrayList<Double> docVec = new ArrayList<>();

            for (int i = 0; i < vecLen; i++) {
                docVec.add(Double.parseDouble(tokens[i+1]));
            }

            SENTENCE.set(docId);
            Random r = new Random(randSeed);
            for (int j = 0; j < permNo; j++){
                SIGNATURE.clear();
                for(int i = 0; i < sigLen; i++){
                    double dp = 0;
                    for (int k = 0; k < vecLen; j++) {
                        dp += docVec.get(k) * randomVectors.get(i).get(k);
                    }
                    if (dp > 0) {
                        SIGNATURE.add(1);
                    } else {
                        SIGNATURE.add(0);
                    }  
                }
                context.write(SIGNATURE, SENTENCE);
                Collections.shuffle(docVec, r);
                }
            }
        }

    /**
     * Emits groups of sentences that hash to the same value. Only emits if there is more than one value for the key. 
     *
     */
    private static class MyReducer extends Reducer<ArrayListOfIntsWritable, Text, IntWritable, Text> {
        private static final IntWritable KEY = new IntWritable();
        private static Queue<ArrayListOfIntsWritable> sigs = new ArrayDeque<>();
        private static Queue<Text> docIds = new ArrayDeque<>();

        private int winSize = 10;
        private int threshold = 15;

        @Override
        public void setup(Context context) {
            winSize = context.getConfiguration().getInt("winSize", 10);
            threshold = context.getConfiguration().getInt("threshold", 15);
        }

        @Override
        public void reduce(ArrayListOfIntsWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                Text val = iter.next();
                Iterator<ArrayListOfIntsWritable> iterator = sigs.iterator();

                while (iterator.hasNext()) {
                    ArrayListOfIntsWritable sig = iterator.next();
                    int dist = 0;
                    for (int i = 0; i < sig.size(); i++) {
                        if (key.get(i) == sig.get(i)) {
                            dist++;
                        }
                        if (dist >= threshold) {
                            break;
                        }
                     }
                    if (dist < threshold) {
                        KEY.set(dist);
                        context.write(KEY, val);
                    }
                }

                if (sigs.size() >= winSize) {
                    sigs.remove();
                    docIds.remove();
                }

                docIds.add(val);
                sigs.add(key);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private RandomProjection() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-veclen", metaVar = "[num]", usage = "length of vector")
        int vecLen = 400;

        @Option(name = "-siglen", metaVar = "[num]", usage = "length of signature")
        int sigLen = 100;

        @Option(name = "-permutate", metaVar = "[num]", usage = "permutation times")
        int permNo = 10;

        @Option(name = "-threshold", metaVar = "[num]", usage = "distance threshold")
        int threshold = 15;

        @Option(name = "-window", metaVar = "[num]", usage = "window size")
        int winSize = 10;

        @Option(name = "-rseed", metaVar = "[num]", usage = "random seed")
        long randSeed = 1123456;

    }

    /**
     * Runs this tool.
     */
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool name: " + RandomProjection.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - vector length: " + args.vecLen);
        LOG.info(" - signature length: " + args.sigLen);
        LOG.info(" - permutation times: " + args.permNo);
        LOG.info(" - distance threshold: " + args.threshold);
        LOG.info(" - window size: " + args.winSize);
        LOG.info(" - random seed: " + args.randSeed);

        Job job = Job.getInstance(getConf());
        job.setJobName(RandomProjection.class.getSimpleName());
        job.setJarByClass(RandomProjection.class);

        job.setNumReduceTasks(args.numReducers);
        job.getConfiguration().setInt("vecLen", args.vecLen);
        job.getConfiguration().setInt("sigLen", args.sigLen);
        job.getConfiguration().setInt("permNo", args.permNo);
        job.getConfiguration().setInt("winSize", args.winSize);
        job.getConfiguration().setInt("threshold", args.threshold);
        job.getConfiguration().setLong("randSeed", args.randSeed);
        
        job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(ArrayListOfIntsWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        // job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        // job.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

        /**
        * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
        *
        * @param args command-line arguments
        * @throws Exception if tool encounters an exception
        */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RandomProjection(), args);
    }

}