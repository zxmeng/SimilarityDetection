package org.NearDuplicateDetection;

import java.io.IOException;
import java.util.*;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

// import tl.lin.data.pair.PairOfStringLong;
// import tl.lin.data.array.ArrayListOfLongsWritable;


public class Cluster extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(Cluster.class);

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text KEY = new Text();
        private static final Text VALUE = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split("]");
            KEY.set(tokens[0]);
            VALUE.set(tokens[1].split(",")[0]);
            context.write(KEY, VALUE);
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();

            if (iter.hasNext()) {
                Text KEY = new Text(iter.next());
                while (iter.hasNext()) {
                    Text VALUE = new Text(iter.next());
                    context.write(KEY, VALUE);
                    context.write(VALUE, KEY);
                }
            }
        }
    }

    private static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text KEY = new Text();
        private static final Text VALUE = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().trim().split("\\s+");
            KEY.set(tokens[0]);
            VALUE.set(tokens[1]);
            context.write(KEY, VALUE);
        }
    }

    private static class MyReducer2 extends Reducer<Text, Text, Text, LongWritable> {
        private static final Text KEY = new Text();
        private static final LongWritable VALUE = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> sentences = new HashSet<>();
            sentences.add(key.toString());

            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                sentences.add(iter.next().toString());
            }

            String id = "";
            for (String s : sentences) {
                id += ", " + s;
            }
            KEY.set(id);
            VALUE.set(sentences.size());
            context.write(KEY, VALUE);
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private Cluster() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;
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

        LOG.info("Tool name: " + Cluster.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(Cluster.class.getSimpleName());
        job.setJarByClass(Cluster.class);

        job.setNumReduceTasks(args.numReducers);

        job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output + "_mid"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output + "_mid");
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        // =============================

        Job job2 = Job.getInstance(getConf());
        job2.setJobName(Cluster.class.getSimpleName());
        job2.setJarByClass(Cluster.class);

        job2.setNumReduceTasks(args.numReducers);

        job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        FileInputFormat.setInputPaths(job2, new Path(args.output + "_mid"));
        FileOutputFormat.setOutputPath(job2, new Path(args.output));

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        // Delete the output directory if it exists already.
        Path outputDir2 = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir2, true);

        long startTime2 = System.currentTimeMillis();
        job2.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");

        FileSystem.get(getConf()).delete(outputDir, true);
        return 0;
    }

    /**
    * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
    *
    * @param args command-line arguments
    * @throws Exception if tool encounters an exception
    */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Cluster(), args);
    }

}