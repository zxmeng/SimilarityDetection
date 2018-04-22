package org.NearDuplicateDetection;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import org.NearDuplicateDetection.MultiplyShiftHash;
import tl.lin.data.array.ArrayListOfLongsWritable;


public class MinHash extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(MinHash.class);

    private static class MyMapper extends Mapper<LongWritable, Text, ArrayListOfLongsWritable, Text> {
        private int numHashes = 20;
        private int numHashBits = 60;
        private int sigLen = 10;
        private int draw = 10;
        private int shingleLen = 12;

        private long randSeed = 112345;
        private long seeds[];
        private long sigSeed;

        private int minLen = 75;
        private int maxLen = 600;

        private static final ArrayListOfLongsWritable SIGNATURE = new ArrayListOfLongsWritable();
        private static final Text SENTENCE = new Text();
        
        private static MultiplyShiftHash hashFamily;
        private static long MINHASH[];

        //Adapted from http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence
        private static final Pattern sentenceRegex = Pattern.compile(
                "# Match a sentence ending in punctuation or EOS.\n" +
                        "[\\s]*    # Leading white space\n" + 
                        "([A-Z\"]    # First char capital letter or quotation\n" +
                        "[^.!?]*      # Greedily consume up to punctuation.\n" +
                        "(?:          # Group for unrolling the loop.\n" +
                        "  [.!?]      # (special) inner punctuation ok if\n" +
                        "  (?!['\"]?\\s|$)  # not followed by ws or EOS.\n" +
                        "  [^.!?]*    # Greedily consume up to punctuation.\n" +
                        ")*           # Zero or more (special normal*)\n" +
                        "[.!?]?       # Optional ending punctuation.\n" +
                        "['\"]?)       # Optional closing quote.\n" +
                        "\\s*$?       # Trailing white space\n",
                        Pattern.MULTILINE | Pattern.COMMENTS);

        @Override
        public void setup(Context context) {
            numHashes = context.getConfiguration().getInt("numHashes", 20);
            numHashBits = context.getConfiguration().getInt("numHashBits", 60);
            sigLen = context.getConfiguration().getInt("sigLen", 10);
            draw = context.getConfiguration().getInt("draw", 10);
            shingleLen = context.getConfiguration().getInt("shingleLen", 12);
            randSeed = context.getConfiguration().getLong("randSeed", 112345);
            minLen = context.getConfiguration().getInt("minLen", 75);
            maxLen = context.getConfiguration().getInt("maxLen", 600);

            seeds = new long[numHashes];
            Random r = new Random(randSeed);
            for (int i = 0; i < numHashes; i++) {
                seeds[i] = r.nextLong();
            }
            sigSeed = r.nextLong();
            hashFamily = new MultiplyShiftHash(numHashBits, seeds);
            MINHASH = new long[numHashes];
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String docid = value.toString().split(",")[0];

            Matcher m = sentenceRegex.matcher(value.toString());
            int sentenceCount = 0;
            while (m.find()){
                for(int i = 0; i < numHashes; i++){
                    MINHASH[i] = Long.MAX_VALUE;
                }

                String sentence = m.group(1);
                int shingleCount = sentence.length() - shingleLen + 1;
                if (shingleCount > minLen && shingleCount < maxLen) {

                    String hashValue[] = new String[numHashes];
                    for (int i = 0; i < shingleCount; i++){
                        String shingle = sentence.substring(i, i + shingleLen);
                        long hash[] = hashFamily.hash(shingle);

                        for (int j = 0; j < hash.length; j++){
                            if (hash[j] < MINHASH[j]){
                                MINHASH[j] = hash[j];
                                hashValue[j] = shingle;
                            }
                        }
                    }

                    // SENTENCE.set(sentence + " " + docid + ":" + sentenceCount);
                    SENTENCE.set(docid + ":" + sentenceCount);
                    Random r = new Random(sigSeed);
                    for (int j = 0; j < draw; j++){
                        SIGNATURE.clear();
                        for(int i = 0; i < sigLen; i++){
                            int x = r.nextInt(numHashes);
                            SIGNATURE.add(MINHASH[x]);
                        }
                        context.write(SIGNATURE, SENTENCE);
                    }
                }
                sentenceCount++;
            }
        }
    }


    /**
     * Emits groups of sentences that hash to the same value. Only emits if there is more than one value for the key. 
     *
     */
    private static class MyReducer extends Reducer<ArrayListOfLongsWritable, Text, ArrayListOfLongsWritable, Text> {

        @Override
        public void reduce(ArrayListOfLongsWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            boolean gt1 = false;
            while (iter.hasNext()) {
                Text val = iter.next();
                if(iter.hasNext()) gt1 = true;
                if(gt1) context.write(key, val);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private MinHash() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-hashfuncs", metaVar = "[num]", usage = "number of hash functions")
        int numHashes = 20;

        @Option(name = "-hashbits", metaVar = "[num]", usage = "number of hash bits")
        int numHashBits = 30;

        @Option(name = "-siglen", metaVar = "[num]", usage = "length of signature")
        int sigLen = 8;

        @Option(name = "-draw", metaVar = "[num]", usage = "draw times")
        int draw = 5;

        @Option(name = "-shingle", metaVar = "[num]", usage = "length of shingle")
        int shingleLen = 15;

        @Option(name = "-rseed", metaVar = "[num]", usage = "random seed")
        long randSeed = 1123456;

        @Option(name = "-min", metaVar = "[num]", usage = "min length of sentence")
        int minLen = 20;

        @Option(name = "-max", metaVar = "[num]", usage = "max length of sentence")
        int maxLen = 600;
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

        LOG.info("Tool name: " + MinHash.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);
        LOG.info(" - num hash functions: " + args.numHashes);
        LOG.info(" - num hash bits: " + args.numHashBits);
        LOG.info(" - signature length: " + args.sigLen);
        LOG.info(" - draw times: " + args.draw);
        LOG.info(" - shingle length: " + args.shingleLen);
        LOG.info(" - random seed: " + args.randSeed);
        LOG.info(" - min length: " + args.minLen);
        LOG.info(" - max length: " + args.maxLen);

        Job job = Job.getInstance(getConf());
        job.setJobName(MinHash.class.getSimpleName());
        job.setJarByClass(MinHash.class);

        job.setNumReduceTasks(args.numReducers);
        job.getConfiguration().setInt("numHashes", args.numHashes);
        job.getConfiguration().setInt("numHashBits", args.numHashBits);
        job.getConfiguration().setInt("sigLen", args.sigLen);
        job.getConfiguration().setInt("draw", args.draw);
        job.getConfiguration().setInt("shingleLen", args.shingleLen);
        job.getConfiguration().setLong("randSeed", args.randSeed);
        job.getConfiguration().setInt("minLen", args.minLen);
        job.getConfiguration().setInt("maxLen", args.maxLen);

        job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(ArrayListOfLongsWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(ArrayListOfLongsWritable.class);
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
        ToolRunner.run(new MinHash(), args);
    }

}