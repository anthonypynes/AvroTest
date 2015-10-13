import com.wpengine.data.test.SimpleRecord;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

public class AvroTestMain {
    private static final Logger logger = Logger.getLogger(AvroTestMain.class);

    public static SimpleRecord[] generateSimpleRecordsForTest(int count) {
        SimpleRecord[] simpleRecords = new SimpleRecord[count];
        for (int i = 0; i < count; i++) {
            simpleRecords[i] = new SimpleRecord("bar" + i, i);
        }
        return simpleRecords;
    }

    private static <T> void serializableWrite(Configuration configuration, String outputPath, Schema schema,
                                              JavaRDD<T> javaRDD)
            throws IOException {
        final Job job = Job.getInstance(configuration);
        AvroJob.setOutputKeySchema(job, schema);
        javaRDD.mapToPair(new PairFunction<T, AvroKey<T>, NullWritable>() {
            public Tuple2<AvroKey<T>, NullWritable> call(T t) {
                return new Tuple2<AvroKey<T>, NullWritable>(new AvroKey<T>(t), NullWritable.get());
            }
        }).saveAsNewAPIHadoopFile(
                outputPath,
                AvroKey.class,
                NullWritable.class,
                AvroKeyOutputFormat.class,
                job.getConfiguration()
        );
    }

    public static void main(String[] args) throws Exception {
        final SparkConf sparkConf = new SparkConf().setAppName(AvroTestMain.class.getSimpleName());
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SimpleRecord[] records = generateSimpleRecordsForTest(5);
        final Path path = new Path("s3://test.data.wpengine.com/test/output.avro");
        JavaRDD<SimpleRecord> simpleRecordJavaRDD = javaSparkContext.parallelize(Arrays.asList(records));
        serializableWrite(javaSparkContext.hadoopConfiguration(), path.toUri().toString(), SimpleRecord.getClassSchema(),
                simpleRecordJavaRDD);

        javaSparkContext.stop();
    }
}
