package com.lordjoe.distributed.database;

import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.hydra.peptide.PeptideSchemaBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.systemsbiology.hadoop.HDFSAccessor;
import org.systemsbiology.xtandem.peptide.IPolypeptide;

import java.io.IOException;

/**
 * com.lordjoe.distributed.database.DatabaseUtilities
 * User: Steve
 * Date: 10/20/2014
 */
public class DatabaseUtilities {

    public static <K> void buildParaquetDatabase( String name,JavaRDD  data,Class  bean) {
        try {
            JavaSparkContext sc = SparkUtilities.getCurrentContext();

            SparkSession currentSession = SparkUtilities.getCurrentSession();
       // Old Code     SQLContext sqlContext = SparkUtilities.getCurrentSQLContext();

               // Apply a schema to an RDD of JavaBeans and register it as a table.
         //     sqlContext.createDataset(data,bean)
            Dataset<Row> frame = currentSession.createDataFrame(data.rdd(), bean);
            // Old Code       Dataset<Row> frame = sqlContext.applySchema(data, bean);

            Configuration conf = sc.hadoopConfiguration();
             FileSystem fs = FileSystem.get(conf);
             HDFSAccessor accessor = new HDFSAccessor(fs);
             accessor.expunge(name);

            //List collect = data.collect();
            // JavaSchemaRDDs can be saved as Parquet files, maintaining the schema information.
             frame.write().parquet(name);
           //       frame.saveAsParquetFile(name);
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public static JavaRDD<IPolypeptide> readParquetDatabase(String name) {
        JavaSparkContext sc = SparkUtilities.getCurrentContext();
        // old Code     SQLContext sqlContext = SparkUtilities.getCurrentSQLContext();
        SparkSession currentSession = SparkUtilities.getCurrentSession();

        Dataset<Row> load  = currentSession.readStream().load(name);
        // Apply a schema to an RDD of JavaBeans and register it as a table.
    // old Code    final Dataset<Row> load = sqlContext.read().load(name);
        JavaRDD<PeptideSchemaBean> beansRDD = load.toJavaRDD().map(PeptideSchemaBean.FROM_ROW);
        JavaRDD<IPolypeptide> peptides = beansRDD.map(PeptideSchemaBean.FROM_BEAN);
        return peptides;
    }

    public static void removeParaquetDatabase( String name) {
        try {
            JavaSparkContext sc = SparkUtilities.getCurrentContext();
            Configuration conf = sc.hadoopConfiguration();
            FileSystem fs = FileSystem.get(conf);
            HDFSAccessor accessor = new HDFSAccessor(fs);
            accessor.expunge(name);
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public static<K  extends IDatabaseBean> void guaranteeParaquetDatabase( String name,JavaRDD<K> data,Class<? extends K> bean) {
        try {
            JavaSparkContext sc = SparkUtilities.getCurrentContext();
            Configuration conf = sc.hadoopConfiguration();
            FileSystem fs = FileSystem.get(conf);
            HDFSAccessor accessor = new HDFSAccessor(fs);
             if(accessor.exists(name))
                return; // already built

            buildParaquetDatabase(name,data,bean);
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public static boolean isParquetDatabaseExist(String name) {
        boolean result = false;
        try {
            JavaSparkContext sc = SparkUtilities.getCurrentContext();
            Configuration conf = sc.hadoopConfiguration();
            FileSystem fs = FileSystem.get(conf);
            HDFSAccessor accessor = new HDFSAccessor(fs);
            if(accessor.exists(name))
                result = true;
        }
        catch (IOException e) {
            return result;
        }
        return result;
    }
}
