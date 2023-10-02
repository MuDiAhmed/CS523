package org.cs523;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.ForeachWriter;

import java.io.IOException;
import java.util.List;

public class HBase extends ForeachWriter<JobPost>
{

    private static String hbaseZookeeper;
    private static String hbasePort;
    private static TableName tableName;
    private static List<String> hbaseColumnsFamily;
    private Table table;
    private Connection connection;

    @Override
    public boolean open(long l, long l1) {
        Config applicationConf = ConfigFactory.parseResources("application.conf").resolve();
        hbaseZookeeper = applicationConf.getString("hbase.zookeeper.quorum");
        hbasePort = applicationConf.getString("hbase.zookeeper.clientPort");
        String hbaseTableName = applicationConf.getString("hbase.table.name");
        boolean hbaseIsDrop = applicationConf.getBoolean("hbase.table.drop");
        boolean hbaseIsCreate = applicationConf.getBoolean("hbase.table.create");
        hbaseColumnsFamily = applicationConf.getStringList("hbase.table.columnsFamily");
        tableName = TableName.valueOf(hbaseTableName);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseZookeeper);
        config.set("hbase.zookeeper.property.clientPort", hbasePort);

        try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            hbaseColumnsFamily.forEach(column  -> descriptor.addFamily(new HColumnDescriptor(column)));

            if (admin.tableExists(descriptor.getTableName()) && hbaseIsDrop)
            {
                admin.disableTable(descriptor.getTableName());
                admin.deleteTable(descriptor.getTableName());
            }

            if(hbaseIsCreate){
                admin.createTable(descriptor);
            }
            table = connection.getTable(tableName);
            this.connection = connection;
            return true;
        } catch (IOException e) {
//            throw new RuntimeException(e);
            return false;
        }

    }

    @Override
    public void process(JobPost jobPost) {

        Put put = new Put(jobPost.getId().getBytes());
        //TODO:: decide table schema
        put.addColumn(hbaseColumnsFamily.get(0).getBytes(), Bytes.toBytes("title"), jobPost.getTitle().getBytes());
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(Throwable throwable) {
        try {
            this.table.close();
            this.connection.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
