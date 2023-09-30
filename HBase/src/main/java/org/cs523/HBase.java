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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBase
{

    private static String hbaseZookeeper;
    private static String hbasePort;
    private static TableName tableName;

    private static List<String> hbaseColumnsFamily;
    public static void initiate() throws IOException {
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
        }
    }

    public static void insert(List<String> insertedData) throws IOException {
        if(hbaseZookeeper == null || hbasePort == null){
            throw new RuntimeException("Please initialize HBase module first");
        }
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseZookeeper);
        config.set("hbase.zookeeper.property.clientPort", hbasePort);
        try (Connection connection = ConnectionFactory.createConnection(config); Table table = connection.getTable(tableName)) {
            List<Put> rows = new ArrayList<>();
            for(String currentRow: insertedData){
                String[] columns = currentRow.split("\t");

                Put put = new Put(columns[0].getBytes());
                //TODO:: decide table schema
                put.addColumn(hbaseColumnsFamily.get(0).getBytes(), Bytes.toBytes("name"), columns[1].getBytes());
                rows.add(put);
            }
            table.put(rows);
        }
    }
}