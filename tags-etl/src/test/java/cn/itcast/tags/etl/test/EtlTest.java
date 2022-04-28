package cn.itcast.tags.etl.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class EtlTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn");
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes("htb_tags"));
        byte[] name = tableDescriptor.getName();
        System.out.println(new String(name));
        HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor d : columnFamilies) {
            System.out.println(d.getNameAsString());
        }
    }
}