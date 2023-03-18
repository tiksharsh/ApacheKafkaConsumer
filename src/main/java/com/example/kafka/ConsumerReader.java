package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import org.junit.*;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ConsumerReader {

//    static Configuration conf = HBaseConfiguration.create();
    public static void main(String[] args) throws SQLException {

        try {
            Class.forName("com.mysql.jdbc.Driver");
//            final String DB_URL = "dbc:mysql://quickstart.cloudera:3306/harsh_db";
            final String DB_URL = "jdbc:mysql://ms.itversity.com:3306/retail_export";
            final String USER = "retail_user";
            final String PASS = "itversity";

            Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
            Statement stmt = conn.createStatement();

            if (conn != null) {
                System.out.println("Connected to the database test1");
            }

//            conf.set("hbase.zookeeper.quorum", "m01.itversity.com,nn02.itversity.com,nn01.itversity.com");
//            conf.set("hbase.zookeeper.property.clientPort", "2181");
//            Connection connection = ConnectionFactory.createConnection(conf);

            Logger logger = LoggerFactory.getLogger(ConsumerReader.class.getName());

            Properties conProp = new Properties();
            conProp.put(ConsumerConfig.CLIENT_ID_CONFIG, ConstantConfigurations.appID);
            conProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantConfigurations.bootStrapServer);
            conProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            conProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            conProp.put(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER_GROUP1");
            conProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Step  2 - Set object of the kafka Consumer

            KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(conProp);
            consumer.subscribe(Arrays.asList("new_topic9"));

            while (true) {

                ConsumerRecords<Integer, String> records = consumer.poll(100);
                for (ConsumerRecord<Integer, String> record : records) {

//                logger.info("Key: "+ record.key() + ", Value:" +record.value());
//                logger.info("Partition:" + record.partition()+",Offset:"+record.offset());
                    System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                    String[] result2 = record.value().split(",");
                   System.out.println("Array Length " +result2.length);
//
                    Integer key = record.key();
                    String a = result2[0];
                    System.out.println(a);
                    String b = result2[1];
                    String c = result2[2];
                    String d = result2[3];
                    String e = result2[4];

                    // Execute a query
                System.out.println("Inserting records into the table !!");
                String sql = "insert into data_h4 values ('"+key+"','"+a+"', '"+b+"', '"+c+"', '"+d+"', '"+e+"')";
                stmt.executeUpdate(sql);
                System.out.println("Updated Successfully !!!");

                }
            }
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
