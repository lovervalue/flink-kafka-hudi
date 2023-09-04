package com.lt.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class HiveTest {

    private final static Logger logger = LoggerFactory.getLogger(HiveTest.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));

        HiveCatalog catalog = new HiveCatalog(
                "myhive",
                "db_test_tmp",
                "/usr/local/service/hive/conf"
        );
        tableEnv.registerCatalog("myhive", catalog);
        tableEnv.useCatalog("myhive");


        tableEnv.executeSql("DROP TABLE IF EXISTS ods.test_kafka_source");
        tableEnv.executeSql("CREATE TABLE ods.test_kafka_source (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'hive_test',\n" +
                " 'properties.bootstrap.servers' = '172.24.11.56:9092',\n" +
                " 'properties.group.id' = 'group_test_01',\n" +
                " 'format' = 'json',\n"+
                " 'is_generic' = 'false',\n"+
                " 'scan.startup.mode' = 'latest-offset')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.test_hive_sink");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  ods.test_hive_sink (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT\n" +
                ") STORED AS PARQUET\n" +
                "TBLPROPERTIES (\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file'\n" +
                ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO ods.test_hive_sink SELECT id,name,age FROM ods.test_kafka_source");
    }
}