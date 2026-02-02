package com.example;

import org.apache.flink.table.api.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class DynamicFlinkJob {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: flink run -c com.example.DynamicFlinkJob your-job.jar <config-file-path>");
            return;
        }

        String configPath = args[0];

        Map<String, StringBuilder> sections = new HashMap<>();
        sections.put("source", new StringBuilder());
        sections.put("sink", new StringBuilder());
        sections.put("insert", new StringBuilder());

        Path path = new Path(configPath);
        FileSystem fs = path.getFileSystem();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            String current = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("[") && line.endsWith("]")) {
                    current = line.substring(1, line.length() - 1).toLowerCase();
                } else if (current != null) {
                    sections.get(current).append(line).append("\n");
                }
            }
        }

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // TODO: tạo bảng source từ config
        String sourceSql = sections.get("source").toString(); // từ config
        tEnv.executeSql(sourceSql);

        // TODO: tạo bảng sink từ config
        String sinkSql = sections.get("sink").toString(); // từ config
        tEnv.executeSql(sinkSql);

        // TODO: insert/transform
        String insertSql = sections.get("insert").toString(); // từ config
        tEnv.executeSql(insertSql);
    }
}
