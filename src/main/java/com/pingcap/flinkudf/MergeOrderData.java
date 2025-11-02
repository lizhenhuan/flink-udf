package com.pingcap.flinkudf;

import org.apache.flink.table.functions.ScalarFunction;
import java.util.*;

public class MergeOrderData extends ScalarFunction {
    
    public String eval(String newOrderDate, String oldOrderDate) {
        // 解析输入参数
        List<String> newDates = parseStringArray(newOrderDate);
        List<String> oldDates = parseStringArray(oldOrderDate);
        
        // 合并所有日期
        List<String> allDates = new ArrayList<>();
        allDates.addAll(newDates);
        allDates.addAll(oldDates);
        
        // 去重并排序
        List<String> mergedDates = new ArrayList<>(new HashSet<>(allDates));
        Collections.sort(mergedDates);
        
        // 格式化输出
        return formatStringArray(mergedDates);
    }
    
    // 解析字符串数组
    private List<String> parseStringArray(String arrayStr) {
        List<String> result = new ArrayList<>();
        if (arrayStr == null || arrayStr.isEmpty() || arrayStr.equals("[]")) {
            return result;
        }
        
        String cleanStr = arrayStr.replace("[", "").replace("]", "").replace("\"", "");
        String[] parts = cleanStr.split(",");
        
        for (String part : parts) {
            if (!part.trim().isEmpty()) {
                result.add(part.trim());
            }
        }
        
        return result;
    }
    
    // 格式化字符串数组输出
    private String formatStringArray(List<String> list) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(list.get(i)).append("\"");
        }
        sb.append("]");
        return sb.toString();
    }
}