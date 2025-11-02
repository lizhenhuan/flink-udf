package com.pingcap.flinkudf;

import org.apache.flink.table.functions.ScalarFunction;
import java.util.*;

public class MergeOrderTimes extends ScalarFunction {
    
    public String eval(String newOrderDate, String newOrderTimes, 
                      String oldOrderDate, String oldOrderTimes) {
        // 解析输入参数
        List<String> newDates = parseStringArray(newOrderDate);
        List<Integer> newTimes = parseIntArray(newOrderTimes);
        List<String> oldDates = parseStringArray(oldOrderDate);
        List<Integer> oldTimes = parseIntArray(oldOrderTimes);
        
        // 创建合并后的映射
        Map<String, Integer> mergedMap = new HashMap<>();
        
        // 合并新数据
        for (int i = 0; i < newDates.size(); i++) {
            String date = newDates.get(i);
            int times = newTimes.get(i);
            mergedMap.put(date, mergedMap.getOrDefault(date, 0) + times);
        }
        
        // 合并老数据
        for (int i = 0; i < oldDates.size(); i++) {
            String date = oldDates.get(i);
            int times = oldTimes.get(i);
            mergedMap.put(date, mergedMap.getOrDefault(date, 0) + times);
        }
        
        // 获取所有日期并按日期排序
        List<String> allDates = new ArrayList<>();
        allDates.addAll(newDates);
        allDates.addAll(oldDates);
        
        // 去重并排序
        List<String> sortedDates = new ArrayList<>(new HashSet<>(allDates));
        Collections.sort(sortedDates);
        
        // 构建合并后的次数数组
        List<Integer> resultTimes = new ArrayList<>();
        for (String date : sortedDates) {
            resultTimes.add(mergedMap.get(date));
        }
        
        // 格式化输出
        return formatIntArray(resultTimes);
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
    
    // 解析整数数组
    private List<Integer> parseIntArray(String arrayStr) {
        List<Integer> result = new ArrayList<>();
        if (arrayStr == null || arrayStr.isEmpty() || arrayStr.equals("[]")) {
            return result;
        }
        
        String cleanStr = arrayStr.replace("[", "").replace("]", "");
        String[] parts = cleanStr.split(",");
        
        for (String part : parts) {
            if (!part.trim().isEmpty()) {
                result.add(Integer.parseInt(part.trim()));
            }
        }
        
        return result;
    }
    
    // 格式化整数数组输出
    private String formatIntArray(List<Integer> list) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(list.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}