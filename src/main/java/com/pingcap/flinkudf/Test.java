package com.pingcap.flinkudf;

public class Test {
    public static void main(String[] args) {
        MergeOrderTimes mergeOrderTimes = new MergeOrderTimes();
        String result = mergeOrderTimes.eval("[\"20250301\",\"20250501\"]","[1,2]","[\"20250301\",\"20250801\"]","[1,1]");
        System.out.println(result);

        MergeOrderData mergeOrderDate = new MergeOrderData();
        String resultDate = mergeOrderDate.eval("[\"20250301\",\"20250501\"]","[\"20250301\",\"20250801\"]");
        System.out.println(resultDate);
        resultDate = mergeOrderDate.eval("1001","1002");
        System.out.println(resultDate);

        resultDate = mergeOrderDate.eval("","1002");
        System.out.println(resultDate);


    }
}