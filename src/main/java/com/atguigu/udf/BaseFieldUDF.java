package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String jsonkeysString) {

        // 0 准备一个sb
        StringBuilder sb = new StringBuilder();

        // 1 切割jsonkeys  mid uid vc vn l sr os ar md
        String[] jsonkeys = jsonkeysString.split(",");

        // 2 处理line   服务器时间 | json
        String[] logContents = line.split("\\|");

        // 3 合法性校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }

        // 4 开始处理json
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(logContents[1]);
            // 获取cm里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");

            // 循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String fieldName = jsonkeys[i].trim();

                if (base.has(fieldName)){
                    sb.append(base.getString(fieldName)).append("\t");
                } else {
                    sb.append("").append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

}
