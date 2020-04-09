package com.atguigu.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义 UDTF 函数
 */
public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        //设置输出参数名称和类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldsType = new ArrayList<>();
        fieldNames.add("event_name");
        fieldsType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldsType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);


        StandardStructObjectInspector standardStructObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsType);
        return standardStructObjectInspector;
    }

    /**
     *
     * @param objects 支持多进多出
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {

        String input = objects[0].toString();

        //判断
        if (StringUtils.isBlank(input)){
            return;
        } else {
            //避免影响其他事件输出，此处try catch
            try {
                JSONArray jsonArray = new JSONArray(input);
                //循环遍历事件
                for (int i = 0; i < jsonArray.length(); i++) {

                    String[] results = new String[2];
                    //获取事件名称
                    results[0] = jsonArray.getJSONObject(i).getString("en");
                    //获取事件整体Json字符串
                    results[1] = jsonArray.getString(i);

                    //将结果返回
                    forward(results);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
