package com.bbva.mpeh.mx.jsprk.ejemplo.v00;

import com.bbva.lrba.spark.transformers.Transform;
//import com.bbva.mpeh.mx.jsprk.ejemplo.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset = datasetsFromRead.get("micsv").as(Encoders.bean(Row.class));

        datasetsToWrite.put("micsvsalida", dataset.toDF());

        return datasetsToWrite;
    }

}