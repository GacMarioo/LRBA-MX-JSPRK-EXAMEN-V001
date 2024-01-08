package com.bbva.mpeh.mx.jsprk.ejemplo.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.mpeh.mx.jsprk.ejemplo.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema = DataTypes.createStructType(
               new StructField[]{
                       DataTypes.createStructField("CAMPO1", DataTypes.StringType, false),
                       DataTypes.createStructField("CAMPO2", DataTypes.StringType, false),
                       DataTypes.createStructField("CAMPO3", DataTypes.StringType, false),
                       DataTypes.createStructField("CAMPO4", DataTypes.StringType, false),
                       DataTypes.createStructField("CAMPO5", DataTypes.StringType, false),
                       DataTypes.createStructField("CAMPO6", DataTypes.StringType, false),
               });
        Row firstRow = RowFactory.create("John","Doe","120 jefferson st.", "Riverside", "NJ", "08075");
        Row secondRow = RowFactory.create("Jack", "McGinnis", "220 hobo Av.", "Phila", "PA", "09119");
        Row thirdRow = RowFactory.create("John Da Man", "Repici", "120 Jefferson St.", "Riverside", "NJ", "08075");

        final List<Row> listRows = Arrays.asList(firstRow, secondRow, thirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("micsv", dataset)));
        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());

        Dataset<Row> returnedDs = datasetMap.get("micsvsalida");
        final List<Row> rows = returnedDs.collectAsList();

        assertEquals(3, rows.size());
        assertEquals("John", rows.get(0).get(0));
        assertEquals("Doe", rows.get(0).get(1));
        assertEquals("120 jefferson st.", rows.get(0).get(2));
        assertEquals("Riverside", rows.get(0).get(3));
        assertEquals("NJ", rows.get(0).get(4));
        assertEquals("08075", rows.get(0).get(5));
    }

}