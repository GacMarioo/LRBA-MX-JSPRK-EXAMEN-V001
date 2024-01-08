package com.bbva.mpeh.mx.jsprk.eje1capa2024.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
//import com.bbva.mpeh.mx.jsprk.eje1capa2024.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.sys.SystemProperties;
import uk.org.webcompere.systemstubs.stream.SystemErrAndOut;
import uk.org.webcompere.systemstubs.stream.SystemOut;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bbva.mpeh.mx.jsprk.eje1capa2024.v00.model.Constants.*;
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
        StructType schemaBioStats = DataTypes.createStructType(
               new StructField[]{

                         DataTypes.createStructField("SerialNumber", DataTypes.StringType, false),
                         DataTypes.createStructField("Name", DataTypes.StringType, false),
                         DataTypes.createStructField("Sex", DataTypes.StringType, false),
                         DataTypes.createStructField("Age", DataTypes.StringType, false),
                         DataTypes.createStructField("Height", DataTypes.StringType, false),
                         DataTypes.createStructField("Weight", DataTypes.StringType, false),
               });
        Row firstRowBioStats = RowFactory.create("123-45-6789","Alex","M","41","74","170 min");
        Row secondRowBioStats = RowFactory.create("123-12-1234","Bert","M","42","68","166 mins");
        Row thirdRowBioStats = RowFactory.create("567-89-0123","Carl","M","32","70","155 min");

        final List<Row> listRowsBioStats = Arrays.asList(firstRowBioStats, secondRowBioStats, thirdRowBioStats);

        DatasetUtils<Row> datasetUtilsBioStats = new DatasetUtils<>();
        Dataset<Row> bioStatsDf = datasetUtilsBioStats.createDataFrame(listRowsBioStats, schemaBioStats);

        StructType schemaGrades = DataTypes.createStructType(
                new StructField[]{

                        DataTypes.createStructField("SSN", DataTypes.StringType, false),
                        DataTypes.createStructField("LastName", DataTypes.StringType, false),
                        DataTypes.createStructField("FirstName", DataTypes.StringType, false),
                        DataTypes.createStructField("Test1", DataTypes.StringType, false),
                        DataTypes.createStructField("Test2", DataTypes.StringType, false),
                        DataTypes.createStructField("Test3", DataTypes.StringType, false),
                        DataTypes.createStructField("Test4", DataTypes.StringType, false),
                        DataTypes.createStructField("Final", DataTypes.StringType, false),
                        DataTypes.createStructField("Grade", DataTypes.StringType, false),
                });
        Row firstRowGrades = RowFactory.create("123-45-6789", "Alfalfa", "Aloysius", "40.0", "90.0", "100.0", "83.0","49.0", "D-");
        Row secondRowGrades = RowFactory.create("123-12-1234", "Alfred", "University", "41.0", "97.0", "96.0", "97.0","48.0", "D+");
        Row thirdRowGrades = RowFactory.create("567-89-0123", "Gerty", "Gramma", "41.0", "80.0", "60.0", "40.0","44.0", "C");

        final List<Row> listRowsGrades = Arrays.asList(firstRowGrades, secondRowGrades, thirdRowGrades);

        DatasetUtils<Row> datasetUtilsGrades = new DatasetUtils<>();
        Dataset<Row> gradesDf = datasetUtilsGrades.createDataFrame(listRowsGrades, schemaGrades);
        final Map<String, Dataset<Row>> dataset = this.transformer.transform(Map.of("bioStats", bioStatsDf, "grades", gradesDf));
            dataset.forEach((k, v) -> System.out.println(k + " - " + v));
        assertNotNull(dataset);
        assertEquals(2, dataset.size());

        Dataset<Row> temporaryOutputDs = dataset.get("temporaryOutput");
        final List<Row> temporaryOutput = temporaryOutputDs.collectAsList();
        assertEquals(3, temporaryOutput.size());
        assertEquals("123-45-6789", temporaryOutput.get(0).get(0));
        assertEquals("Alfalfa", temporaryOutput.get(0).get(1));
        assertEquals("Aloysius", temporaryOutput.get(0).get(2));
        assertEquals("40.0", temporaryOutput.get(0).get(3));
        assertEquals("90.0", temporaryOutput.get(0).get(4));
        assertEquals("100.0", temporaryOutput.get(0).get(5));
        assertEquals("83.0", temporaryOutput.get(0).get(6));
        assertEquals("49.0", temporaryOutput.get(0).get(7));
        assertEquals("D-", temporaryOutput.get(0).get(8));
        assertEquals("123-45-6789", temporaryOutput.get(0).get(9));
        assertEquals("Alex", temporaryOutput.get(0).get(10));
        assertEquals("M", temporaryOutput.get(0).get(11));
        assertEquals("41", temporaryOutput.get(0).get(12));
        assertEquals("74", temporaryOutput.get(0).get(13));
        assertEquals("170 min", temporaryOutput.get(0).get(14));

        Dataset<Row> finalOutputDs = dataset.get("finalOutput");
        finalOutputDs.show();

        final List<Row> finalOutput = finalOutputDs.collectAsList();
        assertEquals(3, finalOutput.size());
        assertEquals("Alfalfa", finalOutput.get(0).get(0));
        assertEquals("Aloysius", finalOutput.get(0).get(1));
        assertEquals("40.0", finalOutput.get(0).get(2));
        assertEquals("90.0", finalOutput.get(0).get(3));
        assertEquals("100.0", finalOutput.get(0).get(4));
        assertEquals("83.0", finalOutput.get(0).get(5));
        assertEquals("49.0", finalOutput.get(0).get(6));
        assertEquals("D-", finalOutput.get(0).get(7));
        assertEquals("123-45-6789", finalOutput.get(0).get(8));
        assertEquals("Alex", finalOutput.get(0).get(9));
        assertEquals("M", finalOutput.get(0).get(10));
        assertEquals("41", finalOutput.get(0).get(11));
        assertEquals("74", finalOutput.get(0).get(12));
        assertEquals("170 min", finalOutput.get(0).get(13));

    }

}