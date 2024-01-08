package com.bbva.mpeh.mx.jsprk.eje1capa2024.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static com.bbva.mpeh.mx.jsprk.eje1capa2024.v00.model.Constants.*;


import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> bioStatsDs = datasetsFromRead.get("bioStats");
        Dataset<Row> gradesDs = datasetsFromRead.get("grades");
        Dataset<Row> bioStatsJoinedGradesDs = gradesDs.join(bioStatsDs,
                bioStatsDs.col(SERIAL_NUMBER_COLUMN)
                        .equalTo(gradesDs.col(ID_COLUMN)), JOIN_TYPE);
        Dataset<Row> bioStatsJoinedGradesKey = bioStatsJoinedGradesDs.drop(gradesDs.col(ID_COLUMN));

        datasetsToWrite.put("temporaryOutput", bioStatsJoinedGradesDs.toDF());
        datasetsToWrite.put("finalOutput", bioStatsJoinedGradesKey.toDF());

        return datasetsToWrite;
    }

}