package com.bbva.mpeh.mx.jsprk.eje1capa2024.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import static com.bbva.mpeh.mx.jsprk.eje1capa2024.v00.model.Constants.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;


import java.lang.reflect.Array;
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

        Dataset<Row> longMoviesS = bioStatsJoinedGradesKey
                .filter(col("Weight").contains("min"))
                .select(col("*"),
                        split(col("Weight"), " ").getItem(0).as("duration_range")
                );
        Column[] ListCol = longMoviesS.columns();
        Dataset<Row> longMoviesSS = longMoviesS.select(col("*"),
                max("duration_range").over().as("max"))
                .select(ListCol.);

        datasetsToWrite.put("temporaryOutput", bioStatsJoinedGradesDs.toDF());
        datasetsToWrite.put("finalOutput", longMoviesSS.toDF());


        return datasetsToWrite;
    }

}