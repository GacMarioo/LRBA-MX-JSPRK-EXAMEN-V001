package com.bbva.mpeh.mx.jsprk.eje2capa2024.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.bbva.mpeh.mx.jsprk.eje2capa2024.v00.model.Constants;

import java.util.*;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> videosDescripcionesDs = datasetsFromRead.get("videosDescripciones");
        Dataset<Row> videosURLsDs = datasetsFromRead.get("videosURLs");
        Dataset<Row> videosDescripcionesDsKey = videosDescripcionesDs.withColumnRenamed(Constants.ID_COLUMN,Constants.ID_SECOND_COLUMN)
                .withColumnRenamed(Constants.ID_VIDEO_COUNT_COLUMN,Constants.ID_VIDEO_COUNT_SECOND_COLUMN);

        Dataset<Row> descriptionsJoinedURLs = videosDescripcionesDsKey.join(videosURLsDs,
                (videosDescripcionesDsKey.col(Constants.ID_SECOND_COLUMN)
                        .equalTo(videosURLsDs.col(Constants.ID_COLUMN))).and
        (videosDescripcionesDsKey.col(Constants.ID_VIDEO_COUNT_SECOND_COLUMN)
                .equalTo(videosURLsDs.col(Constants.ID_VIDEO_COUNT_COLUMN))), Constants.JOIN_TYPE);
        Dataset<Row> selectFile  = descriptionsJoinedURLs.select(col("*"),
                concat(col("Index"), col("Name")).as(Constants.CODE_COLUMN),
                when(col("Section").isin(Constants.SECTION_GAME), 5).as("Rate"))
                .filter(col("Section").isNotNull())
                .drop(Constants.ID_SECOND_COLUMN, Constants.ID_VIDEO_COUNT_SECOND_COLUMN);



        datasetsToWrite.put("archivo", selectFile.toDF());

        return datasetsToWrite;
    }

}