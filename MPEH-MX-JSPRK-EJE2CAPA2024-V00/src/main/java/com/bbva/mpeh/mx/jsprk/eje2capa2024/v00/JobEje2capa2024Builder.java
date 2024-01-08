package com.bbva.mpeh.mx.jsprk.eje2capa2024.v00;

import com.bbva.lrba.builder.annotation.Builder;
import com.bbva.lrba.builder.spark.RegisterSparkBuilder;
import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;

@Builder
public class JobEje2capa2024Builder extends RegisterSparkBuilder {

    @Override
    public SourcesList registerSources() {
        //EXAMPLE WITH A LOCAL SOURCE FILE
        return SourcesList.builder()
                .add(Source.File.Csv.builder()
                        .alias("videosDescripciones")
                        .physicalName("VideosDescripciones.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM videosDescripciones")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("videosURLs")
                        .physicalName("VideosURLs.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM videosURLs")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

    @Override
    public TransformConfig registerTransform() {
        //IF YOU WANT TRANSFORM CLASS
        return TransformConfig.TransformClass.builder().transform(new Transformer()).build();
        //IF YOU WANT SQL TRANSFORM
        //return TransformConfig.SqlStatements.builder().addSql("targetAlias1", "SELECT CAMPO1 FROM sourceAlias1").build();
        //IF YOU DO NOT WANT TRANSFORM
        //return null;
    }

    @Override
    public TargetsList registerTargets() {
        //EXAMPLE WITH A LOCAL TARGET FILE
        return TargetsList.builder()
                .add(Target.File.Csv.builder()
                        .alias("archivo")
                        .physicalName("output/archivo.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

}