package com.bbva.mpeh.mx.jsprk.eje2capa2024.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobEje2capa2024BuilderTest {

    private JobEje2capa2024Builder jobEje2capa2024Builder;

    @BeforeEach
    void setUp() {
        this.jobEje2capa2024Builder = new JobEje2capa2024Builder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobEje2capa2024Builder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(2, sourcesList.getSources().size());

        final Source sourceDesc = sourcesList.getSources().get(0);
        assertNotNull(sourceDesc);
        assertEquals("videosDescripciones", sourceDesc.getAlias());
        assertEquals("VideosDescripciones.csv", sourceDesc.getPhysicalName());

        final Source sourceURL = sourcesList.getSources().get(1);
        assertNotNull(sourceURL);
        assertEquals("videosURLs", sourceURL.getAlias());
        assertEquals("VideosURLs.csv", sourceURL.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobEje2capa2024Builder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobEje2capa2024Builder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobEje2capa2024Builder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobEje2capa2024Builder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(1, targetsList.getTargets().size());

        final Target target = targetsList.getTargets().get(0);
        assertNotNull(target);
        assertEquals("archivo", target.getAlias());
        assertEquals("output/archivo.csv", target.getPhysicalName());
    }

}