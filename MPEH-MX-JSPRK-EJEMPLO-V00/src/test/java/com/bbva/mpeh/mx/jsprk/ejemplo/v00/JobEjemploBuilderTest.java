package com.bbva.mpeh.mx.jsprk.ejemplo.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobEjemploBuilderTest {

    private JobEjemploBuilder jobEjemploBuilder;

    @BeforeEach
    void setUp() {
        this.jobEjemploBuilder = new JobEjemploBuilder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobEjemploBuilder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(1, sourcesList.getSources().size());

        final Source source = sourcesList.getSources().get(0);
        assertNotNull(source);
        assertEquals("micsv", source.getAlias());
        assertEquals("input/addresses.csv", source.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobEjemploBuilder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobEjemploBuilder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobEjemploBuilder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobEjemploBuilder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(1, targetsList.getTargets().size());

        final Target target = targetsList.getTargets().get(0);
        assertNotNull(target);
        assertEquals("micsvsalida", target.getAlias());
        assertEquals("output/micsvsalidafeliz.csv", target.getPhysicalName());
    }

}