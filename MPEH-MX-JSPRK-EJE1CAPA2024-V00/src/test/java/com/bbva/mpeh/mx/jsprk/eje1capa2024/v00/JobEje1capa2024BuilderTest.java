package com.bbva.mpeh.mx.jsprk.eje1capa2024.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobEje1capa2024BuilderTest {

    private JobEje1capa2024Builder jobEje1capa2024Builder;

    @BeforeEach
    void setUp() {
        this.jobEje1capa2024Builder = new JobEje1capa2024Builder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobEje1capa2024Builder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(2, sourcesList.getSources().size());

        final Source source = sourcesList.getSources().get(0);
        assertNotNull(source);
        assertEquals("bioStats", source.getAlias());
        assertEquals("biostats.csv", source.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobEje1capa2024Builder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobEje1capa2024Builder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobEje1capa2024Builder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobEje1capa2024Builder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(2, targetsList.getTargets().size());

        final Target temporaryOutput = targetsList.getTargets().get(0);
        assertNotNull(temporaryOutput);
        assertEquals("temporaryOutput", temporaryOutput.getAlias());
        assertEquals("output/temporaryOutput.csv", temporaryOutput.getPhysicalName());

        final Target finalOutput = targetsList.getTargets().get(1);
        assertNotNull(finalOutput);
        assertEquals("finalOutput", finalOutput.getAlias());
        assertEquals("output/finalOutput.csv", finalOutput.getPhysicalName());
    }

}