package com.bbva.mpeh.mx.jsprk.eje2capa2024.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import  com.bbva.mpeh.mx.jsprk.eje2capa2024.v00.model.Constants;

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
        StructType schemaURL = DataTypes.createStructType(
               new StructField[]{
                         DataTypes.createStructField("Index", DataTypes.StringType, false),
                         DataTypes.createStructField("TrainVideoCount", DataTypes.StringType, false),
                         DataTypes.createStructField("KnowledgeGraphId", DataTypes.StringType, false),
                         DataTypes.createStructField("Name", DataTypes.StringType, false),
                         DataTypes.createStructField("Section", DataTypes.StringType, false),
                         DataTypes.createStructField("WikiUrl", DataTypes.StringType, false)
               });

        Row firstRowURL = RowFactory.create("0","788288","/m/03bt1gh","Game","Games","https://en.wikipedia.org/wiki/Game");
        Row secondRowURL = RowFactory.create("1","539945","/m/01mw1","Video game","Games","https://en.wikipedia.org/wiki/Video_game");

        final List<Row> listRowsURL = Arrays.asList(firstRowURL, secondRowURL);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> datasetURL = datasetUtils.createDataFrame(listRowsURL, schemaURL);

        StructType schemaDesc = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("Index", DataTypes.StringType, false),
                        DataTypes.createStructField("TrainVideoCount", DataTypes.StringType, false),
                        DataTypes.createStructField("Vertical1", DataTypes.StringType, false),
                        DataTypes.createStructField("WikiDescription", DataTypes.StringType, false)
                });

        Row firstRowDesc = RowFactory.create("0","788288","Games","A game is structured form of play, usually undertaken for enjoyment and sometimes used as an educational tool. Games are distinct from work, which is usually carried out for remuneration, and from art, which is more often an expression of aesthetic or ideological elements. However, the distinction is not clear-cut, and many games are also considered to be work or art. Key components of games are goals, rules, challenge, and interaction. Games generally involve mental or physical stimulation, and often both. Many games help develop practical skills, serve as a form of exercise, or otherwise perform an educational, simulational, or psychological role. Attested as early as 2600 BC, games are a universal part of human experience and present in all cultures. The Royal Game of Ur, Senet, and Mancala are some of the oldest known games.");
        Row secondRowDesc = RowFactory.create("1","539945","Games","A video game is an electronic game that involves human or animal interaction with a user interface to generate visual feedback on a video device such as a TV screen or computer monitor. The word video in video game traditionally referred to a raster display device, but as of the 2000s, it implies any type of display device that can produce two- or three-dimensional images. Some theorists categorize video games as an art form, but this designation is controversial. The electronic systems used to play video games are known as platforms; examples of these are personal computers and video game consoles. These platforms range from large mainframe computers to small handheld computing devices. Specialized video games such as arcade games, in which the video game components are housed in a large, coin-operated chassis, while common in the 1980s in video arcades, have gradually declined in use due to the widespread availability of affordable home video game consoles and video games on desktop and laptop computers and smartphones. The input device used for games, the game controller, varies across platforms.");

        final List<Row> listRowsDesc = Arrays.asList(firstRowDesc, secondRowDesc);

        Dataset<Row> datasetDesc = datasetUtils.createDataFrame(listRowsDesc, schemaDesc);


        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("videosURLs", datasetURL, "videosDescripciones", datasetDesc)));

        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());

        Dataset<Row> returnedDs = datasetMap.get("archivo");
        final List<Row> rows = returnedDs.collectAsList();
        assertEquals(2, rows.size());
        assertEquals("Games", rows.get(0).get(0));
        assertEquals(Constants.DESCRIPTION_TEST, rows.get(0).get(1));
        assertEquals("0", rows.get(0).get(2));
        assertEquals("788288", rows.get(0).get(3));
        assertEquals("/m/03bt1gh", rows.get(0).get(4));
        assertEquals("Game", rows.get(0).get(5));
        assertEquals("Games", rows.get(0).get(6));
        assertEquals("https://en.wikipedia.org/wiki/Game", rows.get(0).get(7));
    }

}