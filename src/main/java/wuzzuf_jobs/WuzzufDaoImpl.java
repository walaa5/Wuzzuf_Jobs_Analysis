package wuzzuf_jobs;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;
import org.springframework.stereotype.Service;

import java.awt.*;
import java.io.IOException;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.regexp_replace;

@Service
public class WuzzufDaoImpl implements WuzzufDao {

    private Dataset<Job> dataset;

    @Override
    public Dataset<Job> getDataset() {
        return dataset;
    }

    @Override
    public void setDataset(Dataset<Job> dataset) {
        this.dataset = dataset;
    }

    @SuppressWarnings("SpellCheckingInspection")
    public static Job rowToJob(Row row) {
        StructType schema = row.schema();
        int title = -1, company = -1, location = -1, type = -1, level = -1, yearsExp = -1, country = -1, skills = -1;
        String[] fieldNames = schema.fieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase("title")) {
                title = i;
            } else if (fieldNames[i].equalsIgnoreCase("company")) {
                company = i;
            } else if (fieldNames[i].equalsIgnoreCase("location")) {
                location = i;
            } else if (fieldNames[i].equalsIgnoreCase("type")) {
                type = i;
            } else if (fieldNames[i].equalsIgnoreCase("level")) {
                level = i;
            } else if (fieldNames[i].equalsIgnoreCase("yearsexp")) {
                yearsExp = i;
            } else if (fieldNames[i].equalsIgnoreCase("country")) {
                country = i;
            } else if (fieldNames[i].equalsIgnoreCase("skills")) {
                skills = i;
            }
        }
        return new Job(row.getString(title), row.getString(company), row.getString(location), row.getString(type), row.getString(level), row.getString(yearsExp), row.getString(country), row.getString(skills));
    }

    public static Dataset<Job> mapDataset(Dataset<Row> dataset) {
        return dataset.map(WuzzufDaoImpl::rowToJob, Encoders.bean(Job.class));
    }

    @Override
    public Dataset<Job> readDataset(String filename) {

        SparkSession.Builder builder = SparkSession.builder();
        builder.appName("WuzzufJobs");
        builder.master("local[*]");

        final SparkSession sparkSession = builder.getOrCreate();

        final DataFrameReader dataFrameReader = sparkSession.read();
        dataFrameReader.option("header", "true");

        dataset = mapDataset(dataFrameReader.csv(filename));

        return dataset;
    }

    @Override
    public StructType getStructure() {
        return dataset.schema();
    }

    @Override
    public Dataset<Row> getSummary() {
        return dataset.summary();
    }

    @Override
    public Dataset<Job> cleanDataset() {

        // remove " Yrs of Exp" from "YearsExp" column
        dataset = mapDataset(dataset.withColumn("yearsExp",
                regexp_replace(dataset.col("yearsExp"), " Yrs of Exp", "")));

        // drop jobs with "YearsExp" equal null
        dataset = dataset.where("yearsExp <> \"null\"");

        // remove duplicates
        dataset = dataset.dropDuplicates();

        return dataset;
    }

    @Override
    public Dataset<Row> jobsPerCompany() {

        dataset.createOrReplaceTempView("wuzzuf");

        return dataset.sqlContext().sql(
                "SELECT Company, COUNT(*) AS jobs_count " +
                        "FROM wuzzuf " +
                        "GROUP BY Company " +
                        "ORDER BY jobs_count DESC"
        );
    }

    @Override
    public Dataset<Row> mostPopularJobTitles() {

        dataset.createOrReplaceTempView("wuzzuf");

        return dataset.sqlContext().sql(
                "SELECT Title, COUNT(*) AS Count " +
                        "FROM wuzzuf " +
                        "GROUP BY Title " +
                        "ORDER BY Count DESC"
        );
    }

    @Override
    public Dataset<Row> mostPopularAreas() {

        dataset.createOrReplaceTempView("wuzzuf");

        return dataset.sqlContext().sql(
                "SELECT Location, COUNT(*) AS Count " +
                        "FROM wuzzuf " +
                        "GROUP BY Location " +
                        "ORDER BY Count DESC"
        );
    }

    @Override
    public Dataset<Row> getMostDemandedSkills() {

        dataset.createOrReplaceTempView("wuzzuf");
        Dataset<Row> skills_ds = dataset.sqlContext().sql("SELECT skills FROM wuzzuf");

        skills_ds.createOrReplaceTempView("SkillsView");
        return skills_ds.sqlContext().sql(
                "SELECT skill, count(skill) AS count " +
                        "FROM ( " +
                        "   SELECT EXPLODE(SPLIT(skills, ',')) AS skill " +
                        "   FROM SkillsView) " +
                        "GROUP BY skill " +
                        "ORDER BY count DESC "
        );
    }

    @Override
    public void displayPieChart(Dataset<Row> dataset, String title) throws IOException {

        PieChart pieChart = new PieChartBuilder().title(title).build();
        for (int i = 0; i < 5; i++) {
            Row job = dataset.collectAsList().get(i);
            pieChart.addSeries(job.getString(0), job.getLong(1));
        }
        pieChart.addSeries("Other", dataset.except(dataset.limit(5)).count());


        BitmapEncoder.saveBitmap(pieChart,
                "src/main/resources/static/img/" + title.replaceAll("\\s", "") + "PieChart",
                BitmapEncoder.BitmapFormat.JPG);    }

    @Override
    public void displayBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel) throws IOException {

        CategoryChart barChart = new CategoryChartBuilder().title(title).xAxisTitle(xLabel).yAxisTitle(yLabel).build();
        barChart.getStyler().setXAxisLabelRotation(45);
        barChart.addSeries(xLabel,
                dataset.limit(10).collectAsList().stream().map(job -> job.getString(0)).collect(Collectors.toList()),
                dataset.limit(10).collectAsList().stream().map(job -> job.getLong(1)).collect(Collectors.toList())
        );

        barChart.getStyler().setSeriesColors(new Color[]{new Color(24, 83, 74)});

        BitmapEncoder.saveBitmap(barChart,
                "src/main/resources/static/img/" + title.replaceAll("\\s", "") + "BarChart",
                BitmapEncoder.BitmapFormat.JPG);
    }

    public Dataset<Row> factorizeColumn(String column) {
        StringIndexer indexer = new StringIndexer().setInputCol(column).setOutputCol(column + "_index");
        return indexer.fit(dataset).transform(dataset);
    }

}
