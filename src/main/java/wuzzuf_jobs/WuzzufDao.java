package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public interface WuzzufDao {

    Dataset<Job> getDataset();

    void setDataset(Dataset<Job> dataset);

    Dataset<Job> readDataset(String filename);

    StructType getStructure();

    Dataset<Row> getSummary();

    Dataset<Job> cleanDataset();

    Dataset<Row> jobsPerCompany();

    Dataset<Row> mostPopularJobTitles();

    Dataset<Row> mostPopularAreas();

    Dataset<Row> getMostDemandedSkills();

    void displayPieChart(Dataset<Row> dataset, String title) throws IOException;

    void displayBarChart(Dataset<Row> dataset, String title, String xLabel, String yLabel) throws IOException;
}