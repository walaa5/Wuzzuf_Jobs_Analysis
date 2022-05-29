package wuzzuf_jobs;

import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.util.stream.Collectors;

@SuppressWarnings("SpellCheckingInspection")
@Controller
public class WuzzufController {

    private final WuzzufDaoImpl wuzzufDao;

    @Autowired
    public WuzzufController(WuzzufDaoImpl wuzzufDao) {
        this.wuzzufDao = wuzzufDao;
        this.wuzzufDao.readDataset("src/main/resources/Wuzzuf_Jobs.csv");
    }

    public static String datasetToTable(Dataset<?> dataset) {
        String header = "<table class='w3-table-all'><tr><th>" +
                String.join("</th><th>", dataset.columns()) + "</th></tr>";
        String body = dataset.toDF().collectAsList().stream().map(row -> "<tr><td>" + row.mkString("</td><td>"))
                .collect(Collectors.joining("</td></tr>"));
        return header + body;
    }

    @GetMapping("/")
    public String home() {
        return "home";
    }

    @GetMapping("View_Dataset")
    public String ViewDataset(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.getDataset()));
        return "template";
    }

    @GetMapping("Dataset_Structure")
    public String DatasetStructure(Model model) {
        model.addAttribute("body",
                wuzzufDao.getStructure().treeString().replaceAll("root\n \\|-- ", "")
                        .replaceAll(" \\|-- ", "</br>"));
        return "template";
    }

    @GetMapping("Dataset_Summary")
    public String DatasetSummary(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.getSummary()));
        return "template";
    }

    @GetMapping("Clean_Dataset")
    public String CleanDataset(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.cleanDataset()));
        return "template";
    }

    @GetMapping("Most_Demanding_Companies")
    public String MostDemandingCompanies(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.jobsPerCompany()));
        return "template";
    }

    @GetMapping("Most_Demanding_Companies_Pie_Chart")
    public String MostDemandingCompaniesPieChart(Model model) throws IOException {
        wuzzufDao.displayPieChart(wuzzufDao.jobsPerCompany(), "Jobs Per Company");
        model.addAttribute("body", "<img src='/img/JobsPerCompanyPieChart.jpg'>");
        return "template";
    }

    @GetMapping("Most_Popular_Job_Titles")
    public String MostPopularJobTitles(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.mostPopularJobTitles()));
        return "template";
    }

    @GetMapping("Most_Popular_Job_Titles_Bar_Chart")
    public String MostPopularJobTitlesBarChart(Model model) throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularJobTitles(),
                "Most Popular Job Titles",
                "Job Title",
                "Count");
        model.addAttribute("body", "<img src='/img/MostPopularJobTitlesBarChart.jpg'>");
        return "template";
    }

    @GetMapping("Most_Popular_Areas")
    public String MostPopularAreas(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.mostPopularAreas()));
        return "template";
    }

    @GetMapping("Most_Popular_Areas_Bar_Chart")
    public String MostPopularAreasBarChart(Model model) throws IOException {
        wuzzufDao.displayBarChart(wuzzufDao.mostPopularAreas(),
                "Most Popular Areas",
                "Area",
                "Count");
        model.addAttribute("body", "<img src='/img/MostPopularAreasBarChart.jpg'>");
        return "template";
    }

    @GetMapping("Most_Demanded_Skills")
    public String MostDemandedSkills(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.getMostDemandedSkills()));
        return "template";
    }

    @GetMapping("Factorize_YearsExp")
    public String FactorizeYearsExp(Model model) {
        model.addAttribute("body", datasetToTable(wuzzufDao.factorizeColumn("yearsExp")));
        return "template";
    }
}
