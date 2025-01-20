package cis5550.crawling;

public class runCrawler {

    //@TODO set default seed URL

    /**
     * @param args command-line args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        System.out.println("CRAWLING STARTED");

        String dummyCLI_SeedURL[] = new String[] { "https://en.wikipedia.org/wiki/University_of_Pennsylvania_School_of_Engineering_and_Applied_Science" };

        String submitted = FlameSubmit.submit("localhost:9000", "crawler.jar", "cis5550.jobs.Crawler", dummyCLI_SeedURL);

        if (submitted != null) {
            System.out.println("runCrawler Completed");
        }
    }
}