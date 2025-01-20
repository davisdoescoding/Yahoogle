package cis5550.crawling;


public class runPageRanker {

    //@TODO set default seed URL
    /**
     *
     * @param args cmd-line args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        System.out.println("CRAWLING STARTED");

        String dummyCLI[] = new String[] { "0.1" };
        String submitted = FlameSubmit.submit("localhost:9000", "pagerank.jar", "cis5550.jobs.PageRank", dummyCLI);

        if (submitted != null) {
            System.out.println("runPageRanker Completed");
        }
    }
}