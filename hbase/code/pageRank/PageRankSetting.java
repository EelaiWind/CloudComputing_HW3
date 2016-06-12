package pageRank;

public class PageRankSetting{
	final static String TOTAL_NODE_COUNT_KEY = "TOTAL_NODE_COUNT_KEY";
	final static String TOTAL_DEAD_END_SCORE_KEY = "TOTAL_DEAD_END_SCORE_KEY";
	final static String TMP_OUTPUT_PATH = "/user/s101062105/hw2/tmp_output";
	final static String TMP_INPUT_PATH = "/user/s101062105/hw2/tmp_input";
	final static double ALPHA = 0.85;
	final static double UPSCALE_FACTOR = 1E+18;
	final static double UPSCALE_ERROR_BOUND = 1E+15;
	final static int PARSE_INPUT_REDUCER_COUNT = 10;
}
