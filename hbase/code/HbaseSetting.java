package myHbase;

public class HbaseSetting {
    public final static String DATA = "Data";
    public final static String NODE_NAME = "NodeName";
    public final static String PAGE_RANK = "PageRank";
    public final static String SEGMENT_OFFSET = "SegmentOffset";
    public final static String DOCUMENT_ID_AND_FREQUENCY = "IdANdTf";

    public final static String[] colFamily = {DATA};
    public final static String[] pageRankQualifier= {NODE_NAME, PAGE_RANK};
}