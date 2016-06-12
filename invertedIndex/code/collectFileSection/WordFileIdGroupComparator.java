package invertedIndex;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class WordFileIdGroupComparator extends WritableComparator{

	protected WordFileIdGroupComparator(){
		super(WordFileIdPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b){
		WordFileIdPair obj1 = (WordFileIdPair) a;
		WordFileIdPair obj2 = (WordFileIdPair) b;
		return obj1.getWord().compareTo(obj2.getWord());
	}
}