package invertedIndex;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class FileSectionGroupComparator extends WritableComparator{

	protected FileSectionGroupComparator(){
		super(WordFileIdPositionPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b){
		WordFileIdPositionPair obj1 = (WordFileIdPositionPair) a;
		WordFileIdPositionPair obj2 = (WordFileIdPositionPair) b;
		int wordComparision = obj1.getWord().compareTo(obj2.getWord());
		if ( wordComparision == 0 ){
			return Integer.compare(obj1.getFileId(), obj2.getFileId());
		}
		else{
			return wordComparision;
		}
	}
}