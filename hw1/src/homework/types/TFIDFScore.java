package homework.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TFIDFScore implements Writable {
    
	private String term;
    private double idfScore = -1;
    private Map<Integer, Double> tfScores = new HashMap<>(); 

    public TFIDFScore() {
    }
    
    public void setIdfScore(double idfScore) {
    	this.idfScore = idfScore;
    }

    public void setTfScore(int documentId, double tfScore) {
    	tfScores.put(documentId, tfScore);
    }
    
    public double getTFIDFScore(int documentId) throws Exception {
    	if (idfScore < 0) {
    		throw new Exception("idfScore needs to be set first");
    	}
    	else if (!tfScores.containsKey(documentId)) {
    		throw new Exception("there is no tfScore for given document");
    	}
    	else {
    		return idfScore * tfScores.get(documentId);
    	}    	
    }
    
    //gettenbest()

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, term);
        //out.writeInt(documentId);
        //out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int tFlag = in.readByte();
        /*if (tFlag == 0) type = RecordType.IDF;
        else {
            type = RecordType.TF;
            documentId = in.readInt();
        }

        score = in.readDouble();*/
    }
}

