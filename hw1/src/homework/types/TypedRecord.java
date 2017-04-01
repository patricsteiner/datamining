package homework.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A class represents a record with type tag for join operation. This class contains either TF score or IDF score.
 */
public class TypedRecord implements Writable, Comparable<TypedRecord> {
    public enum RecordType {
        TF, IDF
    }

    private RecordType type;
    private double score;
    private int documentId;

    public TypedRecord() {
    }

    public TypedRecord(TypedRecord other) {
        type = other.type;
        score = other.score;
        documentId = other.documentId;
    }

    /**
     * Returns the type of this record. If this record contains TF score, this method returns {@link RecordType#TF}.
     * Otherwise, this method returns {@link RecordType#IDF}.
     */
    public RecordType getType() {
        return type;
    }

    /**
     * Returns the score value of this record.
     */
    public double getScore() {
        return score;
    }

    /**
     * Returns the document id of this record. If this record contains IDF score, this method returns a garbage value.
     * Don't call this method if this record contains IDF score.
     */
    public int getDocumentId() {
        return documentId;
    }

    /**
     * Sets the type of this record to TF score and copies the given values. The original value of this record will be
     * overwritten.
     */
    public void setTFScore(int documentId, double score) {
        this.type = RecordType.TF;
        this.documentId = documentId;
        this.score = score;
    }

    /**
     * Sets the type of this record to IDF score and copies the given values. The original value of this record will be
     * overwritten.
     */
    public void setIDFScore(double score) {
        this.type = RecordType.IDF;
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (type == RecordType.IDF) out.writeByte(0);
        else {
            out.writeByte(1);
            out.writeInt(documentId);
        }

        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int tFlag = in.readByte();
        if (tFlag == 0) type = RecordType.IDF;
        else {
            type = RecordType.TF;
            documentId = in.readInt();
        }

        score = in.readDouble();
    }

    @Override
    public String toString() {
    	return (type == RecordType.TF ? documentId + "\t" : "") + score;
    }
    
	@Override
	public int compareTo(TypedRecord other) {
		// double to compare is really small, so multiply each number by 10000000 to get better result
		return (int) (other.score*10000000 - score*10000000);
	}
}

