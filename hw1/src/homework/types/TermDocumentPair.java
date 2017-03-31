package homework.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A class represents a (term, document id) pair.
 */
public class TermDocumentPair implements Writable {
    private String term;
    private int documentId;

    /**
     * Update the values of pair.
     */
    public void set(String term, int documentId) {
        this.term = term;
        this.documentId = documentId;
    }

    /**
     * Write the values to Hadoop-related files such as the output of mapper or reducer.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, term);
        out.writeInt(documentId);
    }

    /**
     * Read the values from Hadoop-related files.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        term = Text.readString(in);
        documentId = in.readInt();
    }

    @Override
    public String toString() {
        return String.format("%s\t%s", term, documentId);
    }
}

