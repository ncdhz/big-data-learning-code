import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MemberLogTime implements WritableComparable<MemberLogTime> {

    private String memberName;

    private String logTime;

    public int compareTo(MemberLogTime lt) {
        return this.memberName.compareTo(lt.getLogTime());
    }

    public MemberLogTime() {
    }

    public MemberLogTime(String memberName, String logTime) {
        this.memberName = memberName;
        this.logTime = logTime;
    }

    public String getMemberName() {
        return memberName;
    }

    public void setMemberName(String memberName) {
        this.memberName = memberName;
    }

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.memberName);
        dataOutput.writeUTF(this.logTime);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.memberName = dataInput.readUTF();
        this.logTime = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return memberName + " " + logTime;
    }
}
