import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MemberLogTimeComparator extends WritableComparator {

    protected MemberLogTimeComparator() {
        super(MemberLogTime.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MemberLogTime m1 = (MemberLogTime) a;
        MemberLogTime m2 = (MemberLogTime) b;

        return m1.getMemberName().compareTo(m2.getMemberName()) != 0 ? -1 *
                    m1.getMemberName().compareTo(m2.getMemberName()) : -1 *
                    m1.getLogTime().compareTo(m2.getLogTime());
    }
}
