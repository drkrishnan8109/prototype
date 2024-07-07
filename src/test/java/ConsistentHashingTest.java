import org.junit.jupiter.api.Test;
import org.random.ConsistentHashing;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsistentHashingTest {

    @Test
    public void testCache() {
        ConsistentHashing conHash = new ConsistentHashing();
        conHash.assignCacheToRingDefault(new HashMap<Integer,Integer>(), new HashMap<Integer,Integer>());
        conHash.put(121,121);
        conHash.put(125,125);
        conHash.put(139,139);
        assertEquals(139,conHash.get(139));
        conHash.addMoreCache(2);
        conHash.put(121,121);
        conHash.put(10,10);
        assertEquals(121,conHash.get(121));
    }
}
