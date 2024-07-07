import org.junit.jupiter.api.Test;
import org.random.ConsistentHashing;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsistentHashingTest {

    @Test
    public void testCache() {
        ConsistentHashing conHash = new ConsistentHashing();
        conHash.assignCacheToRingDefault(new HashMap<Integer,Integer>(), new HashMap<Integer,Integer>());
        conHash.put(137,137);
        assertEquals(137,conHash.get(137));
        conHash.put(123,123);
        assertEquals(123,conHash.get(123));
        conHash.put(121,121);
        assertEquals(121,conHash.get(121));

        conHash.addMoreCache(2);

        conHash.put(121,121);
        assertEquals(121,conHash.get(121)); //wonderful, keys rebalanced!
        conHash.put(10,10);
        assertEquals(10,conHash.get(10));
        conHash.gracefullyFailCache(4);
        assertEquals(123,conHash.get(123)); //wonderful, keys rebalanced!
        assertEquals(121,conHash.get(121));
    }
}
