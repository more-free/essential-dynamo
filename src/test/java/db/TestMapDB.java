package db;


import junit.framework.Assert;
import org.junit.Test;

public class TestMapDB {
    @Test
    public void testMapDB() {
        KeyValueDBClient client =
                new MapDBClient("/Users/morefree/Developments/nosql-workspace/mapdb/testMapDb").getCollectionBuilder("test")
                .withAutoCommit();

        client.put("some", "testMapDB2015".getBytes());
        String r = new String((byte [])client.get("some"));
        Assert.assertEquals("testMapDB2015", r);
    }
}
