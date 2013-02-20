package com.github.boneill42;

import java.util.Date;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class AstyanaxDaoTest {
    private static Logger LOG = LoggerFactory.getLogger(AstyanaxDaoTest.class);

    AstyanaxDao dao;

    @Before
    public void setup() {
        dao = new AstyanaxDao("localhost:9160", "examples");
    }

    @After
    public void teardown() {
        dao = null;
    }

    @Test
    public void testDao() throws Exception {
        dumpStrings(dao.read("fishblogs", "bigcat"));
    }

    @Test
    public void testFishBlogDao() throws Exception {
        long now = System.currentTimeMillis();
        FishBlogColumn blogColumn = new FishBlogColumn();
        blogColumn.key.fishtype = "CATFISH";
        blogColumn.key.field = "blog";
        blogColumn.key.when = now;
        blogColumn.value = "this is myblog.".getBytes();
        //dao.writeBlog("fishblogs", "bigcat", blogColumn, "this is myblog.".getBytes());

        FishBlogColumn image = new FishBlogColumn();
        image.key.fishtype = "CATFISH";
        image.key.when = now;
        image.key.field = "image";
        byte[] buffer = new byte[10];
        buffer[0] = 1;
        image.value = buffer;

        dao.writeBlog("fishblogs", "bigcat", blogColumn, image);

        dumpFishBlog(dao.readBlogs("fishblogs", "boneill42"));
    }

    @Test
    public void testFishblogCql3Read() throws ConnectionException {
        CqlResult<String, String> result = dao.readWithCql("fishblogs", "bigcat");

        for (Row<String, String> row : result.getRows()) {
            System.out.println("CQL Key: " + row.getKey());

            ColumnList<String> columns = row.getColumns();

            System.out.println("   userid   : " + columns.getStringValue("userid",      null));
            System.out.println("   when     : " + columns.getDateValue("when",     null));
            System.out.println("   fishtype : " + columns.getStringValue("fishtype",     null));
            System.out.println("   blog     : " + columns.getStringValue("blog", null));
            System.out.println("   image    : " + new String(Hex.encodeHex(columns.getByteArrayValue("image", null))));
        }

    }

    @Test
    public void testFishblogCql3Write() throws ConnectionException {
        dao.writeWithCql("fishblogs");
    }


    public void dumpStrings(ColumnList<String> columns) {
        for (Column<String> column : columns) {
            System.out.println("[" + column.getName() + "]->[" + column.getStringValue() + "]");
        }
    }

    public void dumpFishBlog(ColumnList<FishBlogColumn.Key> columns) {
        for (Column<FishBlogColumn.Key> column : columns) {
            FishBlogColumn.Key fishBlogColumn = column.getName();
            System.out.println("fishBlogColumn.when=>[" + new Date(fishBlogColumn.when) + "]");
            System.out.println("fishBlogColumn.type=>[" + fishBlogColumn.fishtype + "]");
            System.out.println("fishBlogColumn.field=>[" + fishBlogColumn.field + "]");
            System.out.println("fishBlogColumn.value=>[" + column.getStringValue() + "]");
        }
    }
}
