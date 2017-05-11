package com.datastax.driver.core;

public class PrepareIdTest extends PreparedId {

    public PrepareIdTest() {
        super(MD5Digest.wrap(new byte[]{}), new ColumnDefinitionsTest(), new ColumnDefinitionsTest(), new int[]{}, ProtocolVersion.V5);
    }
}
