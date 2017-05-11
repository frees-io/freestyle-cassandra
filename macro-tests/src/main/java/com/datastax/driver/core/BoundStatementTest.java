package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class BoundStatementTest extends BoundStatement {

    private Map<String, ByteBuffer> values = new HashMap<>();

    public BoundStatementTest() {
        super(new PreparedStatementTest());
    }

    @Override
    public Statement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        return null;
    }

    @Override
    public BoundStatement setBytesUnsafe(String name, ByteBuffer v) {
        values.put(name, v);
        return this;
    }

    @Override
    public BoundStatement bind(Object... values) {
        return this;
    }

    public Map<String, ByteBuffer> getValues() {
        return values;
    }
}
