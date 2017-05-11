package com.datastax.driver.core;

import com.datastax.driver.core.policies.RetryPolicy;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PreparedStatementTest implements PreparedStatement {

    @Override
    public ColumnDefinitions getVariables() {
        return new ColumnDefinitionsTest();
    }

    @Override
    public BoundStatement bind(Object... values) {
        return new BoundStatementTest();
    }

    @Override
    public BoundStatement bind() {
        return new BoundStatementTest();
    }

    @Override
    public PreparedStatement setRoutingKey(ByteBuffer routingKey) {
        return null;
    }

    @Override
    public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
        return null;
    }

    @Override
    public ByteBuffer getRoutingKey() {
        return null;
    }

    @Override
    public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency) {
        return null;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel() {
        return null;
    }

    @Override
    public PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
        return null;
    }

    @Override
    public ConsistencyLevel getSerialConsistencyLevel() {
        return null;
    }

    @Override
    public String getQueryString() {
        return null;
    }

    @Override
    public String getQueryKeyspace() {
        return null;
    }

    @Override
    public PreparedStatement enableTracing() {
        return null;
    }

    @Override
    public PreparedStatement disableTracing() {
        return null;
    }

    @Override
    public boolean isTracing() {
        return false;
    }

    @Override
    public PreparedStatement setRetryPolicy(RetryPolicy policy) {
        return null;
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return null;
    }

    @Override
    public PreparedId getPreparedId() {
        return new PrepareIdTest();
    }

    @Override
    public Map<String, ByteBuffer> getIncomingPayload() {
        return new HashMap<>();
    }

    @Override
    public Map<String, ByteBuffer> getOutgoingPayload() {
        return new HashMap<>();
    }

    @Override
    public PreparedStatement setOutgoingPayload(Map<String, ByteBuffer> payload) {
        return null;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return null;
    }

    @Override
    public PreparedStatement setIdempotent(Boolean idempotent) {
        return null;
    }

    @Override
    public Boolean isIdempotent() {
        return null;
    }
}
