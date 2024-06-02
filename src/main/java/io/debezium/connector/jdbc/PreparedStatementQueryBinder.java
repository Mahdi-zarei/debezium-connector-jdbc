/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import com.mchange.v2.c3p0.impl.NewProxyPreparedStatement;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.Oid;
import org.postgresql.core.ParameterList;
import org.postgresql.jdbc.PgArray;
import com.mchange.v2.c3p0.impl.NewProxyConnection;
import org.postgresql.jdbc.PgConnection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

public class PreparedStatementQueryBinder implements QueryBinder {

    private final PreparedStatement binder;

    public PreparedStatementQueryBinder(PreparedStatement binder) {
        this.binder = binder;
    }

    private int getOID(String typeName) {
        typeName = typeName.toLowerCase();
        if (typeName.contains("string") || typeName.contains("text") || typeName.contains("char")) {
            return Oid.TEXT;
        }
        if (typeName.contains("integer") || typeName.contains("int") || typeName.contains("numeric") || typeName.contains("decimal") || typeName.contains("double")) {
            return Oid.NUMERIC;
        }
        if (typeName.contains("date")) {
            return Oid.DATE;
        }
        if (typeName.contains("timestamptz")) {
            return Oid.TIMESTAMPTZ;
        }
        if (typeName.contains("timestamp")) {
            return Oid.TIMESTAMP;
        }
        if (typeName.contains("time")) {
            return Oid.TIME;
        }
        if (typeName.contains("uuid")) {
            return Oid.UUID;
        }
        if (typeName.contains("interval")) {
            return Oid.INTERVAL;
        }
        if (typeName.contains("bit")) {
            return Oid.BIT;
        }
        if (typeName.contains("bool")) {
            return Oid.BOOL;
        }
        return Oid.UNSPECIFIED;
    }

    private int getArrayOID(String typeName) {
        typeName = typeName.toLowerCase();
        if (typeName.contains("string") || typeName.contains("text") || typeName.contains("char")) {
            return Oid.TEXT_ARRAY;
        }
        if (typeName.contains("integer") || typeName.contains("int") || typeName.contains("numeric") || typeName.contains("decimal") || typeName.contains("double")) {
            return Oid.NUMERIC_ARRAY;
        }
        if (typeName.contains("date")) {
            return Oid.DATE_ARRAY;
        }
        if (typeName.contains("timestamptz")) {
            return Oid.TIMESTAMPTZ_ARRAY;
        }
        if (typeName.contains("timestamp")) {
            return Oid.TIMESTAMP_ARRAY;
        }
        if (typeName.contains("time")) {
            return Oid.TIME_ARRAY;
        }
        if (typeName.contains("uuid")) {
            return Oid.UUID_ARRAY;
        }
        if (typeName.contains("interval")) {
            return Oid.INTERVAL_ARRAY;
        }
        if (typeName.contains("bit")) {
            return Oid.BIT_ARRAY;
        }
        if (typeName.contains("bool")) {
            return Oid.BOOL_ARRAY;
        }
        return Oid.UNSPECIFIED;
    }

    private Array getArray(String typeName, Object[] array) throws SQLException {
        int oid = getOID(typeName);
        if (oid == Oid.UNSPECIFIED) {
            throw new IllegalArgumentException("Wrong type " + typeName);
        }
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (int i=0;i<array.length;i++) {
            if (i != 0) {
                sb.append(',');
            }

            if (array[i] == null) {
                sb.append("NULL");
            } else {
                if (oid != Oid.NUMERIC && oid != Oid.BOOL && oid != Oid.BIT) {
                    sb.append('"');
                }
                sb.append(array[i].toString());
                if (oid != Oid.NUMERIC && oid != Oid.BOOL && oid != Oid.BIT) {
                    sb.append('"');
                }
            }
        }
        sb.append('}');

        java.sql.Connection conn;

        try {
            conn = binder.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (conn instanceof BaseConnection) {
            return new PgArray((BaseConnection) conn, oid, sb.toString());
        }

        if (conn instanceof NewProxyConnection) {
            NewProxyConnection proxyConn = (NewProxyConnection) conn;
            Connection inner;
            try {
                Field fInner = proxyConn.getClass().getDeclaredField("inner");
                fInner.setAccessible(true);
                inner = (Connection) fInner.get(proxyConn);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (inner instanceof PgConnection) {
                return new PgArray((PgConnection) inner, oid, sb.toString());
            }

            throw new IllegalStateException("what is this??? " + inner.getClass());
        }

        throw new IllegalStateException("what is this? " + conn.getClass());
    }

    @Override
    public void bind(ValueBindDescriptor valueBindDescriptor) {

        try {
            if (valueBindDescriptor.getTargetSqlType() != null) {
                if (valueBindDescriptor.getTargetSqlType() == Types.ARRAY) {
                    Collection<Object> collection = (Collection<Object>) valueBindDescriptor.getValue();
                    Array array = getArray(valueBindDescriptor.getElementTypeName(), collection.toArray());
                    if (binder instanceof NewProxyPreparedStatement) {
                        NewProxyPreparedStatement pBinder = (NewProxyPreparedStatement) binder;
                        Field fInner = pBinder.getClass().getDeclaredField("inner");
                        fInner.setAccessible(true);

                        PreparedStatement pStmt = (PreparedStatement) fInner.get(pBinder);

                        Field fPrelist = pStmt.getClass().getDeclaredField("preparedParameters");
                        fPrelist.setAccessible(true);

                        ParameterList pList = (ParameterList) fPrelist.get(pStmt);
                        pList.setStringParameter(valueBindDescriptor.getIndex(), array.toString(), getArrayOID(valueBindDescriptor.getElementTypeName()));
                    } else {
                        throw new IllegalStateException("nope " + binder.getClass());
                    }
                }
                else {
                    binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue(), valueBindDescriptor.getTargetSqlType());
                }
            }
            else {
                binder.setObject(valueBindDescriptor.getIndex(), valueBindDescriptor.getValue());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
