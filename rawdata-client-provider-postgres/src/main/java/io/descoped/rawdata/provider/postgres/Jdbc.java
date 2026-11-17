package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Runs work in a transaction on a pooled connection (auto-commit off, READ COMMITTED): commits when the work returns,
 * rolls back when it throws. A rollback failure is added to the original exception, never hides it. SQL exceptions
 * leave as {@link RawdataException}. (On an {@link Error} the pool rolls back when the connection is returned.)
 */
final class Jdbc {

    /**
     * Work on a connection.
     *
     * @param <T> the result type
     */
    @FunctionalInterface
    interface Work<T> {
        T apply(Connection connection) throws SQLException;
    }

    private final DataSource dataSource;

    Jdbc(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    <T> T transaction(Work<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            return commitOrRollback(connection, work);
        } catch (SQLException e) {
            throw new RawdataException("PostgreSQL: " + e.getMessage(), e);
        }
    }

    private static <T> T commitOrRollback(Connection connection, Work<T> work) throws SQLException {
        try {
            T result = work.apply(connection);
            connection.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            rollback(connection, e);
            throw e;
        }
    }

    private static void rollback(Connection connection, Exception cause) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            cause.addSuppressed(rollbackFailure);
        }
    }
}
