package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Runs work in a transaction on a pooled connection (auto-commit off): commits when the work returns, rolls back when
 * it throws. A rollback failure is added to the original exception, never hides it. SQL exceptions leave as
 * {@link RawdataException}.
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
            try {
                T result = work.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                throw e;
            }
        } catch (SQLException e) {
            throw new RawdataException("PostgreSQL: " + e.getMessage(), e);
        }
    }
}
