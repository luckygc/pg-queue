package github.luckygc.pgq;

import java.sql.Connection;
import java.sql.SQLException;
import org.postgresql.Driver;

public class PgqListener {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private Connection con;

    public PgqListener(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void listen() throws SQLException {

    }

    private void checkConnection() {
        if (con == null || con.isClosed()) {

        }
        Driver driver = new Driver();
    }
}
