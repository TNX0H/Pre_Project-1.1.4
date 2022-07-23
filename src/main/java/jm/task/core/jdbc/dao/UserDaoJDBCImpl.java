package jm.task.core.jdbc.dao;


import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() throws SQLException {
        Connection conn = Util.getConnection();
        try {
            conn.setAutoCommit(false);
            String sql = "CREATE TABLE IF NOT EXISTS users " +
                    "(id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), lastname VARCHAR(255), age INT)";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    public void dropUsersTable() throws SQLException {
        Connection conn = Util.getConnection();
        conn.setAutoCommit(false);
        try {
            String sql = "DROP TABLE IF EXISTS users";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        Connection conn = Util.getConnection();
        conn.setAutoCommit(false);
        try {
            PreparedStatement ps = conn.prepareStatement("INSERT INTO users (name,lastname, age) VALUES (?,?,?)");
            ps.setString(1,name);
            ps.setString(2,lastName);
            ps.setByte(3,age);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    public void removeUserById(long id) throws SQLException {
        Connection conn = Util.getConnection();
        conn.setAutoCommit(false);
        try  {
            PreparedStatement ps = conn.prepareStatement("DELETE FROM users WHERE id = ?");
            ps.setLong(1,id);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    public List<User> getAllUsers() throws SQLException {
        Connection conn = Util.getConnection();
        List<User> users = new ArrayList<>();
        conn.setAutoCommit(false);
        try  {
            String sql = "SELECT * FROM users";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery("SELECT * FROM users");
            while (resultSet.next()){
                User user = new User(resultSet.getString("name"), resultSet.getString("lastname"),
                        resultSet.getByte("age"));
                user.setId(resultSet.getLong("id"));
                users.add(user);
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.close();
        }
        return users;
    }

    public void cleanUsersTable() throws SQLException {
        Connection conn = Util.getConnection();
        conn.setAutoCommit(false);
        try  {
            String sql = "TRUNCATE TABLE users";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.close();
        }
    }
}
