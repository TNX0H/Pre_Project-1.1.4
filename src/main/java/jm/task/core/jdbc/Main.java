package jm.task.core.jdbc;

import jm.task.core.jdbc.dao.UserDao;
import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        // реализуйте алгоритм здесь
        UserService user = new UserServiceImpl();
        user.createUsersTable();
        user.saveUser("Biba","Bibonov",(byte) 21);
        user.saveUser("Biba","Bibonov",(byte) 22);
        user.saveUser("Biba","Bibonov",(byte) 23);
        user.saveUser("Biba","Bibonov",(byte) 24);
        user.getAllUsers();
        user.cleanUsersTable();
        user.dropUsersTable();
    }
}
