package org.cossacklabs;

import org.jooq.*;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;
import static jooq.tables.AcraExample.ACRA_EXAMPLE;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Random;
import org.apache.commons.cli.*;

public class Main {

    public static CommandLine cmd = null;
    public static Charset defaultCharset = StandardCharsets.UTF_8;
    public static final String[] NAMES = {"Jessica", "Ashley", "Amanda", "Sarah", "Jennifer", "Emily", "Samantha", "Elizabeth", "Stephanie", "Lauren", "Michael", "Matthew", "Joshua", "Daniel", "David", "Andrew", "John", "Brandon", "Nicholas", "James"};
    public static final String[] EMAILS = {"a1@gmail.com", "b2@aol.com", "c3@zoho.com", "d4@gmail.com", "e5@proton.com", "f6@gmail.com", "g7@outlook.com", "h8@yandex.com", "i9@rambler.com", "j10@gmx.com"};
    public static final String[] PASSWORDS = {"killer", "master", "angel1", "liverpool", "jesus1", "babygirl11", "naruto", "superman1290", "50cent", "letmeit"};

    public static void main(String[] args) {
        if (args.length != 0) {
            Options options = new Options();
            Option jdbc = new Option("j", "jdbc", true, "jdbc string: jdbc:postgresql://localhost:5432/test");
            jdbc.setRequired(true);
            options.addOption(jdbc);

            Option userName = new Option("u", "user", true, "user name");
            userName.setRequired(true);
            options.addOption(userName);

            Option password = new Option("p", "password", true, "password");
            password.setRequired(true);
            options.addOption(password);

            Option records = new Option("insert", true, "number of records to insert");
            records.setRequired(false);
            options.addOption(records);

            Option all_select = new Option("select", true, "select * from acra_example limit <Arg>");
            all_select.setRequired(false);
            options.addOption(all_select);

            Option email_select = new Option("email", true, "value of email to select");
            email_select.setRequired(false);
            options.addOption(email_select);

            Option name_select = new Option("name", true, "value of name to select");
            name_select.setRequired(false);
            options.addOption(name_select);

            Option password_select = new Option("pass", "password_select", true, "value of password to select");
            password_select.setRequired(false);
            options.addOption(password_select);

            Option drop = new Option("drop", "drop_table", false, "drop test table acra-example");
            drop.setRequired(false);
            options.addOption(drop);

            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();

            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {

                System.out.println(e.getMessage());
                formatter.printHelp("utility", options);
                System.exit(1);
            }

            URL = cmd.getOptionValue("jdbc");
            USER_NAME = cmd.getOptionValue("user");
            PASSWORD = cmd.getOptionValue("password");
            try {
                if (cmd.getOptionValue("insert") != null) {
                    ROWS = Integer.parseInt(cmd.getOptionValue("insert"));
                }
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }

            EMAIL_TO_RETREIVE = cmd.getOptionValue("email");
            NAME_TO_RETREIVE = cmd.getOptionValue("name");
            PASSWORD_TO_RETREIVE = cmd.getOptionValue("pass");
        } else {
            //use default parameters. Will open and close connecttion to db.
        }


        try (Connection conn = DriverManager.getConnection(URL, USER_NAME, PASSWORD)) {
            DSLContext create = DSL.using(conn, SQLDialect.POSTGRES, new Settings().withStatementType(StatementType.STATIC_STATEMENT));
            System.out.println("Connected to database");

            if (cmd.hasOption("drop_table")) {
                create.execute("DROP TABLE IF EXISTS acra_example;");
                create.execute("DROP SEQUENCE IF EXISTS acra_example_seq;");
                System.out.println("drop successful");
                System.exit(0);
            }

            create.execute("CREATE SEQUENCE IF NOT EXISTS acra_example_seq START 1;");
            create.execute("CREATE TABLE IF NOT EXISTS acra_example(id INTEGER PRIMARY KEY DEFAULT nextval('acra_example_seq'), name BYTEA, email BYTEA, password BYTEA);");

            // insert randomized records
            Random rand = new Random();
            for (int i = 0; i < ROWS; i++) {
                CreateUserAccount(create, NAMES[rand.nextInt(NAMES.length)],
                                          EMAILS[rand.nextInt(EMAILS.length)],
                                          PASSWORDS[rand.nextInt(PASSWORDS.length)]);
            }
            if (ROWS != 0) {
                System.out.println("Insert successful");
            }

            Result<Record> result;
            if (cmd.getOptionValue("select") != null) {
                try {
                    int n = Integer.parseInt(cmd.getOptionValue("select"));
                    System.out.println("Select * from acra_example");
                    System.out.println("--------------------------");
                    result = create.select().from(ACRA_EXAMPLE).limit(n).fetch();
                    PrintResult(result);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (EMAIL_TO_RETREIVE != null) {
                System.out.println("Retreival by email");
                System.out.println("------------------");
                result = RetreiveUserByEmail(create, EMAIL_TO_RETREIVE);
                PrintResult(result);
                System.out.println("Retreival by email successful");
            }


            if (NAME_TO_RETREIVE != null) {
                System.out.println();
                System.out.println("Retreival by name");
                System.out.println("-----------------");
                result = RetreiveUserByName(create, NAME_TO_RETREIVE);
                PrintResult(result);
                System.out.println("Retreival by name successful");
            }


            if (PASSWORD_TO_RETREIVE != null) {
                System.out.println();
                System.out.println("Retreival by password");
                System.out.println("---------------------");
                result = RetreiveUserByPassword(create, PASSWORD_TO_RETREIVE);
                PrintResult(result);
                System.out.println("Retreival by password successful");
            }

            System.out.println("Disconnected from database");
        }

        // For the sake of this tutorial, let's keep exception handling simple
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void PrintResult(Result<Record> result) {
        for (Record rec : result) {
            Integer id = rec.getValue(ACRA_EXAMPLE.ID);
            byte[]  name = rec.getValue(ACRA_EXAMPLE.NAME);
            byte[]  email = rec.getValue(ACRA_EXAMPLE.EMAIL);
            byte[]  recPassword = rec.getValue(ACRA_EXAMPLE.PASSWORD);
            System.out.println("id: " + id + "\nname: " + new String(name, defaultCharset) + "\nemail: " + new String(email, defaultCharset) + "\npassword: " + new String(recPassword, defaultCharset));
            System.out.println();
        }
    }

    private static Result<Record> SelectQuery(DSLContext database, Condition whereClause) {
        return database.select().from(ACRA_EXAMPLE).where(whereClause).fetch();
    }

    private static void InsertQuery(DSLContext database, byte[] name_enc, byte[] email_enc, byte[] password_enc) {
        database.insertInto(ACRA_EXAMPLE, ACRA_EXAMPLE.NAME, ACRA_EXAMPLE.EMAIL, ACRA_EXAMPLE.PASSWORD)
                .values(name_enc, email_enc, password_enc)
                .execute();
    }


    public static Result<Record> RetreiveUserByEmail(DSLContext database, String email) {
        return SelectQuery(database, ACRA_EXAMPLE.EMAIL.eq(email.getBytes(defaultCharset)));
    }

    public static Result<Record> RetreiveUserByPassword(DSLContext database, String password) {
        return SelectQuery(database, ACRA_EXAMPLE.PASSWORD.eq(password.getBytes(defaultCharset)));
    }

    public static Result<Record> RetreiveUserByName(DSLContext database, String name) {
        return SelectQuery(database, ACRA_EXAMPLE.NAME.eq(name.getBytes(defaultCharset)));
    }

    public static void CreateUserAccount(DSLContext database, String name, String email, String password) {
        InsertQuery(database, name.getBytes(defaultCharset), email.getBytes(defaultCharset), password.getBytes(defaultCharset));
    }



    // CMD parameters
    public static String USER_NAME = "test";
    public static String PASSWORD = "test";
    public static String URL = "jdbc:postgresql://localhost:9393/test";

    public static int ROWS = 0;

    public static String EMAIL_TO_RETREIVE = null;
    public static String NAME_TO_RETREIVE = null;
    public static String PASSWORD_TO_RETREIVE = null;
}
