#include "../cpp/MariaDBConnection.h"
#include "../cpp/SQLiteConnection.h"
#include <gtest/gtest.h>

using namespace luasqlgen;

TEST(SQLite, Connect)
{
	SQLiteConnection c;
	EXPECT_NO_THROW(c.connect("SQLiteTest"));
}

TEST(SQLite, ConnectRAM)
{
	SQLiteConnection c;
	EXPECT_NO_THROW(c.connect(":memory:"));
}

TEST(SQLite, AdHocQueries)
{
	SQLiteConnection c;
	EXPECT_NO_THROW(c.connect(":memory:"));
	EXPECT_NO_THROW(c.query("create table Test (test int, name varchar(255), something text)"));
	EXPECT_NO_THROW(c.query("insert into Test (test, name, something) values (5, 'ASDF', 'ASDF')"));
	EXPECT_NO_THROW(c.queryJson("insert into Test (test, name, something) values (?, ?, ?)", {"7", "ASDF", "ASDF"}));
	EXPECT_NE("[\n]\n", c.queryJson("select * from Test"));
	EXPECT_EQ("[\n]\n", c.queryJson("select * from Test where test = 123"));
	EXPECT_NO_THROW(c.query("drop table Test"));
}

TEST(MariaDB, Connect)
{
	MariaDBConnection c;
	EXPECT_NO_THROW(c.connect("luasqlgen", "localhost", "", "testuser", "test", 0));
}

TEST(MariaDB, AdHocQueries)
{
	MariaDBConnection c;
	EXPECT_NO_THROW(c.connect("luasqlgen", "localhost", "", "testuser", "test", 0));
	EXPECT_NO_THROW(c.query("create table Test (test int, name varchar(255), something text)"));
	EXPECT_NO_THROW(c.query("insert into Test (test, name, something) values (5, 'ASDF', 'ASDF')"));
	EXPECT_NO_THROW(c.queryJson("insert into Test (test, name, something) values (?, ?, ?)", {"7", "ASDF", "ASDF"}));
	EXPECT_NE("[\n]\n", c.queryJson("select * from Test"));
	EXPECT_EQ("[\n]\n", c.queryJson("select * from Test where test = 123"));
	EXPECT_NO_THROW(c.query("drop table Test"));
}


int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
