if #arg == 0 then
   os.exit(0)
end

--
-- Stuff to ensure alphabetic order
-- Taken from http://lua-users.org/wiki/SortedIteration
--
--[[
   Ordered table iterator, allow to iterate on the natural order of the keys of a
   table.

   Example:
]]

function __genOrderedIndex( t )
   local orderedIndex = {}
   for key in pairs(t) do
	  table.insert( orderedIndex, key )
   end
   table.sort( orderedIndex )
   return orderedIndex
end

function orderedNext(t, state)
   -- Equivalent of the next function, but returns the keys in the alphabetic
   -- order. We use a temporary ordered key table that is stored in the
   -- table being iterated.

   local key = nil
   --print("orderedNext: state = "..tostring(state) )
   if state == nil then
	  -- the first time, generate the index
	  t.__orderedIndex = __genOrderedIndex( t )
	  key = t.__orderedIndex[1]
   else
	  -- fetch the next value
	  for i = 1,#t.__orderedIndex do
		 if t.__orderedIndex[i] == state then
			key = t.__orderedIndex[i+1]
		 end
	  end
   end

   if key then
	  return key, t[key]
   end

   -- no more value to return, cleanup
   t.__orderedIndex = nil
   return
end

function orderedPairs(t)
   -- Equivalent of the pairs() function on tables. Allows to iterate
   -- in order
   return orderedNext, t, nil
end

function genMariadbpp(description)

--
-- MariaDBPP C++ glue code
--
--
local mariadbpptypes = {
   string = "string",
   int = "signed32",
   uint = "unsigned32",
   int64 = "signed64",
   uint64 = "unsigned64",
   bool = "unsigned32" -- FIXME: Byte?
}

function type2mariadbpp(type)
   local result = mariadbpptypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
	  return "unsigned64"
   end
   return result
end

function generateCreateFunction(file, name, tbl)
   print("Generating create" .. name)
   local stmtName = "create" .. name .. "Stmt"
   file:write("\n\tmariadb::statement_ref " .. stmtName .. ";\n")
   file:write("\tvoid create" .. name .. "(struct " .. name .. "& self)\n\t{\n")

   local i = 0;
   for p,q in orderedPairs(tbl) do
	  -- FIXME: Translate types!
	  file:write("\t\t" .. stmtName .. "->set_" .. type2mariadbpp(q) .. "(" .. i .. ", self." .. p .. ");\n")
	  i = i + 1
   end

   file:write("\t\tself.id = " .. stmtName .. "->insert();\n")
   file:write("\t}\n")
end

function generateUpdateFunction(file, name, tbl)
   print("Generating update" .. name)
   local stmtName = "update" .. name .. "Stmt"
   file:write("\n\tmariadb::statement_ref " .. stmtName .. ";\n")
   file:write("\tvoid update" .. name .. "(const struct " .. name .. "& self)\n\t{\n")

   local i = 0;
   for p,q in orderedPairs(tbl) do
	  -- FIXME: Translate types!
	  file:write("\t\t" .. stmtName .. "->set_" .. type2mariadbpp(q) .. "(" .. i .. ", self." .. p .. ");\n")
	  i = i + 1
   end

   file:write("\t\t" .. stmtName .. "->set_unsigned64(" .. i .. ", self.id);\n")
   file:write("\t\t" .. stmtName .. "->execute();\n")
   file:write("\t}\n")
end

function generateDeleteFunction(file, name, tbl)
   print("Generating delete" .. name)
   local stmtName = "delete" .. name .. "Stmt"
   file:write("\n\tmariadb::statement_ref " .. stmtName .. ";\n")
   file:write("\tvoid delete" .. name .. "(unsigned long long id)\n\t{\n")

   file:write("\t\t" .. stmtName .. "->set_unsigned64(0, id);\n")
   file:write("\t\t" .. stmtName .. "->execute();\n")
   file:write("\t}\n")
end

function generateGetFunction(file, name, tbl)
   print("Generating get" .. name)
   local stmtName = "get" .. name .. "Stmt"
   file:write("\n\tmariadb::statement_ref " .. stmtName .. ";\n")
   file:write("\tbool get" .. name .. "(unsigned long long id, " .. name .. "& object)\n\t{\n")

   file:write("\t\t" .. stmtName .. "->set_unsigned64(0, id);\n")
   file:write("\t\tmariadb::result_set_ref result = " .. stmtName .. "->query();\n")

   file:write([[
		if(result->row_count() == 0 || !result->next())
			return false;

	 	object.id = id;
]])

   for p,q in orderedPairs(tbl) do
	  file:write("\t\tobject." .. p .. " = " .. "result->get_" .. type2mariadbpp(q) .. "(\"" .. p .. "\");\n")
   end

   file:write("\t\treturn true;\n\t}\n")
end

function generateQueryFunction(file, name, tbl)
   print("Generating query" .. name)
   local stmtName = "query" .. name .. "Stmt"
   file:write("\n\tmariadb::statement_ref " .. stmtName .. ";\n")
   file:write("\tvoid query" .. name .. "(std::vector<" .. name .. ">& out, ") -- "\n\t{\n")

   for p,q in orderedPairs(tbl) do
	  file:write("const std::string& " .. p .. ", ")
   end
   file:seek("cur", -2)
   file:write(")\n\t{\n")
   
   local i = 0;
   for p,q in orderedPairs(tbl) do
	  -- FIXME: Translate types!
	  file:write("\t\t" .. stmtName .. "->set_string(" .. i .. ", " .. p .. ");\n")
	  i = i + 1
   end
   
   file:write("\t\tauto result = " .. stmtName .. "->query();\n\n")
   file:write("\t\tstruct " .. name .. " object;\n")
   --file:write("\t\tstd::cout << result->set_row_index(0) << \" \" << result->error() << std::endl;\n");

   file:write("\t\tfor(unsigned int j = 0; j < result->row_count() && result->next(); j++)\n\t\t{\n")
   file:write("\t\t\tobject.id = " .. "result->get_unsigned32(\"id\");\n")

   for p,q in orderedPairs(tbl) do
	  file:write("\t\t\tobject." .. p .. " = " .. "result->get_" .. type2mariadbpp(q) .. "(\"" .. p .. "\");\n")
   end
   --file:write("\t\tstd::cout << object.toJson() << std::endl;\n");
   file:write("\t\t\tout.push_back(object);\n\t\t}\n")
   
   file:write("\t}\n")
end

function generateSearchFunction(file, name, tbl)
	print("Generating search" .. name)
	local stmtName = "search" .. name .. "Stmt"
	file:write("\n\tmariadb::statement_ref " .. stmtName .. ";\n")
	file:write("\tvoid search" .. name .. "(std::vector<" .. name .. ">& out, const std::string& term)\n\t{\n")

	file:write([[
		std::string processedTerm = term;
		std::replace(processedTerm.begin(), processedTerm.end(), ' ', '%');
]])

	local i = 0;
	for p,q in orderedPairs(tbl) do
		-- FIXME: Translate types!
		file:write("\t\t" .. stmtName .. "->set_string(" .. i .. ", processedTerm);\n")
		i = i + 1
	end

	file:write("\t\tauto result = " .. stmtName .. "->query();\n\n")
	file:write("\t\tstruct " .. name .. " object;\n")
	--file:write("\t\tstd::cout << result->set_row_index(0) << \" \" << result->error() << std::endl;\n");

	file:write("\t\tfor(unsigned int j = 0; j < result->row_count() && result->next(); j++)\n\t\t{\n")
	file:write("\t\t\tobject.id = " .. "result->get_unsigned32(\"id\");\n")

	for p,q in orderedPairs(tbl) do
		file:write("\t\t\tobject." .. p .. " = " .. "result->get_" .. type2mariadbpp(q) .. "(\"" .. p .. "\");\n")
	end
	--file:write("\t\tstd::cout << object.toJson() << std::endl;\n");
	file:write("\t\t\tout.push_back(object);\n\t\t}\n")
	file:write("\t}\n")
end

--
-- MySQL SQL code
--
--
local mysqltypes = {
   string = "text",
   int = "int",
   uint = "int unsigned",
   int64 = "bigint",
   uint64 = "bigint unsigned",
   bool = "bool" -- FIXME: Byte?
}

function type2mysql(type)
   local result = mysqltypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
	  return "bigint"
   end
   return result
end

function generateCreateStmt(file, name, tbl)
   print("Generating create" .. name .. "Stmt")  
   file:write("\t\tcreate" .. name .. "Stmt = m_connection->create_statement(")
   file:write("\"insert into `" .. name .. "` (")

   local size = 0
   for p,q in orderedPairs(tbl) do
	  file:write("`" .. p .. "`, ")
	  size = size + 1
   end

   -- Delete the last ',' as it is not needed
   file:seek("cur", -2)
   
   if size > 0 then
	  file:write(") values (?")
	  for i = 1, size - 1, 1 do file:write(",?") end
	  file:write(");\"")
   else
	  file:write(") values();\"")
   end
   
   file:write(");\n")
end

function generateUpdateStmt(file, name, tbl)
   print("Generating update" .. name .. "Stmt")  
   file:write("\t\tupdate" .. name .. "Stmt = m_connection->create_statement(")
   file:write("\n\t\t\"update `" .. name .. "` set\"\n")

   local size = 0
   for p,q in orderedPairs(tbl) do
	  file:write("\t\t\t\"`" .. p .. "` = ?,\"\n")
   end

   -- Delete the last ',' as it is not needed
   file:seek("cur", -3)
   file:write(" where `id` = ?;\");\n")
end

function generateDeleteStmt(file, name, tbl)
   print("Generating delete" .. name .. "Stmt")  
   file:write("\t\tdelete" .. name .. "Stmt = m_connection->create_statement(")
   file:write("\"delete from `" .. name .. "` where `id` = ?;\");\n")
end

function generateGetStmt(file, name, tbl)
   print("Generating get" .. name .. "Stmt")
   file:write("\t\tget" .. name .. "Stmt = m_connection->create_statement(")
   file:write("\"select * from `" .. name .. "` where `id` = ?;\");\n")
end

function generateQueryStmt(file, name, tbl)
   print("Generating query" .. name .. "Stmt")  
   file:write("\t\tquery" .. name .. "Stmt = m_connection->create_statement(")
   file:write("\"select * from `" .. name .. "` where ")

   for p,q in orderedPairs(tbl) do
	  file:write("`" .. p .. "` like ? and ")
   end

   file:seek("cur", -5)
   file:write(";\");\n")
end

function generateSearchStmt(file, name, tbl)
	print("Generating search" .. name .. "Stmt")
	file:write("\t\tsearch" .. name .. "Stmt = m_connection->create_statement(")
	file:write("\"select * from `" .. name .. "` where ")

	for p,q in orderedPairs(tbl) do
		file:write("`" .. p .. "` like ? or ")
	end

	file:seek("cur", -4)
	file:write(";\");\n")
end

local tables = description.tables

--
-- Write SQL stuff
--
--
local sqlfile = io.open(description.name .. ".sql", "w")
sqlfile:write([[
-- Generated by LuaSQL
-- MySQL table generation code. Execute on a created database to fill it.

create table `DBInfo` (version TEXT NOT NULL);
insert into `DBInfo` (version) values (']] .. tostring(description.version) .. [[');

]])


--
-- Write C++ stuff
--
--
local file = io.open(description.name .. ".h", "w")

file:write([[
// Generated by LuaSQL
#pragma once

#include <string>
#include <cstdint>
#include <mariadb++/connection.hpp>
#include <exception>
#include <vector>
#include <fstream>
]] .. description.defines .. [[

namespace ]] .. description.name ..
   [[
{
using std::string;
typedef uint32_t uint;
typedef int64_t int64;
typedef uint64_t uint64;

]])

for k,v in orderedPairs(tables) do

   -- SQL
   sqlfile:write("create table `" .. k .. "` (\n\t`id` int primary key auto_increment")
   
   -- C++
   file:write("struct " .. k .. "\n{\n")
   file:write("\tunsigned long long id = 0;\n")

   local toJsonString = ""
   for p,q in orderedPairs(v) do
	  -- SQL
	  sqlfile:write(",\n\t`" .. p .. "` " .. type2mysql(q) .. " NOT NULL")
   	  toJsonString = toJsonString .. "\t\t" .. [[ss << "\"]] .. p .. [[\" : \"" << ]] .. p .. " << \"\\\",\" << std::endl;\n";

	  -- C++
	  -- Write into struct
	  if tables[q] ~= nil then
		 if p ~= q then
			file:write("\tunsigned int " .. p .. " = 0;\n")
		 end
	  else
		 if q ~= "string" then
		 	file:write("\t" .. q .. " " .. p .. " = 0;\n")
		 else
			file:write("\t" .. q .. " " .. p .. ";\n")
		 end
	  end
   end

   sqlfile:write(");\n\n")

   -- Generate toJson
   file:write([[

	std::string toJson() const
	{
		std::stringstream ss;
		ss << "{\n";
]] .. toJsonString .. [[
		ss << "\"id\" : \"" << id << "\"\n";
		ss << "}\n";
		return ss.str();
	}
]])
   -- Generate custom methods
   for i,f in ipairs(description.structdef) do
	  file:write(f(k, v))
   end

   file:write("};\n\n")
end


file:write(
   "\n\nclass " .. description.name .. "\n{\npublic:\n\t" .. description.name  ..
	  [[
(const std::string& db, const std::string& host, const std::string& name, 
				const std::string& password, const unsigned short port)
	{
    	connect(db, host, name, password, port);
    }

]])

-- Empty default constructor
file:write("\t" .. description.name .. "() {}\n\n")

file:write("\t~" .. description.name .. "()\n\t{\n")
file:write([[
	if(m_connection != nullptr && m_connection->connected())
		m_connection->disconnect();
]])
file:write("\t}\n\n")

for k,v in orderedPairs(tables) do
   generateCreateFunction(file, k, v)
   generateGetFunction(file, k, v)
   generateUpdateFunction(file, k, v)
   generateDeleteFunction(file, k, v)
   generateQueryFunction(file, k, v)
   generateSearchFunction(file, k, v)
end

file:write([[

	void connect(const std::string& db, const std::string& host,
				const std::string& name, const std::string& password, const unsigned short port)
	{
		mariadb::account_ref account = mariadb::account::create(host, name, password, "", port);
		account->set_auto_commit(true);

		m_connection = mariadb::connection::create(account);
		if(!m_connection->connect())
			throw std::runtime_error("Could not connect to MariaDB database: " + m_connection->error());

		m_connection->execute("CREATE DATABASE IF NOT EXISTS " + db + "; USE " + db + ";");
		m_connection->set_schema(db);
	}

	void execute(const std::string& file)
	{
		std::ifstream in(file);
		if(!in)
			throw std::runtime_error("Could not open SQL script file!");

		std::stringstream buf;
		buf << in.rdbuf();

		m_connection->execute(buf.str());
	}

	void init(const std::string& db)
	{
		// Check if tables exist or not
		if(m_connection->query("show tables like 'DBInfo';")->row_count() == 0)
			execute(db);

]])

for k,v in orderedPairs(tables) do
   generateCreateStmt(file, k, v)
   generateUpdateStmt(file, k, v)
   generateDeleteStmt(file, k, v)
   generateQueryStmt(file, k, v)
   generateGetStmt(file, k, v)
   generateSearchStmt(file, k, v)
end

file:write([[	}

	void drop()
	{
		m_connection->execute("drop database " + m_connection->schema() + ";");
	}

	void close()
	{
		m_connection->disconnect();
	}

	mariadb::connection_ref getConnection() const { return m_connection; }

private:
	mariadb::connection_ref m_connection;

]])

file:write("};\n}\n") -- Close file and namespace
file:close()
sqlfile:close()

local testfile = io.open(description.name .. "Test.cpp", "w")
testfile:write([[
// Google test suite for the database, generated by LuaSQL
#include <gtest/gtest.h>

#ifndef DATABASE 
#define DATABASE "testdb"
#endif

#ifndef HOST 
#define HOST "localhost"
#endif

#ifndef USER
#define USER "handiserv"
#endif

#ifndef PASSWORD
#define PASSWORD "niugnip"
#endif

#ifndef PORT
#define PORT 13306
#endif

]])

testfile:write("#include \"" .. description.name .. ".h\"\n\nusing namespace " .. description.name .. ";\n\n")

testfile:write([[
class ]] .. description.name .. [[Test : public testing::Test
{
protected:

	]] .. description.name .. "::" .. description.name .. [[ sql;

	std::string getDatabaseName()
	{
		const ::testing::TestInfo* const testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
		const std::string database = "handiserv_" + std::string(testInfo->test_case_name())
			+ "_" + std::string(testInfo->name());

		return database;
	}

	virtual void SetUp() override
	{
		sql.connect(getDatabaseName(), HOST, USER, PASSWORD, PORT);
		sql.init("]] .. description.name .. [[.sql");
	}

	virtual void TearDown() override
	{
		sql.drop();
		sql.close();
	}
};

]])

for k,v in orderedPairs(tables) do
   testfile:write("TEST_F(" .. description.name .. "Test, AddUpdateRemoveQuery" .. k .. ")\n{\n")
   testfile:write("\tstruct " .. k .. " object;\n");
   testfile:write("\tobject.id = -1;\n")
   testfile:write("\tsql.create" .. k .. "(object);\n\n")

   testfile:write("\tASSERT_NE(-1, object.id);\n");

   testfile:write("\tstruct " .. k .. " object2;\n");
   testfile:write("\tsql.get" .. k .. "(object.id, object2);\n\n")

   testfile:write("\tASSERT_EQ(object.id, object2.id);\n");

   testfile:write("\tsql.delete" .. k .. "(object.id);\n")
   testfile:write("\tEXPECT_FALSE(sql.get" .. k .. "(object.id, object2));\n\n")

   -- testfile:write("\tstd::cout << object.toJson() << std::endl;\n")
   testfile:write("}\n\n")
end

testfile:close()

--
-- Plantuml
--

local pumlfile = io.open(description.name .. "Design.puml", "w")

pumlfile:write("@startuml\n\n")
for k,v in orderedPairs(tables) do
   local connectionStr = ""
   local connectionMap = {}
   pumlfile:write([[
class ]] .. k .. "{\n")

   for p,q in orderedPairs(v) do
	  pumlfile:write("\t" .. p .. " : " .. q .. "\n")

	  -- Only add every type once
	  if tables[q] ~= nil and connectionMap[q] == nil then
		 connectionStr = connectionStr .. k .. " -- " .. q .. "\n"
		 connectionMap[q] = true
	  end
   end

   pumlfile:write("}\n" .. connectionStr .. "\n")
end
pumlfile:write("@enduml\n")
pumlfile:close()

end

local description = dofile(arg[1])
genMariadbpp(description)
