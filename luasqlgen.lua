if #arg == 0 then
   os.exit(0)
end
--os.exit(0)
function scriptPath()
   local str = debug.getinfo(2, "S").source:sub(2)
   return str:match("(.*/)")
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

function string:escape()
	return self:gsub('"', "\\\"")
				:gsub('\\', "\\\\")
				:gsub('\b', "\\b")
				:gsub('\f', "\\f")
				:gsub('\n', "\\n")
				:gsub('\r', "\\r")
				:gsub('\t', "\\t")
end

local cpptypes = {
	string = true, double = true, float = true, bool = true, int = true, uint = true, uint64 = true
}

local sql = dofile(scriptPath() .. "/sql.lua")
local basePath = arg[1]:sub(0, arg[1]:len() - arg[1]:reverse():find("/"))
local description = dofile(arg[1])
local tables = description.tables

-- Write structs
local structfile = io.open(description.name .. ".h", "w")
structfile:write([[
// Generated by LuaSQL
#pragma once

#include <DatabaseConnection.h>

#include <string>
#include <cstdint>
#include <regex>

// For toJson
#include <sstream>
#include <iomanip>
]] .. description.defines .. [[namespace ]] .. description.name .. "\n" ..
[[
{
using std::string;
typedef uint32_t uint;
typedef int64_t int64;
typedef uint64_t uint64;

]])

for k,v in orderedPairs(tables) do
	-- C++
	structfile:write("struct " .. k .. "\n{\n")
	structfile:write("\tunsigned long long id = 0;\n")

	local toJsonString = ""
	for p,q in orderedPairs(v) do
		if q == "string" then
			toJsonString = toJsonString .. "\t\t" .. [[ss << "\"]] .. p .. [[\" : \"" << luasqlgen::PreparedStmt::jsonEscape(]] .. p .. ") << \"\\\",\" << std::endl;\n";
		else
			toJsonString = toJsonString .. "\t\t" .. [[ss << "\"]] .. p .. [[\" : \"" << ]] .. p .. " << \"\\\",\" << std::endl;\n";
		end

		-- C++
		-- Write into struct
		if tables[q] ~= nil then
			if p ~= q then
				structfile:write("\tunsigned int " .. p .. " = 0;\n")
			end
		else
			 if q ~= "string" and cpptypes[q] then -- If we have a C++ basic type that is no string
				structfile:write("\t" .. q .. " " .. p .. " = 0;\n")
			 elseif q == "string" then -- If we have a string
				structfile:write("\t" .. q .. " " .. p .. ";\n")
			 else -- If we have a non-existing custom type (e.g. reference to table from another module)
				 structfile:write("\tunsigned long long " .. p .. ";\n")
			 end
		end
	end

   structfile:write("\n\tvoid validate()\n\t{\n")
   structfile:write("\t\t// Integrity check\n")
   if description.checks ~= nil and description.checks[k] ~= nil then
   		for field, regex in orderedPairs(description.checks[k]) do
			structfile:write("\t\tif(!std::regex_match(" .. field .. ", std::regex(\"" .. regex
				.. "\")))\n\t\t\tthrow std::runtime_error(\"Integrity check failed: "
				.. field .. "\");\n")
		end
   end
   structfile:write("\t}\n")

   -- Generate toJson
   structfile:write([[

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
	   structfile:write(f(k, v))
	end

	structfile:write("};\n\n")
end
structfile:write("class " .. description.name .. "\n{\n")
structfile:write([[
private:
	std::shared_ptr<luasqlgen::DatabaseConnection> m_connection;
]])

structfile:write("public:\n")
structfile:write("\t" .. description.name .. "(const std::shared_ptr<luasqlgen::DatabaseConnection>& conn) : m_connection(conn) {}\n")

for k,v in orderedPairs(tables) do
	sql:generateCreateFunction(structfile, k, v)
	sql:generateGetFunction(structfile, k, v)
	sql:generateUpdateFunction(structfile, k, v)
	sql:generateDeleteFunction(structfile, k, v)
	sql:generateQueryFunction(structfile, k, v)
	sql:generateSearchFunction(structfile, k, v)
end

-- Write scripts
if description.scripts ~= nil then
	for index, file in ipairs(description.scripts) do

		local sourcePath = basePath .. "/" .. file
		local sourceFile = io.open(sourcePath, "r")
		if not sourceFile then 
			print("Could not open SQL script " .. sourcePath) 
			os.exit(1) 
		end

		local sources = sourceFile:read("*all")
		sourceFile:close()

		local slashLocStart, slashLocEnd = file:reverse():find("/")
		
		slashLocStart = slashLocStart or file:len() + 2
		slashLocStart = file:len() - slashLocStart + 2

		structfile:write("\tvirtual std::string " .. file:sub(slashLocStart, file:find(".sql") - 1) .. "(const std::vector<std::string>& args)\n\t{\n")
		local lines = {}
		for match in sources:gmatch("(.-);") do
			table.insert(lines, "m_connection->queryJson(\"" .. match:escape() .. "\", args);\n");
		end

		for k, v in ipairs(lines) do
			if k == #lines then
				structfile:write("\t\treturn " .. v)
			else
				structfile:write("\t\t" .. v)
			end
		end
		structfile:write("\t}\n\n")

		structfile:write("\tvirtual void " .. file:sub(slashLocStart, file:find(".sql") - 1) .. "(const std::vector<std::string>& args, luasqlgen::DatabaseResult& result)\n\t{\n")
		structfile:write("\t\tm_connection->query(\"" .. sources:escape() .. "\", args, result);\n");
		structfile:write("\t}\n\n")

		-- structfile:write(
--
--	{
--		return m_connection->queryJson(" .. sources:escape() .. [[", args);
--	}
--)
	end
end

structfile:write(
[[
	virtual void begin() { m_connection->query("begin;"); }
	virtual void commit() { m_connection->query("commit;"); }
	virtual void rollback() { m_connection->query("rollback;"); }
	
	virtual void installMariaDB()
	{
		]] .. sql:generateInstallStmtMariaDB(description.tables, description.constraints or {})  ..[[;
	}
	
	virtual void installSQLite()
	{
		]] .. sql:generateInstallStmtSQLite(description.tables, description.constraints or {})  ..[[;
	}
	
	virtual void install()
	{
		if(!strcmp(m_connection->getName(), "MariaDB"))
			installMariaDB();
		else
			installSQLite();
	}
]])

structfile:write("void dropTablesMariaDB()\n{\n")
for k,v in orderedPairs(tables) do
	structfile:write("m_connection->query(\"drop table " .. k .. ";\");\n")
end
structfile:write("}\n")

structfile:write("void clearTablesMariaDB()\n{\n")
for k,v in orderedPairs(tables) do
	structfile:write("m_connection->query(\"delete from " .. k .. ";\");\n")
	structfile:write("m_connection->query(\"alter table " .. k .. " AUTO_INCREMENT=1;\");\n")
end
structfile:write("}\n")

structfile:write("void dropTablesSQLite()\n{\n")
for k,v in orderedPairs(tables) do
	structfile:write("m_connection->query(\"drop table " .. k .. ";\");\n")
end
structfile:write("}\n")

structfile:write("void clearTablesSQLite()\n{\n")
for k,v in orderedPairs(tables) do
	structfile:write("m_connection->query(\"delete from " .. k .. ";\");\n")
	structfile:write("m_connection->query(\"update sqlite_sequence set seq = 0 where name='" .. k .. "';\");\n")
end
structfile:write("}\n")

structfile:write([[
	virtual void dropTables()
	{
		if(!strcmp(m_connection->getName(), "MariaDB"))
			dropTablesMariaDB();
		else
			dropTablesSQLite();
	}
	
	virtual void clearTables()
	{
		if(!strcmp(m_connection->getName(), "MariaDB"))
			clearTablesMariaDB();
		else
			clearTablesSQLite();
	}
]])
	                                                                                                  
structfile:write("};\n") -- Abstract class
structfile:write("}\n") -- Namespace
structfile:close()

local sqlfile = io.open(description.name .. "SQLite.sql", "w")
sqlfile:write(sql:generateInstallScriptSQLite(description))

sqlfile = io.open(description.name .. "MariaDB.sql", "w")
sqlfile:write(sql:generateInstallScriptMariaDB(description))

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

local pumlfile = io.open(description.name .. ".puml", "w")

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
