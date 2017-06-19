local common = dofile(scriptPath() .. "common.lua")

local sqlitetypes = {
   string = "text",
   int = "int",
   uint = "int",
   int64 = "int",
   uint64 = "int",
   bool = "int" -- FIXME: Byte?
}

local function type2sqlite(type)
   local result = sqlitetypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
      return "int"
   end
   return result
end

local sqlitesqltypes = {
   string = "text",
   int = "int",
   uint = "int unsigned",
   int64 = "bigint",
   uint64 = "bigint unsigned",
   bool = "bool" -- FIXME: Byte?
}

function type2sqlitesql(type)
   local result = sqlitesqltypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
      return "bigint"
   end
   return result
end

local SQLite = {}

function SQLite:process(filename)
   local description = dofile(filename)
   self:processStructs(description)
end

function SQLite:generateStatement(name)
   return "sqlite3_stmt* " .. name .. ";"
end

function SQLite:setStatementArg(name, idx, varname, typedef)

   if(typedef ~= "string") then
      return "sqlite3_bind_" .. type2sqlite(typedef) .. "(" .. name .. ", " .. idx + 1 .. ", " .. varname .. ");"
   else
      return "sqlite3_bind_text(" .. name .. ", " .. idx + 1 .. ", " .. varname .. ".c_str(), " .. varname .. ".size(), nullptr);"
   end
end

function SQLite:generateInsert(name, targetid)
   return "{ int err = 0;\n"
      .. "while((err = sqlite3_step(" .. name.. ")) == SQLITE_BUSY) \n{\n}\n\n" ..
[[
    if(err != SQLITE_OK && err != SQLITE_DONE)
    {
      sqlite3_reset(]].. name .. [[);
      throw std::runtime_error(std::string("Could not insert: ") + sqlite3_errmsg(m_database));
    }
 ]] .. targetid .. [[ = sqlite3_last_insert_rowid(m_database);
}
]]
end

function SQLite:generateExecute(name)
   return "while(sqlite3_step(" .. name.. ") == SQLITE_BUSY) \n{\n}"
end	  

function SQLite:getStatementResult(resultname, name, varname, typedef, stmt)

   if typedef ~= "string" then

   return [[if(]] .. resultname .. [[ == SQLITE_ROW)
{
]] .. varname .. " = sqlite3_column_" .. type2sqlite(typedef) .. "(" .. stmt ..
   ", getColumnIndex(" .. stmt .. ", \"" .. name .. "\"));" ..
[[}]]
   else
      return [[if(]] .. resultname .. [[ == SQLITE_ROW)
{
]] .. varname .. " = ((const char*) sqlite3_column_text(" .. stmt ..
              ", getColumnIndex(" .. stmt .. ", \"" .. name .. "\")));" ..
              [[}]]
   end
end

function SQLite:generateQuery(name, varname)
   return "int " .. varname .. ";" -- = sqlite3_step(" .. name.. ");" ..
      --  [[
		--	if(]] .. varname .. [[ != SQLITE_OK && ]] .. varname .. [[ != SQLITE_DONE)
		--	  throw std::runtime_error(std::string("Could not query: ") + sqlite3_errmsg(m_database));
	--	]]
end

function SQLite:generateQueryFetchFirst(name, varname)
   return "int " .. varname .. ";\n"
      .. "while((" .. varname .. " = sqlite3_step(" .. name.. ")) == SQLITE_BUSY) \n{\n}\n\n" ..
   [[
		if(]]..varname..[[ != SQLITE_ROW)
		{
		    sqlite3_reset(]] .. name .. [[);
			return false;
		}
]]
end

function SQLite:generateRowLoop(resultname, name)
   --return "for(unsigned int j = 0; j < " .. resultname .. "->row_count() && " .. resultname .. "->next(); j++)"

   return [[while((]] .. resultname .. [[ = sqlite3_step(]] .. name ..[[)) == SQLITE_ROW)]]
end

function SQLite:beginStatement(name)
   return "{int err = sqlite3_prepare_v2(m_database, "
end

function SQLite:endStatement(name)
   return ", -1, &" .. name .. ", 0); " 
      .. "if(err != SQLITE_OK) { sqlite3_reset(" .. name .. "); throw "
      .. "std::runtime_error(std::string(\"Could not prepare statement:\") +" ..
					      "sqlite3_errmsg(m_database));} }"

end

function SQLite:generateStmtReset(name)
   return "sqlite3_reset(" .. name .. ");"
end

function SQLite:processStructs(description)
   print("Generating structs...")
   local file = io.open(description.name .. "SQLite.h", "w")
   local sqlfile = io.open(description.name .. "SQLite.sql", "w")
   local tables = description.tables

   sqlfile:write([[
-- Generated by LuaSQL
-- SQLite table generation code.
create table `DBInfo` (version TEXT NOT NULL);
insert into `DBInfo` (version) values (']] .. tostring(description.version) .. [[');

]])

   file:write([[
// Generated by LuaSQL
#pragma once

#include <string>
#include <cstdint>
#include <sqlite3.h>
#include <exception>
#include <vector>
#include <fstream>
#include "]] .. description.name .. [[.h"
namespace ]] .. description.name .. "\n{\n")

   for k,v in orderedPairs(tables) do

      -- SQL
      sqlfile:write("create table `" .. k .. "` (\n\t`id` integer primary key autoincrement")
      
      for p,q in orderedPairs(v) do
	 -- SQL
	 sqlfile:write(",\n\t`" .. p .. "` " .. type2sqlitesql(q) .. " NOT NULL")
      end

      sqlfile:write(");\n\n")
   end


   file:write(
      "\n\nclass " .. description.name .. "SQLite\n{\npublic:\n\t" .. description.name  ..
	 [[
SQLite(const std::string& db, const std::string& host, const std::string& name,
				const std::string& password, const unsigned short port)
	{
    	connect(db, host, name, password, port);
    }

   const std::string dbFile = "]] .. description.name .. [[SQLite.sql";
]])

   -- Empty default constructor
   file:write("\t" .. description.name .. "SQLite() {}\n\n")

   file:write("\t~" .. description.name .. "SQLite()\n\t{\n")
   file:write([[
		  if(m_database != nullptr)
		  close();
	       ]])
   file:write("\t}\n\n")

   for k,v in orderedPairs(tables) do
      common:generateCreateFunction(self, file, k, v)
      common:generateGetFunction(self, file, k, v)
      common:generateUpdateFunction(self, file, k, v)
      common:generateDeleteFunction(self, file, k, v)
      common:generateQueryFunction(self, file, k, v)
      common:generateSearchFunction(self, file, k, v)
   end
  
   file:write([[

		  void connect(const std::string& db, const std::string&,
			       const std::string&, const std::string&, const unsigned short)
		  {
		     //sqlite3_shutdown();
		     //if(sqlite3_config(SQLITE_CONFIG_SERIALIZED) != SQLITE_OK)
		     //throw std::runtime_error(std::string("Could not configure thread safety: ") +
		//			      sqlite3_errmsg(m_database));
		     
		  //   sqlite3_initialize();
		     if(db != ":memory:")
			m_databaseName = db + ".db";
		     else
			     m_databaseName = ":memory:";
			     
		     if (sqlite3_open(m_databaseName.c_str(), &m_database))
		     throw std::runtime_error(std::string("Could not open database: ") +
					      sqlite3_errmsg(m_database));
		     
		     if(sqlite3_threadsafe() == 0)
		     std::cerr << "SQlite is not compiled as thread safe!" << std::endl;

		     query("PRAGMA journal_mode=WAL");
		  }

		  void execute(const std::string& file)
		  {
		     std::ifstream in(file);
		     if(!in)
		     throw std::runtime_error(std::string("Could not open SQL script file: ") + strerror(errno));

		     std::stringstream buf;
		     buf << in.rdbuf();

		     query(buf.str());
		  }

		  void init(const std::string& db)
		  {
		     // Check if tables exist or not
			if(!tableExists("DBInfo"))
		     execute(db);

	       ]])

   for k,v in orderedPairs(tables) do
      common:generateCreateStmt(self, file, k, v)
      common:generateUpdateStmt(self, file, k, v)
      common:generateDeleteStmt(self, file, k, v)
      common:generateQueryStmt(self, file, k, v)
      common:generateGetStmt(self, file, k, v)
      common:generateSearchStmt(self, file, k, v)
   end

   file:write([[	}

void drop()
{
   close();
   remove(m_databaseName.c_str());
}

void close()
{
]])

for k,v in orderedPairs(tables) do
	file:write("\tsqlite3_finalize(create" .. k .. "Stmt);\n")
	file:write("\tsqlite3_finalize(update" .. k .. "Stmt);\n")
	file:write("\tsqlite3_finalize(delete" .. k .. "Stmt);\n")
	file:write("\tsqlite3_finalize(query" .. k .. "Stmt);\n")
	file:write("\tsqlite3_finalize(get" .. k .. "Stmt);\n")
	file:write("\tsqlite3_finalize(search" .. k .. "Stmt);\n")
end

file:write([[
   sqlite3_close(m_database); 
   m_database = nullptr;
}

void query(const std::string& q)
{
   char* error = nullptr;
   if(sqlite3_exec(m_database, q.c_str(), nullptr, nullptr, &error) != SQLITE_OK)
   throw std::runtime_error(std::string("Could not access database: ") +
			    error);
}

private:

bool tableExists(const std::string& name)
{
	std::stringstream query;
	query << "SELECT name FROM sqlite_master WHERE type='table' AND name='"
		  << name << "'";

	const auto callback = [](void* data, int argc, char**,
							 char**) {
		bool* result = reinterpret_cast<bool*>(data);
		*result = (argc > 0);
		return 0;
	};

	char* error = nullptr;
        bool result = false;
	if (sqlite3_exec(m_database, query.str().c_str(), callback, &result, &error) != SQLITE_OK)
		throw std::runtime_error(std::string("Could not access database: ") +
								 error);
	return result;
}
// @todo: This is very _very_ inefficient! Results should be cached in a static std::map or similar
unsigned int getColumnIndex(sqlite3_stmt* stmt, const std::string& name)
{
	for(int i = 0; i < sqlite3_column_count(stmt); i++)
	{
	  const char* colname = sqlite3_column_name(stmt, i);
	  if(name == colname)
	    return i;
	}

	return -1;
}

sqlite3* m_database;
std::string m_databaseName;

]])

file:write("};\n}\n") -- Close file and namespace
file:close()
end

return SQLite
