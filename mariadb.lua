local common = dofile(scriptPath() .. "common.lua")

local mariadbpptypes = {
   string = "string",
   int = "signed32",
   uint = "unsigned32",
   int64 = "signed64",
   uint64 = "unsigned64",
   bool = "unsigned32", -- FIXME: Byte?
   double = "double"
}

local function type2mariadbpp(type)
   local result = mariadbpptypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
      return "unsigned64"
   end
   return result
end

local mariadbpptypes = {
   string = "string",
   int = "signed32",
   uint = "unsigned32",
   int64 = "signed32", -- FIXME: 64bit variables are 32bit as of now! Prevents some corruption. Possibly a bug in MariaDBPP
   uint64 = "unsigned32",
   bool = "unsigned32", -- FIXME: Byte?
   double = "double"
}

local function type2mariadbpp(type)
   local result = mariadbpptypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
      return "unsigned64"
   end
   return result
end

local mysqltypes = {
   string = "text",
   int = "bigint",
   uint = "bigint unsigned",
   int64 = "bigint",
   uint64 = "bigint unsigned",
   bool = "bool", -- FIXME: Byte?
   double = "double"
}

function type2mysql(type)
   local result = mysqltypes[type]
   if result == nil then -- If type was not found, return an ID since it is most likely a table
      return "bigint"
   end
   return result
end

local MariaDB = {}

function MariaDB:process(filename)
   local description = dofile(filename)
   self:processStructs(description)
end

function MariaDB:generateConnectionGuard()
   return [[if(!m_connection->connected()) 
		{ 
			m_connection->connect(); 
			m_connection->set_auto_commit(true); 
			init(m_connection->schema());
		}
]]
end

function MariaDB:generateStatement(name)
   return "mariadb::statement_ref " .. name .. ";"
end

function MariaDB:setStatementArg(name, idx, varname, typedef)
   return name .. "->set_" .. type2mariadbpp(typedef) .. "(" .. idx .. ", " .. varname .. ");"
end

function MariaDB:generateInsert(name, targetid)
   return targetid .. " = " .. name .. "->insert();"
end	  

function MariaDB:generateExecute(name, targetid)
   return name .. "->execute();"
end	  

function MariaDB:getStatementResult(resultname, name, varname, typedef)
   return varname .. " = " .. resultname .. "->get_" .. type2mariadbpp(typedef) .. "(\"" .. name .. "\");"
end

function MariaDB:generateQuery(name, varname)
   return "mariadb::result_set_ref " .. varname .. " = " .. name .. "->query();"
end

function MariaDB:generateQueryFetchFirst(name, varname)
   return "mariadb::result_set_ref " .. varname .. " = " .. name .. "->query();\n\n" ..
   [[
		if(]]..varname..[[->row_count() == 0 || !]]..varname..[[->next())
			return false;
]]
end

function MariaDB:generateRowLoop(resultname)
   return "for(unsigned int j = 0; j < " .. resultname .. "->row_count() && " .. resultname .. "->next(); j++)"
end

function MariaDB:beginStatement(name)
   return name .. " = m_connection->create_statement("
end

function MariaDB:endStatement(name)
   return ");"
end

function MariaDB:generateStmtReset(name)
   return ""
end

function MariaDB:processStructs(description)
   -- print("Generating structs...")
   local file = io.open(description.name .. "MariaDB.h", "w")
   local sqlfile = io.open(description.name .. "MariaDB.sql", "w")
   local tables = description.tables

   sqlfile:write([[
-- Generated by LuaSQL
-- MySQL table generation code. Execute on a created database to fill it.
create table `DBInfo` (version TEXT NOT NULL);
insert into `DBInfo` (version) values (']] .. tostring(description.version) .. [[');
]])

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

]])

   for k,v in orderedPairs(tables) do

      -- SQL
      sqlfile:write("create table `" .. k .. "` (\n\t`id` int primary key auto_increment")
    
      for p,q in orderedPairs(v) do
	 -- SQL
	 sqlfile:write(",\n\t`" .. p .. "` " .. type2mysql(q) .. " NOT NULL")
      end

      sqlfile:write(");\n\n")
   end


   file:write(
      "\n\nclass " .. description.name .. "MariaDB : public " .. description.name .. "Abstract"
              .. "\n{\npublic:\n\t" .. description.name  ..
	 [[
MariaDB(const std::string& db, const std::string& host, const std::string& name,
				const std::string& password, const unsigned short port)
	{
    	connect(db, host, name, password, port);
    }

   const std::string dbFile = "]] .. description.name .. [[MariaDB.sql";

]])

   -- Empty default constructor
   file:write("\t" .. description.name .. "MariaDB() {}\n\n")

   file:write("\t~" .. description.name .. "MariaDB()\n\t{\n")
   file:write([[
		  if(m_connection != nullptr && m_connection->connected())
		  m_connection->disconnect();
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

		void connect(const std::string& db, const std::string& host,
			       const std::string& name, const std::string& password, const unsigned short port)
		{
			mariadb::account_ref account = mariadb::account::create(host, name, password, "", port);
			account->set_auto_commit(true);

			m_connection = mariadb::connection::create(account);
			if(!m_connection->connect())
				throw std::runtime_error("Could not connect to MariaDB database: " + m_connection->error());
			
			m_connection->execute("CREATE DATABASE IF NOT EXISTS " + db + ";");
			m_connection->set_schema(db);
		}

		void execute(const std::string& file)
		{
			std::ifstream in(file);
			if(!in)
				throw std::runtime_error("Could not open SQL script file!");

			std::stringstream buf;
			buf << in.rdbuf();

			]] .. MariaDB:generateConnectionGuard() .. [[

			m_connection->execute(buf.str());
		}

		void init(const std::string& db)
		{
			// Select database
			m_connection->execute("USE " + m_connection->schema() + ";");
			
			// Check if tables exist or not
			if(m_connection->query("show tables like 'DBInfo';")->row_count() == 0)
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
	m_connection->execute("drop database " + m_connection->schema() + ";");
}

void close()
{
	m_connection->disconnect();
}

void query(const std::string& q)
{
	]] .. MariaDB:generateConnectionGuard() .. [[
	m_connection->execute(q);
}

std::string queryJson(const std::string& query) override
{
	std::stringstream ss;
	ss << "[\n";
	
	]] .. MariaDB:generateConnectionGuard() .. [[
	
	mariadb::result_set_ref result = m_connection->query(query);
	for(unsigned int j = 0; j < result->row_count() && result->next(); j++)
	{
		for(unsigned int i = 0; i < result->column_count(); i++)
			ss << "\"" << result->column_name(i) << "\" : \"" << result->get_string(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
	}
	
	ss << "]\n";
	return ss.str();
}

std::string queryJson(const std::string& query, const std::vector<std::string>& args)
{
	]] .. MariaDB:generateConnectionGuard() .. [[
	
	mariadb::statement_ref stmt = m_connection->create_statement(query);
	
	for(size_t i = 0; i < args.size(); i++)
		stmt->set_string(i, args[i]);
	
	std::stringstream ss;

	mariadb::result_set_ref result = stmt->query();
	for(unsigned int j = 0; j < result->row_count() && result->next(); j++)
	{
                ss << "{\n";
		for(unsigned int i = 0; i < result->column_count(); i++)
                {
                        switch(result->column_type(i))
                        {
                                case mariadb::value::string:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_string(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::unsigned8:
			                ss << "\"" << result->column_name(i) << "\" : \"" << static_cast<unsigned short>(result->get_unsigned8(i)) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::unsigned16:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_unsigned16(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::unsigned32:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_unsigned32(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::unsigned64:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_unsigned64(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::signed8:
			                ss << "\"" << result->column_name(i) << "\" : \"" << static_cast<short>(result->get_signed8(i)) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::signed16:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_signed16(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::signed32:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_signed32(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::signed64:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_signed64(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::float32:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_float(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;
                                case mariadb::value::double64:
			                ss << "\"" << result->column_name(i) << "\" : \"" << result->get_double(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                                break;

                               case mariadb::value::blob:
                                        ss << "\"" << result->column_name(i) << "\" : \"" << result->get_string(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
                               break;

                                default: throw std::runtime_error(std::string("Received unknown type from MariaDB! (") 
                                        + result->column_name(i) + " is "
                                        + std::to_string(result->column_type(i)) + ")");
                        }
                }

                ss << "},\n";
	}
	
        std::string resultStr = ss.str();
	if(!resultStr.empty())
		resultStr.erase(resultStr.end() - 2);

	return "[\n" + resultStr + "]\n";
}

mariadb::connection_ref getConnection() const { return m_connection; }

private:
mariadb::connection_ref m_connection;

]])

file:write("};\n}\n") -- Close file and namespace
file:close()
end

return MariaDB
