#ifndef LUASQLGEN_MARIADBCONNECTION_H
#define LUASQLGEN_MARIADBCONNECTION_H

#include "DatabaseConnection.h"
#include <mariadb++/connection.hpp>
#include <exception>
#include <unordered_map>

namespace luasqlgen
{

class MariaDBStmt : public PreparedStmt
{
	mariadb::connection_ref m_connection;
	mariadb::statement_ref m_stmt;
	
	void translateType(std::stringstream& ss, const mariadb::result_set_ref& result, size_t i)
	{
		switch(result->column_type(i))
		{
			case mariadb::value::string:
				ss << "\"" << result->column_name(i) << "\" : \"" << jsonEscape(result->get_string(i)) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
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
			
			case mariadb::value::decimal:
				ss << "\"" << result->column_name(i) << "\" : \"" << result->get_decimal(i).double64() << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
			break;
			
			case mariadb::value::double64:
				ss << "\"" << result->column_name(i) << "\" : \"" << result->get_double(i) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
			break;

			case mariadb::value::blob:
				ss << "\"" << result->column_name(i) << "\" : \"" << jsonEscape(result->get_string(i)) << (i == result->column_count() - 1 ? "\"\n" : "\",\n");
			break;

			default: throw std::runtime_error(std::string("Received unknown type from MariaDB! (") 
				+ result->column_name(i) + " is "
				+ std::to_string(result->column_type(i)) + ")");
		}	
	}
	
	std::string toString(const mariadb::result_set_ref& result, size_t i)
	{
		if(result->get_is_null(i)) return "";
		
		switch(result->column_type(i))
		{
			case mariadb::value::null:
				return "";
			case mariadb::value::blob:
			case mariadb::value::string:
				return result->get_string(i);
			case mariadb::value::unsigned8:
				return std::to_string(static_cast<unsigned short>(result->get_unsigned8(i)));
			case mariadb::value::unsigned16:
				return std::to_string(result->get_unsigned16(i));
			case mariadb::value::unsigned32:
				return std::to_string(result->get_unsigned32(i));
			case mariadb::value::unsigned64:
				return std::to_string(result->get_unsigned64(i));
			case mariadb::value::signed8:
				return std::to_string(static_cast<short>(result->get_signed8(i)));
			case mariadb::value::signed16:
				return std::to_string(result->get_signed16(i));
			case mariadb::value::signed32:
				return std::to_string(result->get_signed32(i));
			case mariadb::value::signed64:
				return std::to_string(result->get_signed64(i));
			case mariadb::value::float32:
				return std::to_string(result->get_float(i));
			case mariadb::value::double64:
				return std::to_string(result->get_double(i));
			case mariadb::value::decimal:
				return std::to_string(result->get_decimal(i).double64());
			
			default: throw std::runtime_error(std::string("Received unknown type from MariaDB! (") 
				+ result->column_name(i) + " is "
				+ std::to_string(result->column_type(i)) + ")");
		}	
	}
	
public:
	MariaDBStmt(const mariadb::connection_ref& conn):
		m_connection(conn) {}
	
	std::string queryJson(const std::vector<std::string> & args) override
	{
		if(!m_stmt) build();
		
		for(size_t i = 0; i < args.size(); i++)
			m_stmt->set_string(i, args[i]);
		
		std::stringstream ss;

		mariadb::result_set_ref result = m_stmt->query();
		for(unsigned int j = 0; j < result->row_count() && result->next(); j++)
		{
			ss << "{\n";
			for(unsigned int i = 0; i < result->column_count(); i++)
			{
				translateType(ss, result, i);
			}

			ss << "},\n";
		}
		
		std::string resultStr = ss.str();
		if(!resultStr.empty())
			resultStr.erase(resultStr.end() - 2);

		return "[\n" + resultStr + "]\n";
	}

	std::string queryJson() override
	{
		if(!m_stmt) build();
		std::stringstream ss;
	
		mariadb::result_set_ref result = m_stmt->query();
		for(unsigned int j = 0; j < result->row_count() && result->next(); j++)
		{
			ss << "{\n";
			for(unsigned int i = 0; i < result->column_count(); i++)
			{
				translateType(ss, result, i);
			}
			ss << "},\n";
		}
		
		std::string resultStr = ss.str();
		if(!resultStr.empty())
			resultStr.erase(resultStr.end() - 2);
		
		return "[\n" + resultStr + "]\n";
	}
	
	void query() override
	{
		if(!m_stmt) build();
		m_stmt->query();
	}
	
	void query(const std::vector<std::string>& args, DatabaseResult& dbresult) override
	{
		if(!m_stmt) build();
		
		for(size_t i = 0; i < args.size(); i++)
			m_stmt->set_string(i, args[i]);
		
		mariadb::result_set_ref result = m_stmt->query();
		for(unsigned int j = 0; j < result->row_count() && result->next(); j++)
		{
			std::unordered_map<std::string, std::string> row;
			for(unsigned int i = 0; i < result->column_count(); i++)
			{
				row[result->column_name(i)] = toString(result, i);
			}
			dbresult.push_back(std::move(row));
		}
	}
	
	void build() override
	{
		m_stmt = m_connection->create_statement(getSource());
	}
};
	
class MariaDBConnection : public DatabaseConnection
{
	std::unordered_map<std::string, std::shared_ptr<MariaDBStmt>> m_stmtCache;
	mariadb::connection_ref m_connection;
	
	void reconnect()
	{
		if(!m_connection->connected()) 
		{
			// Reconnect
			m_connection->disconnect();
			m_connection->connect(); 
			m_connection->set_auto_commit(true); 
			m_connection->execute("use " + m_connection->schema() + ";");
			
			// Rebuild all statements
			for(auto& k : m_stmtCache)
			{
				k.second->build();
			}
		}
	}
	
public:
	void connect(const std::string& db, const std::string& host, const std::string& socket,
			       const std::string& name, const std::string& password, const unsigned short port) override
	{
		mariadb::account_ref account = mariadb::account::create(host, name, password, "", port, socket);
		account->set_auto_commit(true);

		m_connection = mariadb::connection::create(account);
		if(!m_connection->connect())
			throw std::runtime_error("Could not connect to MariaDB database: " + m_connection->error());
		
		m_connection->execute("create database if not exists " + db + ";");
		m_connection->set_schema(db);
	}
	
	std::shared_ptr<PreparedStmt> getStatement(const std::string& source) override
	{
		auto stmt = std::make_shared<MariaDBStmt>(m_connection);
		stmt->buildSource(source);
		return stmt;
	}
	
	std::shared_ptr<PreparedStmt> getCachedStmt(const std::string& source)
	{
		auto stmtIter = m_stmtCache.find(source);
		if(stmtIter == m_stmtCache.end())
		{
			auto stmt = std::make_shared<MariaDBStmt>(m_connection);
			stmt->buildSource(source);
			
			m_stmtCache[source] = stmt;
			return stmt;
		}
		
		return stmtIter->second;
	}
	
	std::string queryJson(const std::string& query, const std::vector<std::string>& args) override
	{
		reconnect();
		return getCachedStmt(query)->queryJson(args);
	}

	std::string queryJson(const std::string & query) override
	{
		reconnect();
		return getCachedStmt(query)->queryJson();
	}
	
	void query(const std::string& q) override
	{
		reconnect();
		getCachedStmt(q)->query();
	}
	
	void query(const std::string& query, const std::vector<std::string>& args, DatabaseResult& result) override
	{
		reconnect();
		getCachedStmt(query)->query(args, result);
	}
	
	void execute(const std::string & file) override
	{
			std::ifstream in(file);
			if(!in)
				throw std::runtime_error("Could not open SQL script file!");

			std::stringstream buf;
			buf << in.rdbuf();

			reconnect();
			m_connection->execute(buf.str());
	}
	
	void close() override
	{
		m_connection->disconnect();
	}
	
	unsigned long long getLastInsertID() override
	{
		DatabaseResult result;
		query("select LAST_INSERT_ID();", {}, result);
		return std::stoll(result[0].begin()->second);
	}
	
	const char* getName() const override { return "MariaDB"; }
};

}

#endif
