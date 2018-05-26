#ifndef LUASQLGEN_SQLITECONNECTION_H
#define LUASQLGEN_SQLITECONNECTION_H

#include "DatabaseConnection.h"
#include <sqlite3.h>
#include <exception>
#include <sstream>
#include <unordered_map>

namespace luasqlgen
{

class SQLiteStmt : public PreparedStmt
{
	sqlite3_stmt* m_stmt = nullptr;
	sqlite3* m_database = nullptr;
public:
	SQLiteStmt(sqlite3* db) : m_database(db) {}
	~SQLiteStmt() { if(m_stmt) { sqlite3_finalize(m_stmt); }}
	
	std::string queryJson(const std::vector<std::string> & args) override
	{
		if(!m_stmt) throw std::runtime_error("Statement was not built!");
		for(size_t i = 0; i < args.size(); i++)
		{
			sqlite3_bind_text(m_stmt, i + 1, args[i].c_str(), args[i].size(), nullptr);
		}

		std::stringstream ss;
		size_t colnum = sqlite3_column_count(m_stmt);
		int rc = 0;
		while(true) // TODO  Maybe row limit?
		{
			rc = sqlite3_step(m_stmt);
			if(rc == SQLITE_ROW)
			{
				ss << "{\n";
				for (size_t i = 0; i < colnum; i++)
				{
					const char* coltext = (const char*) sqlite3_column_text(m_stmt, i);
					ss << "\"" << sqlite3_column_name(m_stmt, i) << "\" : \"" << (coltext ? jsonEscape(coltext) : "") << (i == colnum - 1 ? "\"\n" : "\",\n");
				}
				
				ss << "},\n";
			}
			else
			{
				break;
			}
		}

		if(rc != SQLITE_DONE)
		{
			sqlite3_reset(m_stmt);
			throw std::runtime_error(std::string("Could not execute statement:") + sqlite3_errmsg(m_database) + "\n\nWith statement\n" + getSource()); 
		}

		sqlite3_reset(m_stmt);

		std::string result = ss.str();
		if(!result.empty())
			result.erase(result.end() - 2);

		return "[\n" + result + "]\n";
	}

	std::string queryJson() override
	{
		if(!m_stmt) throw std::runtime_error("Statement was not built!");
		std::stringstream ss;
		size_t colnum = sqlite3_column_count(m_stmt);
		int rc = 0;
		while(true) // TODO  Maybe row limit?
		{
			rc = sqlite3_step(m_stmt);
			if(rc == SQLITE_ROW)
			{
				ss << "{\n";
				for (size_t i = 0; i < colnum; i++)
				{
					const char* coltext = (const char*) sqlite3_column_text(m_stmt, i);
					ss << "\"" << sqlite3_column_name(m_stmt, i) << "\" : \"" << (coltext ? jsonEscape(coltext) : "") << (i == colnum - 1 ? "\"\n" : "\",\n");
				}
				
				ss << "},\n";
			}
			else
			{
				break;
			}
		}

		if(rc != SQLITE_DONE)
		{
			sqlite3_reset(m_stmt);
			throw std::runtime_error(std::string("Could not execute statement:") + sqlite3_errmsg(m_database) + "\n\nWith statement\n" + getSource()); 
		}

		sqlite3_reset(m_stmt);

		std::string result = ss.str();
		if(!result.empty())
			result.erase(result.end() - 2);

		return "[\n" + result + "]\n";
	}
	
	void query() override
	{
		if(!m_stmt) throw std::runtime_error("Statement was not built!");
		int rc = 0;
		while(true)
		{
			rc = sqlite3_step(m_stmt);
			if(rc != SQLITE_ROW)
				break;
		}

		if(rc != SQLITE_DONE)
		{
			sqlite3_reset(m_stmt);
			throw std::runtime_error(std::string("Could not execute statement:") + sqlite3_errmsg(m_database) + "\n\nWith statement\n" + getSource()); 
		}

		sqlite3_reset(m_stmt);
	}
	
	void query(const std::vector<std::string>& args, DatabaseResult& result) override
	{
		if(!m_stmt) throw std::runtime_error("Statement was not built!");
		for(size_t i = 0; i < args.size(); i++)
		{
			sqlite3_bind_text(m_stmt, i + 1, args[i].c_str(), args[i].size(), nullptr);
		}
		
		size_t colnum = sqlite3_column_count(m_stmt);
		int rc = 0;
		while(true) // TODO  Maybe row limit?
		{
			rc = sqlite3_step(m_stmt);
			if(rc == SQLITE_ROW)
			{
				std::unordered_map<std::string, std::string> row;
				for (size_t i = 0; i < colnum; i++)
				{
					row[sqlite3_column_name(m_stmt, i)] = reinterpret_cast<const char*>(sqlite3_column_text(m_stmt, i));
				}
				result.push_back(std::move(row));
			}
			else
			{
				break;
			}
			
		}

		if(rc != SQLITE_DONE)
		{
			sqlite3_reset(m_stmt);
			throw std::runtime_error(std::string("Could not execute statement:") + sqlite3_errmsg(m_database) + "\n\nWith statement\n" + getSource()); 
		}

		sqlite3_reset(m_stmt);
	}
	
	void build() override
	{
		if(m_stmt) throw std::runtime_error("Statement was already built!");
		if(sqlite3_prepare_v2(m_database, getSource().c_str(), -1, &m_stmt, 0) != SQLITE_OK) 
		{ 
			sqlite3_finalize(m_stmt);
			m_stmt = nullptr;
			throw std::runtime_error(std::string("Could not prepare statement:") + sqlite3_errmsg(m_database) + "\n\nWith statement\n" + getSource()); 
		}
	}
};
	
class SQLiteConnection : public DatabaseConnection
{
	std::unordered_map<std::string, std::shared_ptr<SQLiteStmt>> m_stmtCache;
	sqlite3* m_database;
	std::string m_databaseName;

public:
	~SQLiteConnection() { close(); }
	
	void connect(const std::string& db, const std::string& host = "", const std::string& socket = "",
			       const std::string& name = "", const std::string& password = "", const unsigned short port = 0) override
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

		sqlite3_busy_timeout(m_database, 1000);
		query("PRAGMA journal_mode=WAL");
	}
	
	std::shared_ptr<PreparedStmt> getStatement(const std::string& source) override
	{
		auto stmt = std::make_shared<SQLiteStmt>(m_database);
		stmt->buildSource(source);
		return stmt;
	}
	
	std::shared_ptr<PreparedStmt> getCachedStmt(const std::string& source)
	{
		auto stmtIter = m_stmtCache.find(source);
		if(stmtIter == m_stmtCache.end())
		{
			auto stmt = std::make_shared<SQLiteStmt>(m_database);
			stmt->buildSource(source);
			
			m_stmtCache[source] = stmt;
			return stmt;
		}
		
		return stmtIter->second;
	}
	
	std::string queryJson(const std::string& query, const std::vector<std::string>& args) override
	{
		return getCachedStmt(query)->queryJson(args);
	}

	std::string queryJson(const std::string & query) override
	{
		return getCachedStmt(query)->queryJson();
	}
	
	void query(const std::string& q) override
	{
		char* error = nullptr;
		if(sqlite3_exec(m_database, q.c_str(), nullptr, nullptr, &error) != SQLITE_OK)
			throw std::runtime_error(std::string("Could not access database: ") + error);
	}
	
	void query(const std::string& query, const std::vector<std::string>& args, DatabaseResult& result) override
	{
		getCachedStmt(query)->query(args, result);
	}
	
	void execute(const std::string & file) override
	{
		std::ifstream in(file);
		if(!in)
			throw std::runtime_error(std::string("Could not open SQL script file: ") + strerror(errno));

		std::stringstream buf;
		buf << in.rdbuf();

		query(buf.str());
	}
	
	void close() override
	{
		m_stmtCache.clear();
		sqlite3_close(m_database); 
		m_database = nullptr;
	}
	
	unsigned long long getLastInsertID() override
	{
		DatabaseResult result;
		query("select last_insert_rowid();", {}, result);
		return std::stoll(result[0].begin()->second);
	}
	
	const char* getName() const override { return "SQLite"; }
};

}

#endif
