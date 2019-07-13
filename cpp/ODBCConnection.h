#ifndef LUASQLGEN_ODBCCONNECTION_H
#define LUASQLGEN_ODBCCONNECTION_H

#include "DatabaseConnection.h"
#include <exception>
#include <unordered_map>

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

namespace luasqlgen
{

namespace
{
void throwODBCError(const std::string& msg, SQLHENV hEnv = nullptr, SQLHDBC hDbc = nullptr, SQLHSTMT hStmt = nullptr)
{
	SQLTCHAR szError[512];
	SQLTCHAR szSqlState[10];
	SQLINTEGER  nNativeError;
	SQLSMALLINT nErrorMsg;

	std::string error;

	if(hStmt)
	{
		while(SQLError(hEnv, hDbc, hStmt, szSqlState, &nNativeError, szError, sizeof(szError), &nErrorMsg) == SQL_SUCCESS)
		{
			error += (const char*) szError;
			error += "\n";
		}
	}

	if(hDbc)
	{
		while (SQLError(hEnv, hDbc, 0, szSqlState, &nNativeError, szError, sizeof(szError), &nErrorMsg) == SQL_SUCCESS)
		{
			error += (const char*) szError;
			error += "\n";
		}
	}

	if(hEnv)
	{
		while(SQLError(hEnv, 0, 0, szSqlState, &nNativeError, szError, sizeof(szError), &nErrorMsg) == SQL_SUCCESS)
		{
			error += (const char*) szError;
			error += "\n";
		}
	}

	throw std::runtime_error(msg + error);
}
}

class ODBCStmt : public PreparedStmt
{
	SQLHENV m_sql = nullptr;
	SQLHSTMT m_stmt = nullptr;
	SQLHDBC m_db = nullptr;
	
	void bindArgs(const std::vector<std::string>& args)
	{
		for(size_t i = 0; i < args.size(); i++)
		{
			SQLRETURN ret = SQLBindParameter(m_stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, args[i].size(), 0, (void*) args[i].c_str(), 0, nullptr);
			if(ret != SQL_SUCCESS)
				throwODBCError("Could not bind parameter: ", m_sql, m_db, m_stmt);
		}
	}
	
public:
	ODBCStmt(SQLHDBC env, SQLHDBC db):
		m_sql(env), m_db(db) {}
	
	~ODBCStmt()
	{
		if(m_stmt)
			SQLFreeHandle(SQL_HANDLE_STMT, m_stmt);
	}
	
	std::string queryJson(const std::vector<std::string> & args) override
	{
		if(!m_stmt) build();
		bindArgs(args);
		return queryJson();
	}

	std::string queryJson() override
	{
		if(!m_stmt) build();
		query();
		
		std::stringstream ss;
		SQLSMALLINT cols;
				
		if(SQLNumResultCols(m_stmt, &cols) != SQL_SUCCESS)
			throwODBCError("Could not determine the number of columns: ", m_sql, m_db, m_stmt);
		
		SQLRETURN ret;
		std::vector<SQLCHAR> dataBuf(4096);
		SQLCHAR colName[256];
		
		while((ret = SQLFetch(m_stmt)) == SQL_SUCCESS)
		{
			ResultLine entry;
			ss << "{\n";
			for(SQLSMALLINT i = 0; i < cols; i++)
			{
				if(SQLDescribeCol(m_stmt, i+1, colName, sizeof(colName),
					       nullptr, nullptr, nullptr, nullptr, nullptr) != SQL_SUCCESS)
				{
					throwODBCError("Could not get column name: ", m_sql, m_db, m_stmt);
				}
				
				if(SQLGetData(m_stmt, i+1, SQL_CHAR, dataBuf.data(), dataBuf.size(), nullptr) != SQL_SUCCESS)
				{
					throwODBCError("Could not get column name: ", m_sql, m_db, m_stmt);
				}
				
				ss << "\"" << (char*) colName << "\":\"" << jsonEscape((char*) dataBuf.data()) << "\"";
				if(i < cols-1)
					ss << ",";
				
				ss << "\n";
			}

			ss << "},\n";
		}
		
		SQLFreeStmt(m_stmt, SQL_CLOSE);
		
		std::string resultStr = ss.str();
		if(!resultStr.empty())
			resultStr.erase(resultStr.end() - 2);

		return "[\n" + resultStr + "]\n";
	}
	
	void query() override
	{
		if(SQLExecute(m_stmt) != SQL_SUCCESS)
			throwODBCError("Could not execute statement: ", m_sql, m_db, m_stmt);
	}
	
	void query(const std::vector<std::string>& args, DatabaseResult& dbresult) override
	{
		if(!m_stmt) build();
		bindArgs(args);
		query();
		
		SQLSMALLINT cols;
				
		if(SQLNumResultCols(m_stmt, &cols) != SQL_SUCCESS)
			throwODBCError("Could not determine the number of columns: ", m_sql, m_db, m_stmt);
		
		dbresult.reserve(32);
		
		SQLRETURN ret;
		std::vector<SQLCHAR> dataBuf(4096);
		SQLCHAR colName[256];
		
		while((ret = SQLFetch(m_stmt)) == SQL_SUCCESS)
		{
			ResultLine entry;
			for(SQLSMALLINT i = 0; i < cols; i++)
			{
				if(SQLDescribeCol(m_stmt, i+1, colName, sizeof(colName),
					       nullptr, nullptr, nullptr, nullptr, nullptr) != SQL_SUCCESS)
				{
					throwODBCError("Could not get column name: ", m_sql, m_db, m_stmt);
				}
				
				if(SQLGetData(m_stmt, i+1, SQL_CHAR, dataBuf.data(), dataBuf.size(), nullptr) != SQL_SUCCESS)
				{
					throwODBCError("Could not get column name: ", m_sql, m_db, m_stmt);
				}
				
				entry[std::string((char*) colName)] = std::string((char*) dataBuf.data());
			}
			
			dbresult.push_back(std::move(entry));
		}
		
		SQLFreeStmt(m_stmt, SQL_CLOSE);
	}
	
	void build() override
	{
		if(SQLAllocStmt(m_db, &m_stmt) != SQL_SUCCESS)
			throwODBCError("Could not allocate statement: ", m_sql, m_db, m_stmt);
		
		if(SQLPrepare(m_stmt, (unsigned char*) getSource().c_str(), SQL_NTS) != SQL_SUCCESS)
			throwODBCError("Could not prepare statement: ", m_sql, m_db, m_stmt);
	}
};
	
// https://www.easysoft.com/developer/languages/c/odbc_tutorial.html#connect
class ODBCConnection : public DatabaseConnection
{
	std::unordered_map<std::string, std::shared_ptr<ODBCStmt>> m_stmtCache;
	SQLHENV m_sql;
	SQLHDBC m_db;
	
	void reconnect()
	{
		
	}

public:
	~ODBCConnection()
	{
		m_stmtCache.clear();
		
		if(m_db)
		{
			SQLDisconnect(m_sql);
			SQLFreeHandle(SQL_HANDLE_DBC, m_db);
		}
		
		if(m_sql)
			SQLFreeHandle(SQL_HANDLE_ENV, m_sql);
	}
	
	void connect(const std::string& db, const std::string& host, const std::string& socket,
			       const std::string& name, const std::string& password, const unsigned short port) override
	{
		SQLRETURN ret;
		
		SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_sql);
		SQLSetEnvAttr(m_sql, SQL_ATTR_ODBC_VERSION, (void*) SQL_OV_ODBC3, 0);
		SQLAllocConnect(m_sql, &m_db);
		
		std::string connectionStr = "DSN=" + db + ";";
		if(!name.empty())
			connectionStr += "UID=" + name + ";PWD=" + password + ";";
		
		ret = SQLDriverConnect(m_db, nullptr, (unsigned char*) connectionStr.c_str(), SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_COMPLETE);
		
		if(!SQL_SUCCEEDED(ret))
			throwODBCError("Could not connect to database: ", m_sql, m_db);
	}
	
	std::shared_ptr<PreparedStmt> getStatement(const std::string& source) override
	{
		auto stmt = std::make_shared<ODBCStmt>(m_sql, m_db);
		stmt->buildSource(source);
		return stmt;
	}
	
	std::shared_ptr<PreparedStmt> getCachedStmt(const std::string& source)
	{
		auto stmtIter = m_stmtCache.find(source);
		if(stmtIter == m_stmtCache.end())
		{
			auto stmt = std::make_shared<ODBCStmt>(m_sql, m_db);
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
		
		SQLHSTMT stmt;
		SQLAllocStmt(m_db, &stmt);
		
		if(SQLExecDirect(stmt, (unsigned char*) buf.str().c_str(), SQL_NTS) != SQL_SUCCESS)
		{
			SQLFreeStmt(stmt, SQL_DROP);
			throwODBCError("Could not execute SQL script: ", m_sql, m_db, stmt);
		}
		
		SQLFreeStmt(stmt, SQL_DROP);
	}
	
	void close() override
	{
		
	}
	
	unsigned long long getLastInsertID() override
	{

	}
	
	const char* getName() const override { return "ODBC"; }
	DBTYPE getType() const override { return ODBC; }
};

}

#endif
