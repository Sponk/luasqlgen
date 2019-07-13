#ifndef LUASQLGEN_DATABASECONNECTION_H
#define LUASQLGEN_DATABASECONNECTION_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <sstream>
#include <iomanip>
#include <fstream>

namespace luasqlgen
{

enum DBTYPE
{
	SQLITE = 0,
	MARIADB,
	ODBC
};

typedef std::unordered_map<std::string, std::string> ResultLine;
typedef std::vector<ResultLine> DatabaseResult;

class DatabaseConnection;
class PreparedStmt
{
	std::string m_sources;
	
public:
	// Escapes JSON strings using STL only.
	// FIXME Is this fast enough?
	// Adapted from: https://stackoverflow.com/questions/7724448/simple-json-string-escape-for-c#7725289
	static inline std::string jsonEscape(const std::string &s)
	{
		std::stringstream ss;
		for(auto& c : s)
		{
			switch(c)
			{
				case '"': ss << "\\\""; break;
				case '\\': ss << "\\\\"; break;
				case '\b': ss << "\\b"; break;
				case '\f': ss << "\\f"; break;
				case '\n': ss << "\\n"; break;
				case '\r': ss << "\\r"; break;
				case '\t': ss << "\\t"; break;
				default:
					if ('\x00' <= c && c <= '\x1f')
					{
						ss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << *((int32_t*) &c); // TODO Ensure the size of the string is big enough!
					}
					else
					{
						ss << c;
					}
			}
		}
		return ss.str();
	}
	
	virtual ~PreparedStmt() {}
	virtual std::string queryJson(const std::vector<std::string>& args) = 0;
	virtual std::string queryJson() = 0;
	virtual void query() = 0;
	virtual void query(const std::vector<std::string>& args, DatabaseResult& result) = 0;

	virtual void build() = 0;
	void buildSource(const std::string& source)
	{
		m_sources = source;
		build();
	}
	
	std::string getSource() const { return m_sources; }
};

class DatabaseConnection
{

public:
	virtual ~DatabaseConnection() = default;
	virtual void connect(const std::string& db, const std::string& host = "", const std::string& socket = "",
				const std::string& name = "", const std::string& password = "", const unsigned short port = 0) = 0;
		
	virtual void execute(const std::string& file) = 0;
	virtual void close() = 0;	
	virtual void query(const std::string& q) = 0;
	virtual void query(const std::string& query, const std::vector<std::string>& args, DatabaseResult& result) = 0;

	virtual std::string queryJson(const std::string& query) = 0;
	virtual std::string queryJson(const std::string& query, const std::vector<std::string>& args) = 0;
	
	virtual std::shared_ptr<PreparedStmt> getStatement(const std::string& source) = 0;
	virtual unsigned long long getLastInsertID() = 0;
	virtual const char* getName() const = 0;
	virtual DBTYPE getType() const = 0;
};

}

#endif
