
local SQL = {}

function SQL:generateCreateFunction(file, name, tbl)

	file:write("\tvoid create" .. name .. "(struct " .. name .. "& self)\n\t{\n")
	file:write("\t\tstd::vector<std::string> args = {")

	local i = 0;
	for p,q in orderedPairs(tbl) do
		if q == "string" then
			file:write("self." .. p .. ", ")
		else
			file:write("std::to_string(self." .. p .. "), ")
		end
		i = i + 1
	end
	file:seek("cur", -2)

	file:write("};\n")

	file:write("\t\tm_connection->queryJson(")
	self:generateCreateStmt(file, name, tbl)
	file:write(", args);\n")

	file:write("\t\tself.id = m_connection->getLastInsertID();\n")
	file:write("\t}\n\n")

	file:write("\tvoid create(struct " .. name .. "& self) { create" .. name .. "(self);}\n\n")
end

function SQL:generateUpdateFunction(file, name, tbl)

	file:write("\tvoid update" .. name .. "(struct " .. name .. "& self)\n\t{\n")
	file:write("\t\tstd::vector<std::string> args = {")

	local i = 0;
	for p,q in orderedPairs(tbl) do

		if q == "string" then
			file:write("self." .. p .. ", ")
		else
			file:write("std::to_string(self." .. p .. "), ")
		end
		i = i + 1
	end
	file:seek("cur", -2)

	file:write(", std::to_string(self.id) };\n")

	file:write("\t\tm_connection->queryJson(")
	self:generateUpdateStmt(file, name, tbl)
	file:write(", args);\n")

	--file:write("\t\tself.id = " .. stmtName .. "->insert();\n")
	file:write("\t}\n\n")
	file:write("\tvoid update(struct " .. name .. "& self) { update" .. name .. "(self);}\n\n")
end

function SQL:generateDeleteFunction(file, name, tbl)
	file:write("\tvoid delete" .. name .. "(unsigned long long id)\n\t{\n")
	file:write("\t\tm_connection->queryJson(\"delete from `" .. name .. "` where id = ?;\", {std::to_string(id)});\n")
	file:write("\t}\n\n")

	file:write("\tvoid remove(struct " .. name .. "& self) { delete" .. name .. "(self.id);}\n\n")
end

function SQL:generateGetFunction(file, name, tbl)
	-- print("Generating get" .. name)
	file:write("\tbool get" .. name .. "(unsigned long long id, " .. name .. "& object)\n\t{\n")

	--file:write("\t\t" .. db:setStatementArg(stmtName, 0, "id", "uint64") .. "\n")
	file:write("\t\tluasqlgen::DatabaseResult result;\n")
	file:write("\t\tm_connection->query(\"select * from `" .. name .. "` where id = ?;\", {std::to_string(id)}, result);\n\n")

	file:write("\t\tif(result.empty()) return false;\n\n")

	file:write("\t\tauto& row = result[0];\n");
	file:write("\t\tobject.id = id;\n")

	for p,q in orderedPairs(tbl) do
		if q == "string" then
			file:write("\t\tobject." .. p .. " = std::move(row[\"" .. p .. "\"]);\n")
		else
			file:write("\t\tobject." .. p .. " = std::stoll(row[\"" .. p .. "\"]);\n")
		end
	end

	--file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
	file:write("\n\t\treturn true;\n\t}\n\n")
	file:write("\t bool get(unsigned long long id, struct " .. name .. "& self) { return get" .. name .. "(id, self);}\n\n")
end

function SQL:generateQueryFunction(file, name, tbl)
	file:write("\tvoid query" .. name .. "(std::vector<" .. name .. ">& out, ") -- "\n\t{\n")

	for p,q in orderedPairs(tbl) do
		file:write("const std::string& " .. p .. ", ")
	end
	file:seek("cur", -2)
	file:write(")\n\t{\n")

	file:write("\t\tstd::vector<std::string> args = {")
	local i = 0;
	for p,q in orderedPairs(tbl) do
		file:write(p .. ", ")
		i = i + 1
	end
	file:seek("cur", -2)
	file:write("};\n")

	file:write("\t\tluasqlgen::DatabaseResult result;\n")
	file:write("\t\tm_connection->query(");
	self:generateQueryStmt(file, name, tbl)
	file:write(", args, result);\n")

	file:write("\t\tfor(auto& row : result)\n\t\t{\n")
	file:write("\t\t\t" .. name .. " object;\n");
	file:write("\t\t\tobject.id = std::stoll(row[\"id\"]);\n")

	for p,q in orderedPairs(tbl) do
		if q == "string" then
			file:write("\t\t\tobject." .. p .. " = std::move(row[\"" .. p .. "\"]);\n")
		else
			file:write("\t\t\tobject." .. p .. " = std::stoll(row[\"" .. p .. "\"]);\n")
		end
	end
	file:write("\t\t\tout.push_back(object);\n")
	file:write("\t\t}\n")
	file:write("\t}\n\n")
end

function SQL:generateSearchFunction(file, name, tbl)

   file:write("\tvoid search" .. name .. "(std::vector<" .. name .. ">& out, const std::string& term)\n\t{\n")
   file:write([[
		std::string processedTerm = "%" + term + "%";
		std::replace(processedTerm.begin(), processedTerm.end(), ' ', '%');
		luasqlgen::DatabaseResult result;

]])

  file:write("\t\tstd::vector<std::string> args = {")
	local i = 0;
	for p,q in orderedPairs(tbl) do
		file:write("processedTerm, ")
		i = i + 1
	end
	file:seek("cur", -2)
	file:write("};\n")

	file:write("\t\tm_connection->query(");
	self:generateSearchStmt(file, name, tbl)
	file:write(", args, result);\n")

	file:write("\t\tfor(auto& row : result)\n\t\t{\n")
	file:write("\t\t\t" .. name .. " object;\n");
	file:write("\t\t\tobject.id = std::stoll(row[\"id\"]);\n")

	for p,q in orderedPairs(tbl) do
		if q == "string" then
			file:write("\t\t\tobject." .. p .. " = std::move(row[\"" .. p .. "\"]);\n")
		else
			file:write("\t\t\tobject." .. p .. " = std::stoll(row[\"" .. p .. "\"]);\n")
		end
	end
	file:write("\t\t\tout.push_back(object);\n")
	file:write("\t\t}\n")
	file:write("\t}\n\n")
end

function SQL:generateCreateStmt(file, name, tbl)
   -- print("Generating create" .. name .. "Stmt")

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
end

function SQL:generateUpdateStmt(file, name, tbl)
   file:write("\"update `" .. name .. "` set \"\n")

   local size = 0
   for p,q in orderedPairs(tbl) do
      file:write("\t\t\t\"`" .. p .. "` = ?,\"\n")
   end

   -- Delete the last ',' as it is not needed
   file:seek("cur", -3)
   file:write(" where `id` = ?;\"")
end

function SQL:generateQueryStmt(file, name, tbl)
   file:write("\"select * from `" .. name .. "` where ")

   for p,q in orderedPairs(tbl) do
      file:write("`" .. p .. "` like ? and ")
   end

   file:seek("cur", -5)
   file:write(";\"")
end

function SQL:generateSearchStmt(file, name, tbl)
   file:write("\"select * from `" .. name .. "` where ")

   for p,q in orderedPairs(tbl) do
      file:write("`" .. p .. "` like ? or ")
   end

   file:seek("cur", -4)
   file:write(";\"")
end

local mysqltypes = {
	string = "text",
	int = "int",
	uint = "int unsigned",
	int64 = "bigint",
	uint64 = "bigint unsigned",
	bool = "bool", -- FIXME: Byte?
	double = "double"
}

local function type2mysql(type)
	local result = mysqltypes[type]
	if result == nil then -- If type was not found, return an ID since it is most likely a table
	return "bigint unsigned"
	end
	return result
end

function SQL:generateInstallStmtMariaDB(tables, constraints)
	local result = ""
	for k,v in orderedPairs(tables) do
		-- SQL
		local stmt = ""
		stmt = stmt .. "create table if not exists `" .. k .. "` (\n\t`id` bigint unsigned primary key auto_increment"
		for p,q in orderedPairs(v) do
			 -- SQL
			 stmt = stmt .. ",\n\t`" .. p .. "` " .. type2mysql(q) .. " NOT NULL"

			 if constraints[k] and constraints[k][p] then
				stmt = stmt .. " check(" .. constraints[k][p] .. ")"
			 end
		end

		stmt = stmt .. ");\n\n"
		result = result .. "m_connection->query(\"" .. stmt:escape() .. "\");\n"
	end
	return result
end

local sqlitetypes = {
	string = "text",
	int = "int",
	uint = "int",
	int64 = "int",
	uint64 = "int",
	bool = "int", -- FIXME: Byte?
	double = "double"
}

local function type2sqlite(type)
	local result = sqlitetypes[type]
	if result == nil then -- If type was not found, return an ID since it is most likely a table
		return "int"
	end
	return result
end

function SQL:generateInstallStmtSQLite(tables, constraints)
	local result = ""
	for k,v in orderedPairs(tables) do
		-- SQL
		local stmt = ""
		stmt = stmt .. "create table if not exists `" .. k .. "` (\n\t`id` integer primary key autoincrement"

		for p,q in orderedPairs(v) do
			-- SQL
			stmt = stmt .. ",\n\t`" .. p .. "` " .. type2sqlite(q) .. " NOT NULL"

			if constraints[k] and constraints[k][p] then
				stmt = stmt .. " check(" .. constraints[k][p] .. ")"
			end
		end

		stmt = stmt .. ");\n\n"
		result = result .. "m_connection->query(\"" .. stmt:escape() .. "\");\n"
	end
	return result
end

function SQL:generateInstallScriptSQLite(description)                                                                                
	local result = [[
-- Generated by LuaSQL
-- SQLite table generation code.
-- create table `DBInfo` (version TEXT NOT NULL);
-- insert into `DBInfo` (version) values (']] .. tostring(description.version) .. [[');

]]
	for k,v in orderedPairs(description.tables) do
		-- SQL
		result = result .. "create table if not exists `" .. k .. "` (\n\t`id` integer primary key autoincrement"

		for p,q in orderedPairs(v) do
			-- SQL
			result = result .. ",\n\t`" .. p .. "` " .. type2sqlite(q) .. " NOT NULL"

			if description.constraints and description.constraints[k] and description.constraints[k][p] then
				result = result .. " check(" .. description.constraints[k][p] .. ")"
			end
		end

		result = result .. ");\n\n"
	end
	return result
end

function SQL:generateInstallScriptMariaDB(description)                                                                                
	local result = [[
-- Generated by LuaSQL
-- MariaDB table generation code.
-- create table `DBInfo` (version TEXT NOT NULL);
-- insert into `DBInfo` (version) values (']] .. tostring(description.version) .. [[');

]]
	for k,v in orderedPairs(description.tables) do
		-- SQL
		result = result .. "create table if not exists `" .. k .. "` (\n\t`id` integer primary key auto_increment"

		for p,q in orderedPairs(v) do
			-- SQL
			result = result .. ",\n\t`" .. p .. "` " .. type2mysql(q) .. " NOT NULL"

			if description.constraints and description.constraints[k] and description.constraints[k][p] then
				result = result .. " check(" .. description.constraints[k][p] .. ")"
			end
		end

		result = result .. ");\n\n"
	end
	return result
end
	                                                                                    
return SQL
