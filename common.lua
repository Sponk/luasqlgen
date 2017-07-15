
local Common = {}

function Common:generateCreateFunction(db, file, name, tbl)
   -- print("Generating create" .. name)
   local stmtName = "create" .. name .. "Stmt"
   
   file:write("\n\t".. db:generateStatement(stmtName) .. "\n")
   file:write("\tvoid create" .. name .. "(struct " .. name .. "& self)\n\t{\n")
   
   local i = 0;
   for p,q in orderedPairs(tbl) do
      file:write("\t\t" .. db:setStatementArg(stmtName, i, "self." .. p, q) .. "\n")
      i = i + 1
   end

   file:write("\t\t" .. db:generateInsert(stmtName, "self.id") .. "\n")
   file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
   --file:write("\t\tself.id = " .. stmtName .. "->insert();\n")
   file:write("\t}\n")
end

function Common:generateUpdateFunction(db, file, name, tbl)
   -- print("Generating update" .. name)
   local stmtName = "update" .. name .. "Stmt"

   file:write("\n\t".. db:generateStatement(stmtName) .. "\n")
   file:write("\tvoid update" .. name .. "(const struct " .. name .. "& self)\n\t{\n")

   local i = 0;
   for p,q in orderedPairs(tbl) do
      file:write("\t\t" .. db:setStatementArg(stmtName, i, "self." .. p, q) .. "\n")
      i = i + 1
   end


   file:write("\t\t" .. db:setStatementArg(stmtName, i, "self.id", "unsigned64") .. "\n")
   file:write("\t\t" .. db:generateExecute(stmtName) .. "\n")
   file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
   file:write("\t}\n")
end

function Common:generateDeleteFunction(db, file, name, tbl)
   -- print("Generating delete" .. name)
   local stmtName = "delete" .. name .. "Stmt"
   file:write("\n\t".. db:generateStatement(stmtName) .. "\n")
   file:write("\tvoid delete" .. name .. "(unsigned long long id)\n\t{\n")

   file:write("\t\t" .. db:setStatementArg(stmtName, 0, "id", "uint64") .. "\n")

   file:write("\t\t" .. db:generateExecute(stmtName) .. "\n")
   file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
   file:write("\t}\n")
end

function Common:generateGetFunction(db, file, name, tbl)
   -- print("Generating get" .. name)
   local stmtName = "get" .. name .. "Stmt"

   file:write("\n\t".. db:generateStatement(stmtName) .. "\n")
   file:write("\tbool get" .. name .. "(unsigned long long id, " .. name .. "& object)\n\t{\n")


   file:write("\t\t" .. db:setStatementArg(stmtName, 0, "id", "uint64") .. "\n")
   file:write("\t\t" .. db:generateQueryFetchFirst(stmtName, "result") .. "\n")
   file:write("\t\tobject.id = id;")

   for p,q in orderedPairs(tbl) do
      file:write("\t\t" .. db:getStatementResult("result", p, "object." .. p, q, stmtName) .. "\n")
   end

   file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
   file:write("\t\treturn true;\n\t}\n")
end

function Common:generateQueryFunction(db, file, name, tbl)
   -- print("Generating query" .. name)
   local stmtName = "query" .. name .. "Stmt"

   file:write("\n\t".. db:generateStatement(stmtName) .. "\n")
   file:write("\tvoid query" .. name .. "(std::vector<" .. name .. ">& out, ") -- "\n\t{\n")

   for p,q in orderedPairs(tbl) do
      file:write("const std::string& " .. p .. ", ")
   end
   file:seek("cur", -2)
   file:write(")\n\t{\n")
   
   local i = 0;
   for p,q in orderedPairs(tbl) do
      file:write("\t\t" .. db:setStatementArg(stmtName, i, p, "string") .. "\n")
      i = i + 1
   end
   
   file:write("\t\t" .. db:generateQuery(stmtName, "result") .. "\n\n")
   file:write("\t\tstruct " .. name .. " object;\n")
   --file:write("\t\tstd::cout << result->set_row_index(0) << \" \" << result->error() << std::endl;\n");


   file:write("\t\t" .. db:generateRowLoop("result", stmtName) .. "\n\t\t{\n")
   file:write("\t\t\t" .. db:getStatementResult("result", "id", "object.id", "uint64", stmtName) .. "\n")
   
   for p,q in orderedPairs(tbl) do
      file:write("\t\t" .. db:getStatementResult("result", p, "object." .. p, q, stmtName) .. "\n")
   end
   -- file:write("\t\tstd::cout << object.toJson() << \" \" << out.size() << \" \" << out.empty() << std::endl;\n");
   file:write("\t\t\tout.push_back(object);\n\t\t}\n")
   file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
   file:write("\t}\n")
end

function Common:generateSearchFunction(db, file, name, tbl)
   -- print("Generating search" .. name)
   local stmtName = "search" .. name .. "Stmt"

   file:write("\n\t".. db:generateStatement(stmtName) .. "\n")
   file:write("\tvoid search" .. name .. "(std::vector<" .. name .. ">& out, const std::string& term)\n\t{\n")

   file:write([[
		std::string processedTerm = "%" + term + "%";
		std::replace(processedTerm.begin(), processedTerm.end(), ' ', '%');
]])

   local i = 0;
   for p,q in orderedPairs(tbl) do
      file:write("\t\t" .. db:setStatementArg(stmtName, i, "processedTerm", "string") .. "\n")
      i = i + 1
   end

   file:write("\t\t" .. db:generateQuery(stmtName, "result") .. "\n\n")
   file:write("\t\tstruct " .. name .. " object;\n")

   file:write("\t\t" .. db:generateRowLoop("result", stmtName) .. "\n\t\t{\n")
   file:write("\t\t\t" .. db:getStatementResult("result", "id", "object.id", "uint64", stmtName) .. "\n")
   
   for p,q in orderedPairs(tbl) do
      file:write("\t\t\t" .. db:getStatementResult("result", p, "object." .. p, q, stmtName) .. "\n")
   end
   --file:write("\t\tstd::cout << object.toJson() << std::endl;\n");
   file:write("\t\t\tout.push_back(object);\n\t\t}\n")
   file:write("\t\t" .. db:generateStmtReset(stmtName) .. "\n")
   file:write("\t}\n")
end

function Common:generateCreateStmt(db, file, name, tbl)
   -- print("Generating create" .. name .. "Stmt")

   local stmtName =  "create" .. name .. "Stmt"
   file:write("\t\t" .. db:beginStatement(stmtName) .. "\n")
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
   
   file:write("\t\t" .. db:endStatement(stmtName) .. "\n")
end

function Common:generateUpdateStmt(db, file, name, tbl)
   -- print("Generating update" .. name .. "Stmt")  
  -- file:write("\t\tupdate" .. name .. "Stmt = m_connection->create_statement(")
   local stmtName =  "update" .. name .. "Stmt"
   file:write("\t\t" .. db:beginStatement(stmtName) .. "\n")
   file:write("\n\t\t\"update `" .. name .. "` set\"\n")

   local size = 0
   for p,q in orderedPairs(tbl) do
      file:write("\t\t\t\"`" .. p .. "` = ?,\"\n")
   end

   -- Delete the last ',' as it is not needed
   file:seek("cur", -3)
   file:write(" where `id` = ?;\"")
   file:write(db:endStatement(stmtName) .. "\n")
end

function Common:generateDeleteStmt(db, file, name, tbl)
   -- print("Generating delete" .. name .. "Stmt")
   local stmtName =  "delete" .. name .. "Stmt"
   file:write("\t\t" .. db:beginStatement(stmtName) .. "\n")
   file:write("\"delete from `" .. name .. "` where `id` = ?;\"")
   file:write("\t\t" .. db:endStatement(stmtName) .. "\n")
end

function Common:generateGetStmt(db, file, name, tbl)
   -- print("Generating get" .. name .. "Stmt")
   local stmtName = "get" .. name .. "Stmt"
   file:write("\t\t" .. db:beginStatement(stmtName) .. "\n")
   file:write("\"select * from `" .. name .. "` where `id` = ?;\"")
   file:write("\t\t" .. db:endStatement(stmtName) .. "\n")
end

function Common:generateQueryStmt(db, file, name, tbl)
   -- print("Generating query" .. name .. "Stmt")
   local stmtName = "query" .. name .. "Stmt"
   file:write("\t\t" .. db:beginStatement(stmtName) .. "\n")
   file:write("\"select * from `" .. name .. "` where ")

   for p,q in orderedPairs(tbl) do
      file:write("`" .. p .. "` like ? and ")
   end

   file:seek("cur", -5)
   file:write(";\"\t\t" .. db:endStatement(stmtName) .. "\n")
end

function Common:generateSearchStmt(db, file, name, tbl)
   -- print("Generating search" .. name .. "Stmt")
   local stmtName = "search" .. name .. "Stmt"
   file:write("\t\t" .. db:beginStatement(stmtName) .. "\n")
   file:write("\"select * from `" .. name .. "` where ")

   for p,q in orderedPairs(tbl) do
      file:write("`" .. p .. "` like ? or ")
   end

   file:seek("cur", -4)
   file:write(";\"")
   file:write("\t\t" .. db:endStatement(stmtName) .. "\n")
end

return Common
