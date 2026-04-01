package service

import pg_query "github.com/pganalyze/pg_query_go/v6"

const executeSQLSelectLimit int64 = 100

func injectSelectLimit(statement string) string {
	tree, err := pg_query.Parse(statement)
	if err != nil || len(tree.Stmts) != 1 || tree.Stmts[0] == nil || tree.Stmts[0].Stmt == nil {
		return statement
	}

	selectStmt := tree.Stmts[0].Stmt.GetSelectStmt()
	if selectStmt == nil || selectStmt.LimitCount != nil {
		return statement
	}

	selectStmt.LimitCount = pg_query.MakeAConstIntNode(executeSQLSelectLimit, -1)
	selectStmt.LimitOption = pg_query.LimitOption_LIMIT_OPTION_COUNT

	rewritten, err := pg_query.Deparse(tree)
	if err != nil {
		return statement
	}

	return rewritten
}
