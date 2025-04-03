%pip install sqlglot

import string
from sqlglot import parse_one, parse, exp
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
from sqlglot.lineage import lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer import optimize

# get the notebook that is using this function <- this will probably have to be a parameter that is passed from the calling notebook to get the correct value in
this_notebook_name = notebookutils.runtime.context['currentNotebookName']
this_workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
this_workspace_name = notebookutils.runtime.context.get("currentWorkspaceName")

def columnLineage(sql, this_notebook_name, this_workspace_id, this_workspace_name): 

    # sql = the sql string to analyze, can be composed of multiple sql statements
    # this_notebook_name = the current notepad
    # this_workspace_id = the id of the workspace
    # this_workspace_name = the workspace name
  
    ## parse the sql
    parsed_qrys = parse(sql_string, read = "spark")

    # iterate through the SQL statements only analysing those that contain a SELECT and a CREATE
    for ast in parsed_qrys:
        if ast.find(exp.Select) != None:
            ast = qualify(ast)
            cleaned_sql = optimize(ast).sql(pretty=True)

            ast = parse_one(cleaned_sql, read = "spark")

            ## Get the create to get the name of the target table
            sink_tbl = ast.find(exp.Create).find(exp.Table).name

            ## Get the name of the target database
            sink_db = ast.find(exp.Create).find(exp.Table).db

            all_cols = {}

            source_db = ""

            for select in ast.find_all(exp.Select):
                for projection in select.expressions:
                    sink_col = projection.alias_or_name
                    lin = lineage(col_name, cleaned_sql, dialect="spark", trim_selects=True)
                    source_tbl = lin.expression.this.this.this
                    source_col = lin.name

                    for tbl in lin.source.find_all(exp.Table):
                        if source_tbl == tbl.name:
                            source_db = tbl.db
                            break

                    all_cols[sink_col] = {"source_db": source_db, "source_tbl": source_tbl, "source_col": source_col, "sink_db": sink_db, "sink_tbl": sink_tbl, "sink_col": sink_col, "notebook_name": this_notebook_name, "workspace_name": this_workspace_name, "workspace_id": this_workspace_id}

    return all_cols

out = columnLineage(sql, this_notebook_name, this_workspace_id, this_workspace_name)
print(out)
