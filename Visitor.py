from generated.ExprVisitor import ExprVisitor
from generated.ExprParser import ExprParser


class Visitor(ExprVisitor):
    def visitProgram(self, ctx: ExprParser.ProgramContext):
        return self.visit(ctx.expr())

    def visitExpr(self, ctx: ExprParser.ExprContext):
        return [self.visit(select_ctx) for select_ctx in ctx.selectExpr()]

    def visitSelectExpr(self, ctx: ExprParser.SelectExprContext):
        if ctx.OP() is not None:
            select_items = [{"type": "all_columns", "table": None}]
        else:
            select_items = self.visit(ctx.selectList())

        joins = [self.visit(join_ctx) for join_ctx in ctx.joinExpr()]
        where_parts = [self.visit(where_ctx) for where_ctx in ctx.whereExpr()]

        where_clause = None
        if len(where_parts) == 1:
            where_clause = where_parts[0]
        elif len(where_parts) > 1:
            where_clause = where_parts[0]
            for cond in where_parts[1:]:
                where_clause = {
                    "type": "and",
                    "left": where_clause,
                    "right": cond,
                }

        return {
            "type": "select",
            "select": select_items,
            "from": self.visit(ctx.fromList()),
            "joins": joins,
            "where": where_clause,
        }

    def visitSelectList(self, ctx: ExprParser.SelectListContext):
        return [self.visit(item) for item in ctx.selectItem()]

    def visitSelectItem(self, ctx: ExprParser.SelectItemContext):
        table = self.visit(ctx.table_name())

        if ctx.DOT() is None:
            return {
                "type": "table",
                "table": table,
            }

        if ctx.OP() is not None:
            return {
                "type": "all_columns",
                "table": table,
            }

        return {
            "type": "column",
            "table": table,
            "column": self.visit(ctx.column()),
        }

    def visitWhereExpr(self, ctx: ExprParser.WhereExprContext):
        return self.visit(ctx.condition())

    def visitOrCond(self, ctx: ExprParser.OrCondContext):
        left, right = ctx.condition()
        return {
            "type": "or",
            "left": self.visit(left),
            "right": self.visit(right),
        }

    def visitAndCond(self, ctx: ExprParser.AndCondContext):
        left, right = ctx.condition()
        return {
            "type": "and",
            "left": self.visit(left),
            "right": self.visit(right),
        }

    def visitParenCond(self, ctx: ExprParser.ParenCondContext):
        return {
            "type": "group",
            "value": self.visit(ctx.condition()),
        }

    def visitEqualityCond(self, ctx: ExprParser.EqualityCondContext):
        return self.visit(ctx.tacnost())

    def visitNotCond(self, ctx: ExprParser.NotCondContext):
        return {
            "type": "not",
            "value": self.visit(ctx.condition()),
        }

    def visitUporedjivanje(self, ctx: ExprParser.UporedjivanjeContext):
        left, right = ctx.term()
        return {
            "type": "comparison",
            "operator": self.visit(ctx.uporedi()),
            "left": self.visit(left),
            "right": self.visit(right),
        }

    def visitIsNullProvera(self, ctx: ExprParser.IsNullProveraContext):
        return {
            "type": "is_null",
            "value": self.visit(ctx.term()),
        }

    def visitIntVal(self, ctx: ExprParser.IntValContext):
        return {
            "type": "int",
            "value": int(ctx.INT().getText()),
        }

    def visitStringVal(self, ctx: ExprParser.StringValContext):
        raw = ctx.STRING().getText()
        return {
            "type": "string",
            "value": raw[1:-1],
        }

    def visitFullColumnVal(self, ctx: ExprParser.FullColumnValContext):
        return {
            "type": "column_ref",
            "table": self.visit(ctx.table_name()),
            "column": self.visit(ctx.column()),
        }

    def visitSimpleColumnVal(self, ctx: ExprParser.SimpleColumnValContext):
        return {
            "type": "column_ref",
            "table": None,
            "column": self.visit(ctx.column()),
        }

    def visitTable_name(self, ctx: ExprParser.Table_nameContext):
        return ctx.getText()

    def visitColumn(self, ctx: ExprParser.ColumnContext):
        return ctx.getText()

    def visitUporedi(self, ctx: ExprParser.UporediContext):
        return ctx.getText()

    def visitFromList(self, ctx: ExprParser.FromListContext):
        return [self.visit(item) for item in ctx.fromExpr()]

    def visitFromExpr(self, ctx: ExprParser.FromExprContext):
        aliases = [token.getText() for token in ctx.ID()]

        result = {
            "type": "from",
            "table": self.visit(ctx.table_name()),
        }

        if aliases:
            result["aliases"] = aliases

        return result

    def visitJoinExpr(self, ctx: ExprParser.JoinExprContext):
        return {
            "type": "join",
            "table": self.visit(ctx.table_name()),
            "on": self.visit(ctx.stat()),
        }

    def visitStat(self, ctx: ExprParser.StatContext):
        left_table, right_table = ctx.table_name()
        left_column, right_column = ctx.column()

        return {
            "type": "join_condition",
            "operator": "=",
            "left": {
                "type": "column_ref",
                "table": self.visit(left_table),
                "column": self.visit(left_column),
            },
            "right": {
                "type": "column_ref",
                "table": self.visit(right_table),
                "column": self.visit(right_column),
            },
        }