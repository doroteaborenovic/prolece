import csv
import os
import time
import tracemalloc

class ExecutionError(Exception):
    pass


class SqlExecutor:
    """
    Ovaj executor izvršava AST koji vrati Visitor.

    Primer:
        FROM users      -> učita examples/users.csv
        JOIN orders     -> učita examples/orders.csv

    Očekivani CSV format:
        id:number,name:string
        1,Marko
        2,Ana
    """

    def __init__(self, database_dir="examples"):
        self.database_dir = database_dir
        self.loaded_tables = {}

    # =========================================================
    # GLAVNI POZIV
    # =========================================================
    def benchmark_execute(self, ast):
        """
        Pokreće execute() i vraća:
        - rezultat upita
        - benchmark podatke

        Benchmark meri:
        - execution time
        - peak memory usage
        """
        tracemalloc.start()
        start_time = time.perf_counter()

        result = self.execute(ast)

        end_time = time.perf_counter()
        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        benchmark = {
            "execution_time_seconds": end_time - start_time,
            "execution_time_ms": (end_time - start_time) * 1000,
            "current_memory_bytes": current_memory,
            "peak_memory_bytes": peak_memory,
            "current_memory_kb": current_memory / 1024,
            "peak_memory_kb": peak_memory / 1024,
            "row_count": self._count_rows_in_result(result),
        }

        return result, benchmark

    def print_benchmark(self, benchmark):
        """
        Lep ispis benchmark rezultata.
        """
        print()
        print("=== Benchmark ===")
        print(f"Execution time: {benchmark['execution_time_seconds']:.6f} s")
        print(f"Execution time: {benchmark['execution_time_ms']:.3f} ms")
        print(f"Current memory: {benchmark['current_memory_bytes']} B ({benchmark['current_memory_kb']:.3f} KB)")
        print(f"Peak memory:    {benchmark['peak_memory_bytes']} B ({benchmark['peak_memory_kb']:.3f} KB)")
        print(f"Returned rows:  {benchmark['row_count']}")

    def _count_rows_in_result(self, result):
        """
        Broji koliko redova je vraćeno.

        Podržava:
        - jedan SELECT -> list[dict]
        - više SELECT-ova -> list[list[dict]]
        """
        if result is None:
            return 0

        if not isinstance(result, list):
            return 0

        if len(result) == 0:
            return 0

        # jedan SELECT rezultat
        if all(isinstance(row, dict) for row in result):
            return len(result)

        # više SELECT rezultata
        if all(isinstance(part, list) for part in result):
            total = 0
            for part in result:
                total += len(part)
            return total

        return 0
    def execute(self, ast):
        """
        Pokreće izvršavanje AST-a.

        Tvoj Visitor uglavnom vraća listu SELECT naredbi.
        Ako postoji samo jedan SELECT, vraćamo njegov rezultat.
        Ako ih ima više, vraćamo listu rezultata.
        """
        if isinstance(ast, list):
            all_results = []

            for statement in ast:
                result = self.execute_one_select(statement)
                all_results.append(result)

            if len(all_results) == 1:
                return all_results[0]

            return all_results

        return self.execute_one_select(ast)

    def execute_one_select(self, select_ast):
        """
        Izvršava jednu SELECT naredbu.
        """
        if select_ast.get("type") != "select":
            raise ExecutionError("Executor očekuje SELECT naredbu.")

        # 1. FROM deo pravi početni skup redova
        current_contexts = self.build_contexts_from_from_clause(select_ast["from"])

        # 2. JOIN dodaje nove tabele i filtrira redove po ON uslovu
        current_contexts = self.apply_joins(current_contexts, select_ast["joins"])

        # 3. WHERE filtrira rezultat
        where_part = select_ast.get("where")
        if where_part is not None:
            filtered_contexts = []

            for context in current_contexts:
                if self.evaluate_condition(where_part, context):
                    filtered_contexts.append(context)

            current_contexts = filtered_contexts

        # 4. SELECT bira koje kolone idu u finalni rezultat
        final_rows = []
        for context in current_contexts:
            output_row = self.make_output_row(select_ast["select"], context)
            final_rows.append(output_row)

        return final_rows

    # =========================================================
    # FROM I JOIN
    # =========================================================

    def build_contexts_from_from_clause(self, from_items):
        """
        Pravi početne kontekste iz FROM dela.

        Svaki context je dict oblika:
            {
                "users": {"id": 1, "name": "Marko"},
                "orders": {"id": 10, "user_id": 1}
            }

        Ako ima više tabela u FROM, pravi se kartenzijev proizvod.
        """
        if not from_items:
            raise ExecutionError("Nedostaje FROM deo.")

        contexts = [{}]

        for from_item in from_items:
            table_name = from_item["table"]
            visible_name = self.get_visible_table_name(from_item)
            table_rows = self.load_table(table_name)

            new_contexts = []

            for old_context in contexts:
                for row in table_rows:
                    new_context = dict(old_context)
                    new_context[visible_name] = row
                    new_contexts.append(new_context)

            contexts = new_contexts

        return contexts

    def apply_joins(self, contexts, join_items):
        """
        Dodaje JOIN tabele i zadržava samo kombinacije koje zadovoljavaju ON uslov.
        """
        for join_item in join_items:
            table_name = join_item["table"]
            table_rows = self.load_table(table_name)
            join_condition = join_item["on"]

            new_contexts = []

            for old_context in contexts:
                for row in table_rows:
                    candidate_context = dict(old_context)
                    candidate_context[table_name] = row

                    if self.evaluate_condition(join_condition, candidate_context):
                        new_contexts.append(candidate_context)

            contexts = new_contexts

        return contexts

    def get_visible_table_name(self, from_item):
        """
        Ako postoji alias, koristi alias.
        Inače koristi pravo ime tabele.
        """
        aliases = from_item.get("aliases", [])
        if aliases:
            return aliases[0]

        return from_item["table"]

    # =========================================================
    # SELECT DEO
    # =========================================================

    def make_output_row(self, select_items, context):
        """
        Pravi jedan izlazni red na osnovu SELECT dela.
        """
        output = {}

        for item in select_items:
            item_type = item["type"]

            if item_type == "column":
                table_name = item["table"]
                column_name = item["column"]

                row = self.get_row_for_table(context, table_name)
                output[f"{table_name}.{column_name}"] = row.get(column_name)

            elif item_type == "all_columns":
                table_name = item.get("table")

                # SELECT *
                if table_name is None:
                    for current_table_name, row in context.items():
                        for column_name, value in row.items():
                            output[f"{current_table_name}.{column_name}"] = value

                # SELECT users.*
                else:
                    row = self.get_row_for_table(context, table_name)
                    for column_name, value in row.items():
                        output[f"{table_name}.{column_name}"] = value

            elif item_type == "table":
                # grammar ti dozvoljava i SELECT users
                # tretiramo ga kao SELECT users.*
                table_name = item["table"]
                row = self.get_row_for_table(context, table_name)

                for column_name, value in row.items():
                    output[f"{table_name}.{column_name}"] = value

            else:
                raise ExecutionError(f"Nepodržan SELECT item type: {item_type}")

        return output

    # =========================================================
    # WHERE / ON USLOVI
    # =========================================================

    def evaluate_condition(self, condition_ast, context):
        """
        Vraća True ili False za WHERE ili JOIN ON uslov.
        """
        condition_type = condition_ast["type"]

        if condition_type == "and":
            left_ok = self.evaluate_condition(condition_ast["left"], context)
            right_ok = self.evaluate_condition(condition_ast["right"], context)
            return left_ok and right_ok

        if condition_type == "or":
            left_ok = self.evaluate_condition(condition_ast["left"], context)
            right_ok = self.evaluate_condition(condition_ast["right"], context)
            return left_ok or right_ok

        if condition_type == "not":
            return not self.evaluate_condition(condition_ast["value"], context)

        if condition_type == "group":
            return self.evaluate_condition(condition_ast["value"], context)

        if condition_type == "comparison":
            left_value = self.evaluate_term(condition_ast["left"], context)
            right_value = self.evaluate_term(condition_ast["right"], context)
            operator = condition_ast["operator"]
            return self.compare_values(left_value, right_value, operator)

        if condition_type == "join_condition":
            left_value = self.evaluate_term(condition_ast["left"], context)
            right_value = self.evaluate_term(condition_ast["right"], context)
            operator = condition_ast["operator"]
            return self.compare_values(left_value, right_value, operator)

        if condition_type == "is_null":
            value = self.evaluate_term(condition_ast["value"], context)
            return value is None

        raise ExecutionError(f"Nepodržan condition type: {condition_type}")

    def evaluate_term(self, term_ast, context):
        """
        Pretvara AST term u konkretnu Python vrednost.
        """
        term_type = term_ast["type"]

        if term_type == "int":
            return term_ast["value"]

        if term_type == "string":
            return term_ast["value"]

        if term_type == "column_ref":
            table_name = term_ast.get("table")
            column_name = term_ast["column"]
            return self.resolve_column_value(context, table_name, column_name)

        raise ExecutionError(f"Nepodržan term type: {term_type}")

    def compare_values(self, left, right, operator):
        """
        Poredi dve vrednosti.
        """
        if operator == "=":
            return left == right
        if operator == "!=":
            return left != right
        if operator == "<":
            return left < right
        if operator == ">":
            return left > right
        if operator == "<=":
            return left <= right
        if operator == ">=":
            return left >= right

        raise ExecutionError(f"Nepodržan operator: {operator}")

    def resolve_column_value(self, context, table_name, column_name):
        """
        Nalazi vrednost kolone.

        Ako je zadato ime tabele:
            users.id
        onda uzima baš iz te tabele.

        Ako nije:
            id
        pokušava da je nađe među svim tabelama.
        """
        if table_name is not None:
            row = self.get_row_for_table(context, table_name)

            if column_name not in row:
                raise ExecutionError(
                    f"Kolona '{column_name}' ne postoji u tabeli '{table_name}'."
                )

            return row[column_name]

        found_values = []

        for current_table_name, row in context.items():
            if column_name in row:
                found_values.append((current_table_name, row[column_name]))

        if not found_values:
            raise ExecutionError(f"Kolona '{column_name}' nije pronađena.")

        if len(found_values) > 1:
            table_names = ", ".join(table_name for table_name, _ in found_values)
            raise ExecutionError(
                f"Kolona '{column_name}' je dvosmislena. Nađena je u tabelama: {table_names}"
            )

        return found_values[0][1]

    def get_row_for_table(self, context, table_name):
        """
        Vraća red za traženu tabelu iz trenutnog context-a.
        """
        if table_name not in context:
            raise ExecutionError(
                f"Tabela '{table_name}' nije dostupna u trenutnom context-u."
            )

        return context[table_name]

    # =========================================================
    # UČITAVANJE CSV TABELE
    # =========================================================

    def load_table(self, table_name):
        """
        Učitava tabelu iz CSV fajla.
        Koristi cache da se isti fajl ne čita više puta.
        """
        if table_name in self.loaded_tables:
            return self.loaded_tables[table_name]

        file_path = os.path.join(self.database_dir, f"{table_name}.csv")

        if not os.path.exists(file_path):
            raise ExecutionError(
                f"Tabela '{table_name}' nije pronađena. Očekivan fajl: {file_path}"
            )

        rows = self.read_csv_file(file_path)
        self.loaded_tables[table_name] = rows
        return rows

    def read_csv_file(self, file_path):
        """
        Čita CSV fajl i vraća listu redova.

        Prvi red mora biti:
            column:type,column:type,...

        Primer:
            id:number,name:string
            1,Marko
            2,Ana
        """
        with open(file_path, "r", encoding="utf-8", newline="") as file:
            reader = csv.reader(file)
            all_rows = list(reader)

        if not all_rows:
            return []

        header = all_rows[0]
        columns = self.parse_csv_header(header, file_path)

        parsed_rows = []

        for line_number, raw_row in enumerate(all_rows[1:], start=2):
            if len(raw_row) != len(columns):
                raise ExecutionError(
                    f"Red {line_number} u fajlu {file_path} nema dobar broj kolona."
                )

            parsed_row = {}

            for (column_name, column_type), raw_value in zip(columns, raw_row):
                parsed_row[column_name] = self.parse_cell_value(raw_value, column_type)

            parsed_rows.append(parsed_row)

        return parsed_rows

    def parse_csv_header(self, header, file_path):
        """
        Pretvara header iz CSV-a u listu:
            [("id", "number"), ("name", "string")]
        """
        if not header:
            raise ExecutionError(f"CSV fajl {file_path} nema validan header.")

        columns = []

        for cell in header:
            if ":" not in cell:
                raise ExecutionError(
                    f"Kolona '{cell}' u fajlu {file_path} nije u formatu name:type"
                )

            column_name, column_type = cell.split(":", 1)
            column_name = column_name.strip()
            column_type = column_type.strip()

            if column_type not in ("number", "string"):
                raise ExecutionError(
                    f"Tip '{column_type}' nije podržan u fajlu {file_path}."
                )

            columns.append((column_name, column_type))

        return columns

    def parse_cell_value(self, raw_value, column_type):
        """
        Pretvara CSV string u Python vrednost.
        """
        value = raw_value.strip()

        # prazno polje -> NULL
        if value == "":
            return None

        if column_type == "string":
            return value

        if column_type == "number":
            if "." in value:
                return float(value)
            return int(value)

        raise ExecutionError(f"Nepodržan tip kolone: {column_type}")

    # =========================================================
    # PRETTY PRINT
    # =========================================================

    def pretty_print(self, result):
        """
        Lep ispis rezultata.

        Mogući oblici:
        - jedan SELECT -> list[dict]
        - više SELECT-ova -> list[list[dict]]
        """
        if result is None:
            print("(no result)")
            return

        if not isinstance(result, list):
            print(result)
            return

        if len(result) == 0:
            print("(empty result)")
            return

        # jedan SELECT
        if all(isinstance(row, dict) for row in result):
            self.print_table(result)
            return

        # više SELECT rezultata
        if all(isinstance(part, list) for part in result):
            for index, one_result in enumerate(result, start=1):
                print(f"--- Result set {index} ---")
                self.print_table(one_result)
                print()
            return

        print(result)

    def print_table(self, rows):
        """
        Štampa jednu tabelu.
        """
        if not rows:
            print("(0 rows)")
            return

        headers = self.collect_headers(rows)

        string_rows = []
        for row in rows:
            current_row = []
            for header in headers:
                value = row.get(header)
                current_row.append(self.format_cell(value))
            string_rows.append(current_row)

        column_widths = []
        for header in headers:
            column_widths.append(len(header))

        for row in string_rows:
            for index, cell in enumerate(row):
                column_widths[index] = max(column_widths[index], len(cell))

        separator = "+" + "+".join("-" * (width + 2) for width in column_widths) + "+"

        header_line = "|"
        for index, header in enumerate(headers):
            header_line += " " + header.ljust(column_widths[index]) + " |"

        print(separator)
        print(header_line)
        print(separator)

        for row in string_rows:
            line = "|"
            for index, cell in enumerate(row):
                line += " " + cell.ljust(column_widths[index]) + " |"
            print(line)

        print(separator)
        print(f"({len(rows)} rows)")

    def collect_headers(self, rows):
        """
        Skuplja sva imena kolona redom kako se pojavljuju.
        """
        headers = []
        seen = set()

        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    headers.append(key)

        return headers

    def format_cell(self, value):
        """
        Pretvara vrednost u string za ispis.
        """
        if value is None:
            return "NULL"

        return str(value)