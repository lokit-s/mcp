import os
import pyodbc
import psycopg2
from typing import Any

# MCP server
from fastmcp import FastMCP
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ————————————————
# 1. MySQL Configuration
# ————————————————
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")


def get_mysql_conn(db: str | None = MYSQL_DB):
    """If db is None we connect to the server only (needed to CREATE DATABASE)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        ssl_disabled=False,  # Aiven requires TLS; keep this False
        autocommit=True,
    )


# ————————————————
# 2. PostgreSQL Configuration (Products)
# ————————————————
def must_get(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var {key}")
    return val


PG_HOST = must_get("PG_HOST")
PG_PORT = int(must_get("PG_PORT"))
PG_DB = os.getenv("PG_DB", "postgres")  # db name can default
PG_USER = must_get("PG_USER")
PG_PASS = must_get("PG_PASSWORD")


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",  # Supabase enforces TLS
    )


# ————————————————
# 3. PostgreSQL Configuration (Sales)
# ————————————————
PG_SALES_HOST = must_get("PG_SALES_HOST")
PG_SALES_PORT = int(must_get("PG_SALES_PORT"))
PG_SALES_DB = os.getenv("PG_SALES_DB", "sales_db")
PG_SALES_USER = must_get("PG_SALES_USER")
PG_SALES_PASS = must_get("PG_SALES_PASSWORD")


def get_pg_sales_conn():
    return psycopg2.connect(
        host=PG_SALES_HOST,
        port=PG_SALES_PORT,
        dbname=PG_SALES_DB,
        user=PG_SALES_USER,
        password=PG_SALES_PASS,
        sslmode="require",
    )


# ————————————————
# 4. Instantiate your MCP server
# ————————————————
mcp = FastMCP("CRUDServer")


# ————————————————
# 5. Synchronous Setup: Create & seed tables
# ————————————————
def seed_databases():
    # ---------- MySQL (Customers) ----------
    root_cnx = get_mysql_conn(db=None)
    root_cur = root_cnx.cursor()
    root_cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}`;")
    root_cur.close()
    root_cnx.close()

    sql_cnx = get_mysql_conn()
    sql_cur = sql_cnx.cursor()
    sql_cur.execute("DROP TABLE IF EXISTS Customers;")
    sql_cur.execute("""
                    CREATE TABLE Customers
                    (
                        Id        INT AUTO_INCREMENT PRIMARY KEY,
                        Name      VARCHAR(100) NOT NULL,
                        Email     VARCHAR(100) NOT NULL,
                        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """)
    sql_cur.executemany(
        "INSERT INTO Customers (Name, Email) VALUES (%s, %s)",
        [("Alice", "alice@example.com"),
         ("Bob", "bob@example.com"),
         ("Charlie", "charlie@example.com")]
    )
    sql_cnx.close()

    # ---------- PostgreSQL (Products) ----------
    pg_cnxn = get_pg_conn()
    pg_cnxn.autocommit = True
    pg_cur = pg_cnxn.cursor()

    pg_cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    pg_cur.execute("""
                   CREATE TABLE products
                   (
                       id          SERIAL PRIMARY KEY,
                       name        TEXT           NOT NULL,
                       price       NUMERIC(10, 4) NOT NULL,
                       description TEXT
                   );
                   """)

    pg_cur.executemany(
        "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)",
        [("Widget", 9.99, "A standard widget."),
         ("Gadget", 14.99, "A useful gadget."),
         ("Tool", 24.99, "A handy tool.")]
    )
    pg_cnxn.close()

    # ---------- PostgreSQL Sales Database ----------
    sales_cnxn = get_pg_sales_conn()
    sales_cnxn.autocommit = True
    sales_cur = sales_cnxn.cursor()

    sales_cur.execute("DROP TABLE IF EXISTS sales;")
    sales_cur.execute("""
                      CREATE TABLE sales
                      (
                          id           SERIAL PRIMARY KEY,
                          customer_id  INT            NOT NULL,
                          product_id   INT            NOT NULL,
                          quantity     INT            NOT NULL DEFAULT 1,
                          unit_price   NUMERIC(10, 4) NOT NULL,
                          total_amount NUMERIC(10, 4) NOT NULL,
                          sale_date    TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
                      );
                      """)

    # Sample sales data
    sales_cur.executemany(
        "INSERT INTO sales (customer_id, product_id, quantity, unit_price, total_amount) VALUES (%s, %s, %s, %s, %s)",
        [(1, 1, 2, 9.99, 19.98),  # Alice bought 2 Widgets
         (2, 2, 1, 14.99, 14.99),  # Bob bought 1 Gadget
         (3, 3, 3, 24.99, 74.97)]  # Charlie bought 3 Tools
    )
    sales_cnxn.close()


# ————————————————
# 6. Helper Functions for Cross-Database Queries
# ————————————————
def get_customer_name(customer_id: int) -> str:
    """Fetch customer name from MySQL database"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        mysql_cur.execute("SELECT Name FROM Customers WHERE Id = %s", (customer_id,))
        result = mysql_cur.fetchone()
        mysql_cnxn.close()
        return result[0] if result else f"Unknown Customer ({customer_id})"
    except Exception:
        return f"Unknown Customer ({customer_id})"


def get_product_details(product_id: int) -> dict:
    """Fetch product name and price from PostgreSQL products database"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        pg_cur.execute("SELECT name, price FROM products WHERE id = %s", (product_id,))
        result = pg_cur.fetchone()
        pg_cnxn.close()
        if result:
            return {"name": result[0], "price": float(result[1])}
        else:
            return {"name": f"Unknown Product ({product_id})", "price": 0.0}
    except Exception:
        return {"name": f"Unknown Product ({product_id})", "price": 0.0}


def validate_customer_exists(customer_id: int) -> bool:
    """Check if customer exists in MySQL database"""
    try:
        mysql_cnxn = get_mysql_conn()
        mysql_cur = mysql_cnxn.cursor()
        mysql_cur.execute("SELECT COUNT(*) FROM Customers WHERE Id = %s", (customer_id,))
        result = mysql_cur.fetchone()
        mysql_cnxn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def validate_product_exists(product_id: int) -> bool:
    """Check if product exists in PostgreSQL products database"""
    try:
        pg_cnxn = get_pg_conn()
        pg_cur = pg_cnxn.cursor()
        pg_cur.execute("SELECT COUNT(*) FROM products WHERE id = %s", (product_id,))
        result = pg_cur.fetchone()
        pg_cnxn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


# ————————————————
# 7. MySQL CRUD Tool (Customers)
# ————————————————
@mcp.tool()
async def sqlserver_crud(
        operation: str,
        name: str = None,
        email: str = None,
        limit: int = 10,
        customer_id: int = None,
        new_email: str = None,
        table_name: str = None,
) -> Any:
    cnxn = get_mysql_conn()
    cur = cnxn.cursor()

    if operation == "create":
        if not name or not email:
            return {"sql": None, "result": "❌ 'name' and 'email' required for create."}

        sql_query = "INSERT INTO Customers (Name, Email) VALUES (%s, %s)"
        cur.execute(sql_query, (name, email))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer '{name}' added."}

    elif operation == "read":
        sql_query = """
                    SELECT Id, Name, Email, CreatedAt
                    FROM Customers
                    ORDER BY Id ASC \
                    """
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {"Id": r[0], "Name": r[1], "Email": r[2], "CreatedAt": r[3].isoformat()}
            for r in rows
        ]
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not customer_id or not new_email:
            return {"sql": None, "result": "❌ 'customer_id' and 'new_email' required for update."}

        sql_query = "UPDATE Customers SET Email = %s WHERE Id = %s"
        cur.execute(sql_query, (new_email, customer_id))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer id={customer_id} updated."}

    elif operation == "delete":
        if not customer_id:
            return {"sql": None, "result": "❌ 'customer_id' required for delete."}

        sql_query = "DELETE FROM Customers WHERE Id = %s"
        cur.execute(sql_query, (customer_id,))
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Customer id={customer_id} deleted."}

    elif operation == "describe":
        if not table_name:
            return {"sql": None, "result": "❌ 'table_name' required for describe."}

        sql_query = """
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s \
                      AND TABLE_NAME = %s \
                    """
        cur.execute(sql_query, (MYSQL_DB, table_name))
        rows = cur.fetchall()
        result = [
            {"column": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]}
            for r in rows
        ]
        return {"sql": sql_query, "result": result}

    else:
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 8. PostgreSQL CRUD Tool (Products)
# ————————————————
@mcp.tool()
async def postgresql_crud(
        operation: str,
        name: str = None,
        price: float = None,
        description: str = None,
        limit: int = 10,
        product_id: int = None,
        new_price: float = None,
        table_name: str = None,
) -> Any:
    cnxn = get_pg_conn()
    cur = cnxn.cursor()

    if operation == "create":
        if not name or price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'name' and 'price' required for create."}
        sql_query = "INSERT INTO products (name, price, description) VALUES (%s, %s, %s)"
        cur.execute(sql_query, (name, price, description))
        cnxn.commit()
        result = f"✅ Product '{name}' added."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "read":
        sql_query = (
            "SELECT id, name, price, description "
            "FROM products "
            "ORDER BY id ASC"
        )
        cur.execute(sql_query)
        rows = cur.fetchall()
        result = [
            {"id": r[0], "name": r[1], "price": float(r[2]), "description": r[3] or ""}
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not product_id or new_price is None:
            cnxn.close()
            return {"sql": None, "result": "❌ 'product_id' and 'new_price' required for update."}
        sql_query = "UPDATE products SET price = %s WHERE id = %s"
        cur.execute(sql_query, (new_price, product_id))
        cnxn.commit()
        result = f"✅ Product id={product_id} updated."
        cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "delete":
        if not product_id and not name:
            return {"sql": None, "result": "❌ Provide 'product_id' **or** 'name' for delete."}

        if product_id:
            sql_query = "DELETE FROM products WHERE id = %s"
            params = (product_id,)
        else:
            sql_query = "DELETE FROM products WHERE name = %s"
            params = (name,)

        cur.execute(sql_query, params)
        cnxn.commit()
        return {"sql": sql_query, "result": f"✅ Deleted product."}

    elif operation == "describe":
        if not table_name:
            cnxn.close()
            return {"sql": None, "result": "❌ 'table_name' required for describe."}
        sql_query = """
                    SELECT column_name, data_type, is_nullable, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = %s \
                    """
        cur.execute(sql_query, (table_name,))
        rows = cur.fetchall()
        result = [
            {"column": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]}
            for r in rows
        ]
        cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 9. Sales CRUD Tool (Separate PostgreSQL Database)
# ————————————————
@mcp.tool()
async def sales_crud(
        operation: str,
        customer_id: int = None,
        product_id: int = None,
        quantity: int = 1,
        unit_price: float = None,
        total_amount: float = None,
        sale_id: int = None,
        new_quantity: int = None,
        table_name: str = None,
) -> Any:
    sales_cnxn = get_pg_sales_conn()
    sales_cur = sales_cnxn.cursor()

    if operation == "create":
        if not customer_id or not product_id:
            sales_cnxn.close()
            return {"sql": None, "result": "❌ 'customer_id' and 'product_id' required for create."}

        # Validate customer exists
        if not validate_customer_exists(customer_id):
            sales_cnxn.close()
            return {"sql": None, "result": f"❌ Customer with ID {customer_id} not found."}

        # Validate product exists and get price
        if not validate_product_exists(product_id):
            sales_cnxn.close()
            return {"sql": None, "result": f"❌ Product with ID {product_id} not found."}

        # Get product price if not provided
        if not unit_price:
            product_details = get_product_details(product_id)
            unit_price = product_details["price"]

        if not total_amount:
            total_amount = unit_price * quantity

        sql_query = """
                    INSERT INTO sales (customer_id, product_id, quantity, unit_price, total_amount)
                    VALUES (%s, %s, %s, %s, %s) \
                    """
        sales_cur.execute(sql_query, (customer_id, product_id, quantity, unit_price, total_amount))
        sales_cnxn.commit()

        # Get customer and product names for response
        customer_name = get_customer_name(customer_id)
        product_details = get_product_details(product_id)

        result = f"✅ Sale created: {customer_name} bought {quantity} {product_details['name']}(s) for ${total_amount:.2f}"
        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "read":
        sql_query = """
                    SELECT id, customer_id, product_id, quantity, unit_price, total_amount, sale_date
                    FROM sales
                    ORDER BY id ASC \
                    """
        sales_cur.execute(sql_query)
        rows = sales_cur.fetchall()

        # Enrich data with customer names and product details
        result = []
        for r in rows:
            customer_name = get_customer_name(r[1])
            product_details = get_product_details(r[2])

            result.append({
                "id": r[0],
                "customer_id": r[1],
                "customer_name": customer_name,
                "product_id": r[2],
                "product_name": product_details["name"],
                "quantity": r[3],
                "unit_price": float(r[4]),
                "total_amount": float(r[5]),
                "sale_date": r[6].isoformat()
            })

        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "update":
        if not sale_id or new_quantity is None:
            sales_cnxn.close()
            return {"sql": None, "result": "❌ 'sale_id' and 'new_quantity' required for update."}

        # Recalculate total amount
        sql_query = """
                    UPDATE sales
                    SET quantity     = %s, \
                        total_amount = unit_price * %s
                    WHERE id = %s \
                    """
        sales_cur.execute(sql_query, (new_quantity, new_quantity, sale_id))
        sales_cnxn.commit()
        result = f"✅ Sale id={sale_id} updated to quantity {new_quantity}."
        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "delete":
        if not sale_id:
            sales_cnxn.close()
            return {"sql": None, "result": "❌ 'sale_id' required for delete."}

        sql_query = "DELETE FROM sales WHERE id = %s"
        sales_cur.execute(sql_query, (sale_id,))
        sales_cnxn.commit()
        result = f"✅ Sale id={sale_id} deleted."
        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    elif operation == "describe":
        if not table_name:
            sales_cnxn.close()
            return {"sql": None, "result": "❌ 'table_name' required for describe."}

        sql_query = """
                    SELECT column_name, data_type, is_nullable, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = %s \
                    """
        sales_cur.execute(sql_query, (table_name,))
        rows = sales_cur.fetchall()
        result = [
            {"column": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]}
            for r in rows
        ]
        sales_cnxn.close()
        return {"sql": sql_query, "result": result}

    else:
        sales_cnxn.close()
        return {"sql": None, "result": f"❌ Unknown operation '{operation}'."}


# ————————————————
# 10. Main: seed + run server
# ————————————————
if __name__ == "__main__":
    # 1) Create + seed all databases (if needed)
    seed_databases()
    
    # 2) Launch the MCP server for cloud deployment
    import os
    port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)

