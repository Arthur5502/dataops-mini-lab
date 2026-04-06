from pathlib import Path
import duckdb
import pandas as pd

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
DB_PATH = Path(__file__).parent.parent / "warehouse" / "ecommerce.duckdb"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def extract():
    print("\n[1/3] EXTRAÇÃO")
    customers  = pd.read_csv(RAW_DIR / "customers.csv")
    products   = pd.read_csv(RAW_DIR / "products.csv")
    orders     = pd.read_csv(RAW_DIR / "orders.csv")
    order_items = pd.read_csv(RAW_DIR / "order_items.csv")

    print(f"  customers  : {len(customers):>6} linhas")
    print(f"  products   : {len(products):>6} linhas")
    print(f"  orders     : {len(orders):>6} linhas")
    print(f"  order_items: {len(order_items):>6} linhas")

    return customers, products, orders, order_items

def transform(customers, products, orders, order_items):
    print("\n[2/3] TRANSFORMAÇÃO")

    customers["signup_date"] = pd.to_datetime(customers["signup_date"])
    customers["customer_id"] = customers["customer_id"].astype(int)

    products["product_id"] = products["product_id"].astype(int)
    products["price"] = products["price"].astype(float)

    orders["order_id"]    = orders["order_id"].astype(int)
    orders["customer_id"] = orders["customer_id"].astype(int)
    orders["order_date"]  = pd.to_datetime(orders["order_date"])

    order_items["order_item_id"] = order_items["order_item_id"].astype(int)
    order_items["order_id"]      = order_items["order_id"].astype(int)
    order_items["product_id"]    = order_items["product_id"].astype(int)
    order_items["quantity"]      = order_items["quantity"].astype(int)
    order_items["unit_price"]    = order_items["unit_price"].astype(float)

    before = {
        "customers": len(customers),
        "products":  len(products),
        "orders":    len(orders),
        "order_items": len(order_items),
    }
    customers  = customers.dropna(subset=["customer_id", "customer_name", "state"])
    products   = products.dropna(subset=["product_id", "price"])
    orders     = orders.dropna(subset=["order_id", "customer_id", "order_date"])
    order_items = order_items.dropna(subset=["order_item_id", "order_id", "product_id"])

    for name, df, b in zip(
        ["customers", "products", "orders", "order_items"],
        [customers, products, orders, order_items],
        before.values(),
    ):
        dropped = b - len(df)
        if dropped:
            print(f"  {name}: {dropped} linhas removidas por nulos")

    order_items["total_price"] = (order_items["quantity"] * order_items["unit_price"]).round(2)

    items_with_product = order_items.merge(
        products[["product_id", "product_name", "category"]],
        on="product_id",
        how="left",
    )

    items_with_order = items_with_product.merge(
        orders[["order_id", "customer_id", "order_date", "status"]],
        on="order_id",
        how="left",
    )

    consolidated = items_with_order.merge(
        customers[["customer_id", "customer_name", "city", "state"]],
        on="customer_id",
        how="left",
    )

    consolidated["order_year"]  = consolidated["order_date"].dt.year
    consolidated["order_month"] = consolidated["order_date"].dt.month
    consolidated["order_ym"]    = consolidated["order_date"].dt.to_period("M").astype(str)

    print(f"  Base consolidada: {len(consolidated):,} linhas x {len(consolidated.columns)} colunas")

    return customers, products, orders, order_items, consolidated

def load(customers, products, orders, order_items, consolidated):
    print(f"\n[3/3] CARGA → {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))
    for name, df in [
        ("raw_customers",   customers),
        ("raw_products",    products),
        ("raw_orders",      orders),
        ("raw_order_items", order_items),
    ]:
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM df")
        print(f"  raw.{name}: {len(df):,} linhas")

    con.execute("DROP TABLE IF EXISTS treated_customers")
    con.execute("""
        CREATE TABLE treated_customers AS
        SELECT
            customer_id,
            customer_name,
            email,
            city,
            state,
            signup_date::DATE AS signup_date
        FROM raw_customers
    """)

    con.execute("DROP TABLE IF EXISTS treated_products")
    con.execute("""
        CREATE TABLE treated_products AS
        SELECT
            product_id,
            product_name,
            category,
            ROUND(price, 2) AS price
        FROM raw_products
    """)

    con.execute("DROP TABLE IF EXISTS treated_orders")
    con.execute("""
        CREATE TABLE treated_orders AS
        SELECT
            order_id,
            customer_id,
            order_date::DATE AS order_date,
            status
        FROM raw_orders
    """)

    con.execute("DROP TABLE IF EXISTS treated_order_items")
    con.execute("""
        CREATE TABLE treated_order_items AS
        SELECT
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            ROUND(quantity * unit_price, 2) AS total_price
        FROM raw_order_items
    """)
    print("  Tabelas tratadas criadas.")

    con.execute("DROP TABLE IF EXISTS fct_order_items")
    con.execute(f"CREATE TABLE fct_order_items AS SELECT * FROM consolidated")
    print(f"  fct_order_items: {len(consolidated):,} linhas (tabela analítica final)")

    con.close()

def run_analytics():
    print("\n" + "=" * 60)
    print("CONSULTAS ANALÍTICAS")
    print("=" * 60)

    con = duckdb.connect(str(DB_PATH))

    print("\n>> Faturamento total por mês (top 12 mais recentes):")
    result = con.execute("""
        SELECT
            order_ym            AS mes,
            ROUND(SUM(total_price), 2) AS faturamento
        FROM fct_order_items
        WHERE status != 'cancelled'
        GROUP BY order_ym
        ORDER BY order_ym DESC
        LIMIT 12
    """).fetchdf()
    print(result.to_string(index=False))

    print("\n>> Faturamento por categoria:")
    result = con.execute("""
        SELECT
            category,
            ROUND(SUM(total_price), 2)  AS faturamento,
            COUNT(DISTINCT order_id)    AS qtd_pedidos
        FROM fct_order_items
        WHERE status != 'cancelled'
        GROUP BY category
        ORDER BY faturamento DESC
    """).fetchdf()
    print(result.to_string(index=False))

    print("\n>> Quantidade de pedidos por estado:")
    result = con.execute("""
        SELECT
            state,
            COUNT(DISTINCT order_id) AS qtd_pedidos
        FROM fct_order_items
        GROUP BY state
        ORDER BY qtd_pedidos DESC
    """).fetchdf()
    print(result.to_string(index=False))

    print("\n>> Ticket médio por cliente (top 10):")
    result = con.execute("""
        SELECT
            customer_name,
            state,
            COUNT(DISTINCT order_id)            AS qtd_pedidos,
            ROUND(SUM(total_price), 2)          AS total_gasto,
            ROUND(SUM(total_price) /
                  COUNT(DISTINCT order_id), 2)  AS ticket_medio
        FROM fct_order_items
        WHERE status != 'cancelled'
        GROUP BY customer_name, state
        HAVING COUNT(DISTINCT order_id) >= 2
        ORDER BY ticket_medio DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    print("\n>> Top 10 produtos mais vendidos (por quantidade):")
    result = con.execute("""
        SELECT
            product_name,
            category,
            SUM(quantity)                      AS unidades_vendidas,
            ROUND(SUM(total_price), 2)         AS receita_total
        FROM fct_order_items
        WHERE status != 'cancelled'
        GROUP BY product_name, category
        ORDER BY unidades_vendidas DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()
    print("\n" + "=" * 60)
    print(f"Banco de dados salvo em: {DB_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    print("=" * 60)
    print("PIPELINE ETL — E-COMMERCE BATCH")
    print("=" * 60)

    customers, products, orders, order_items = extract()
    customers, products, orders, order_items, consolidated = transform(
        customers, products, orders, order_items
    )
    load(customers, products, orders, order_items, consolidated)
    run_analytics()

    print("\nPipeline concluído com sucesso!")
