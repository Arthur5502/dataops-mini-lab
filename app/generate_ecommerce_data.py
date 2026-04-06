import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker("pt_BR")
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 3000
N_PRODUCTS = 300
N_ORDERS = 10000
N_ORDER_ITEMS = 20000

CATEGORIES = [
    "Eletrônicos",
    "Roupas",
    "Calçados",
    "Casa e Decoração",
    "Esportes",
    "Beleza",
    "Livros",
    "Brinquedos",
    "Alimentos",
    "Informática",
]

ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

STATES = [
    "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "CE", "PE",
    "AM", "PA", "MT", "MS", "ES", "DF", "PB", "RN", "MA", "AL",
]

def random_date(start_year=2023, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def generate_customers():
    print(f"Gerando {N_CUSTOMERS} clientes...")
    path = OUTPUT_DIR / "customers.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["customer_id", "customer_name", "email", "city", "state", "signup_date"]
        )
        writer.writeheader()
        for i in range(1, N_CUSTOMERS + 1):
            writer.writerow(
                {
                    "customer_id": i,
                    "customer_name": fake.name(),
                    "email": fake.email(),
                    "city": fake.city(),
                    "state": random.choice(STATES),
                    "signup_date": random_date(2022, 2024),
                }
            )
    print(f"  -> {path}")
    return list(range(1, N_CUSTOMERS + 1))

def generate_products():
    print(f"Gerando {N_PRODUCTS} produtos...")
    path = OUTPUT_DIR / "products.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["product_id", "product_name", "category", "price"]
        )
        writer.writeheader()
        for i in range(1, N_PRODUCTS + 1):
            category = random.choice(CATEGORIES)
            price = round(random.uniform(10.0, 2000.0), 2)
            writer.writerow(
                {
                    "product_id": i,
                    "product_name": fake.catch_phrase()[:60],
                    "category": category,
                    "price": price,
                }
            )
    print(f"  -> {path}")
    return list(range(1, N_PRODUCTS + 1))

def generate_orders(customer_ids):
    print(f"Gerando {N_ORDERS} pedidos...")
    path = OUTPUT_DIR / "orders.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["order_id", "customer_id", "order_date", "status"]
        )
        writer.writeheader()
        for i in range(1, N_ORDERS + 1):
            writer.writerow(
                {
                    "order_id": i,
                    "customer_id": random.choice(customer_ids),
                    "order_date": random_date(2023, 2025),
                    "status": random.choice(ORDER_STATUSES),
                }
            )
    print(f"  -> {path}")
    return list(range(1, N_ORDERS + 1))

def generate_order_items(order_ids, product_ids):
    print(f"Gerando {N_ORDER_ITEMS} itens de pedido...")
    path = OUTPUT_DIR / "order_items.csv"

    products_price = {}
    with open(OUTPUT_DIR / "products.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            products_price[int(row["product_id"])] = float(row["price"])

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
            ],
        )
        writer.writeheader()
        for i in range(1, N_ORDER_ITEMS + 1):
            order_id = random.choice(order_ids)
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            unit_price = products_price[product_id]
            unit_price = round(unit_price * random.uniform(0.9, 1.1), 2)
            writer.writerow(
                {
                    "order_item_id": i,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                }
            )
    print(f"  -> {path}")

if __name__ == "__main__":
    print("=" * 50)
    print("Iniciando geração de dados sintéticos de e-commerce")
    print("=" * 50)

    customer_ids = generate_customers()
    product_ids = generate_products()
    order_ids = generate_orders(customer_ids)
    generate_order_items(order_ids, product_ids)

    print("\nConcluído! Arquivos gerados em data/raw/:")
    for f in sorted(OUTPUT_DIR.glob("*.csv")):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name}: {size_kb:.1f} KB")