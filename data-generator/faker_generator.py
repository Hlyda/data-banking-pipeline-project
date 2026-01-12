import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
import os
from dotenv import load_dotenv

load_dotenv()  # charge les variables d'environnement depuis .env

# -----------------------------
# Configuration du projet
# -----------------------------
NUM_CUSTOMERS = 10          # nombre de clients par itération
ACCOUNTS_PER_CUSTOMER = 2   # comptes par client
NUM_TRANSACTIONS = 50       # transactions par itération
MAX_TXN_AMOUNT = 1000.00    # montant max d'une transaction
CURRENCY = "EUR"
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")
DEFAULT_LOOP = True
SLEEP_SECONDS = 2

# CLI pour exécuter une seule itération
parser = argparse.ArgumentParser(description="Fake data generator with balance check")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Helpers
# -----------------------------
fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# -----------------------------
# Connexion à PostgreSQL
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
conn.autocommit = True
cur = conn.cursor()

# -----------------------------
# Génération des données
# -----------------------------
def run_iteration():
    customers = []

    # 1. Générer des clients
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()

        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]
        customers.append(customer_id)

    # 2. Générer des comptes
    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, initial_balance, CURRENCY),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)

    # 3. Générer des transactions
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
        related_account = None
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related_account),
        )

    print(f"\n✅ Generated {len(customers)} customers, {len(accounts)} accounts, {NUM_TRANSACTIONS} transactions.")

    # 4. Afficher les soldes après transactions
    print("\n--- Balance Check ---")
    cur.execute("""
    SELECT a.id, a.balance, COALESCE(SUM(
        CASE 
            WHEN t.txn_type='DEPOSIT' THEN t.amount
            WHEN t.txn_type='WITHDRAWAL' THEN -t.amount
            WHEN t.txn_type='TRANSFER' AND t.account_id = a.id THEN -t.amount
            WHEN t.txn_type='TRANSFER' AND t.related_account_id = a.id THEN t.amount
        END
    ),0) AS net_txn
    FROM accounts a
    LEFT JOIN transactions t ON a.id = t.account_id OR a.id = t.related_account_id
    GROUP BY a.id, a.balance
    ORDER BY a.id;
    """)
    for row in cur.fetchall():
        account_id, initial_balance, net_txn = row
        final_balance = initial_balance + net_txn
        print(f"Account {account_id}: initial={initial_balance}, net_txn={net_txn}, final={final_balance}")

# -----------------------------
# Boucle principale
# -----------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()
    sys.exit(0)
