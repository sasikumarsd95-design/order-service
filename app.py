import os, json, base64
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
import sqlalchemy

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()

def create_pool():
    db_socket = os.environ["DB_HOST"]
    return sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            database=os.environ["DB_NAME"],
            query={"unix_sock": f"{db_socket}/.s.PGSQL.5432"},
        ),
        pool_size=5, max_overflow=2,
        pool_recycle=1800, pool_pre_ping=True
    )

pool = None

@app.before_first_request
def startup():
    global pool
    pool = create_pool()
    with pool.connect() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                customer_email TEXT NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                status TEXT DEFAULT 'placed',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))

@app.route('/orders', methods=['POST'])
def create_order():
    data = request.get_json()
    if not data or 'customer_email' not in data or 'amount' not in data:
        return jsonify({'error': 'customer_email and amount are required'}), 400
    with pool.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                "INSERT INTO orders (customer_email, amount) "
                "VALUES (:email, :amount) RETURNING id"
            ),
            {"email": data["customer_email"], "amount": data["amount"]}
        )
        order_id = result.fetchone()[0]
    project = os.environ.get("PROJECT_ID", "")
    topic = f"projects/{project}/topics/order-events"
    event = json.dumps({
        "order_id": order_id,
        "customer_email": data["customer_email"],
        "amount": data["amount"]
    })
    publisher.publish(topic, event.encode(), event_type="order_placed")
    return jsonify({"order_id": order_id, "status": "placed"}), 201

@app.route('/health')
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
