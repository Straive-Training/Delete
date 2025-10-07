from flask import Flask, jsonify
import redis
import logging


app = Flask(__name__)

try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.ping()  # Test connection
    print(" Connected to Redis successfully!")
except redis.ConnectionError:
    print(" Failed to connect to Redis. Make sure the server is running.")


logging.basicConfig(
    filename="flask_redis_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


@app.route('/')
def home():
    """Home route showing basic info."""
    return jsonify(
        message="Welcome to Flask + Redis Example!",
        endpoints=[
            "/set/<key>/<value>",
            "/get/<key>",
            "/list/<key>/push/<value>",
            "/list/<key>",
            "/set/add/<key>/<value>",
            "/set/view/<key>",
            "/hash/<key>/<field>/<value>",
            "/hash/view/<key>",
            "/zset/<key>/<member>/<int:score>",
            "/zset/view/<key>",
            "/visits"
        ]
    )

# 1. String - Set value
@app.route('/set/<key>/<value>')
def set_value(key, value):
    try:
        r.set(key, value)
        logging.info(f"Key set: {key} -> {value}")
        return jsonify(status="success", message=f"Stored {key} = {value} in Redis")
    except Exception as e:
        logging.error(f"Error setting value: {e}")
        return jsonify(status="error", message=str(e)), 500

# 1a. String - Get value
@app.route('/get/<key>')
def get_value(key):
    """Get a value from Redis."""
    try:
        value = r.get(key)
        if value:
            value = value.decode('utf-8')
            logging.info(f"Key fetched: {key} -> {value}")
            return jsonify(status="success", key=key, value=value)
        else:
            return jsonify(status="error", message=f"Key '{key}' not found"), 404
    except Exception as e:
        logging.error(f"Error fetching value: {e}")
        return jsonify(status="error", message=str(e)), 500

# 2. List - Push value
@app.route('/list/<key>/push/<value>')
def push_list(key, value):
    try:
        r.lpush(key, value)
        logging.info(f"Pushed to list {key}: {value}")
        return jsonify(status="success", message=f"Pushed {value} to list {key}")
    except Exception as e:
        logging.error(f"Error pushing to list: {e}")
        return jsonify(status="error", message=str(e)), 500

# 2a. List - View contents
@app.route('/list/<key>')
def get_list(key):
    try:
        raw_values = r.lrange(key, 0, -1)
        values = [item.decode('utf-8') if isinstance(item, bytes) else item for item in raw_values]
        if not values:
            return jsonify(status="empty", message=f"List {key} is empty or does not exist"), 404
        return jsonify(status="success", key=key, values=values)
    except Exception as e:
        logging.error(f"Error retrieving list {key}: {e}")
        return jsonify(status="error", message=str(e)), 500


@app.route('/set/add/<key>/<value>')
def add_set_member(key, value):
    try:
        r.sadd(key, value)
        logging.info(f"Added to set {key}: {value}")
        return jsonify(status="success", message=f"Added {value} to set {key}")
    except redis.exceptions.ResponseError as e:
        if "WRONGTYPE" in str(e):
            return jsonify(status="error", message=f"Key '{key}' exists but is not a set"), 400
        else:
            logging.error(f"Error adding to set: {e}")
            return jsonify(status="error", message=str(e)), 500
    except Exception as e:
        logging.error(f"Error adding to set: {e}")
        return jsonify(status="error", message=str(e)), 500

@app.route('/set/view/<key>')
def view_set(key):
    try:
        members = r.smembers(key)
        if not members:
            return jsonify(status="empty", message=f"Set {key} is empty or does not exist"), 404
        decoded_members = [member.decode('utf-8') if isinstance(member, bytes) else member for member in members]
        return jsonify(status="success", key=key, members=decoded_members)
    except Exception as e:
        logging.error(f"Error retrieving set {key}: {e}")
        return jsonify(status="error", message=str(e)), 500

@app.route('/hash/<key>/<field>/<value>')
def set_hash_field(key, field, value):
    try:
        r.hset(key, field, value)
        logging.info(f"Hash set: {key}[{field}] = {value}")
        return jsonify(status="success", message=f"Set {field} = {value} in hash {key}")
    except Exception as e:
        logging.error(f"Error setting hash field: {e}")
        return jsonify(status="error", message=str(e)), 500

@app.route('/hash/view/<key>')
def view_hash(key):
    try:
        hash_data = r.hgetall(key)
        if not hash_data:
            return jsonify(status="empty", message=f"Hash {key} is empty or does not exist"), 404
        decoded_hash = {k.decode('utf-8'): v.decode('utf-8') for k, v in hash_data.items()}
        return jsonify(status="success", key=key, fields=decoded_hash)
    except Exception as e:
        logging.error(f"Error retrieving hash {key}: {e}")
        return jsonify(status="error", message=str(e)), 500


# 5. Sorted Set - Add member with score
@app.route('/zset/<key>/<member>/<int:score>')
def add_zset_member(key, member, score):
    try:
        r.zadd(key, {member: score})
        logging.info(f"Added to sorted set {key}: {member} with score {score}")
        return jsonify(status="success", message=f"Added {member} with score {score} to sorted set {key}")
    except Exception as e:
        logging.error(f"Error adding to sorted set: {e}")
        return jsonify(status="error", message=str(e)), 500

@app.route('/zset/view/<key>')
def view_zset(key):
    try:
        items = r.zrange(key, 0, -1, withscores=True)
        if not items:
            return jsonify(status="empty", message=f"Sorted set {key} is empty or does not exist"), 404
        formatted_items = [
            {"member": member.decode('utf-8') if isinstance(member, bytes) else member, "score": score}
            for member, score in items
        ]
        return jsonify(status="success", key=key, members=formatted_items)
    except Exception as e:
        logging.error(f"Error retrieving sorted set {key}: {e}")
        return jsonify(status="error", message=str(e)), 500


# Page visits counter
@app.route('/visits')
def visit_counter():
    try:
        visits = r.incr('visit_count')
        logging.info(f"Visit count updated: {visits}")
        return jsonify(message="Page visit counter", total_visits=visits)
    except Exception as e:
        logging.error(f"Error updating visit count: {e}")
        return jsonify(status="error", message=str(e)), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True)
