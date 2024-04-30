import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta


# create an instance of the Faker class and localize it 'fa_IR'
fake = Faker(['fa_IR'])


# Function to generate random datetime within a range
def generate_random_datetime(start_date, end_date):
    time_diff = end_date - start_date
    random_time_diff = random.randint(0, time_diff.total_seconds())
    random_datetime = start_date + timedelta(seconds=random_time_diff)
    return random_datetime



# create fake user data
def generate_users(num_users):
    users = []
    for _ in range(num_users):
        user = {
            'name': fake.name(),
            'email': fake.email(),
            'address': fake.address(),
            'phone_number': fake.phone_number()
        }
        users.append(user)
    return users

# Generate fake product data
def generate_products(num_products):
    products = []
    for _ in range(num_products):
        product = {
            'name': fake.word().capitalize(),
            'price': round(random.uniform(10, 1000), 2),
            'description': fake.text(),
            'category': fake.word().capitalize()
        }
        products.append(product)
    return products

# Generate fake payment data
def generate_payments(users, products, num_payments):
    payments = []
    current_time = datetime.now()
    for _ in range(num_payments):
        random_user = random.choice(users)
        random_product = random.choice(products)
        random_order_time = generate_random_datetime(current_time - timedelta(days=365), current_time)
        payment = {
            'user_id': random_user['id'],
            'product_id': random_product['id'],
            'amount': round(random.uniform(1, 500), 2),
            'order_time': random_order_time.strftime('%Y-%m-%d %H:%M:%S')
        }
        payments.append(payment)

    return payments


    

if __name__ == "__main__":
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="dwhPassWprd",
        database="transactions"
    )
    mycursor = mydb.cursor()

    # Define SQL queries to create tables
    create_users_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255),
        address TEXT,
        phone_number VARCHAR(20)
    )
    """

    create_products_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        price DECIMAL(10, 2),
        description TEXT,
        category VARCHAR(255)
    )
    """

    create_payments_table_query = """
    CREATE TABLE IF NOT EXISTS payments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        product_id INT,
        amount DECIMAL(10, 2),
        order_time DATETIME,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    """

    # Execute SQL queries to create tables
    mycursor.execute(create_users_table_query)
    mycursor.execute(create_products_table_query)
    mycursor.execute(create_payments_table_query)

    # Generate fake data
    num_users = 10
    num_products = 200
    num_orders = 1000

    users = generate_users(num_users)
    products = generate_products(num_products)

    # Insert generated users into the database
    for user in users:
        sql = "INSERT INTO users (name, email, address, phone_number) VALUES (%s, %s, %s, %s)"
        val = (user['name'], user['email'], user['address'], user['phone_number'])
        mycursor.execute(sql, val)
        mydb.commit()
    
    # Insert generated products into the database
    for product in products:
        sql = "INSERT INTO products (name, price, description, category) VALUES (%s, %s, %s, %s)"
        val = (product['name'], product['price'], product['description'], product['category'])
        mycursor.execute(sql, val)
        mydb.commit()


    # fetch users and products from database
    mycursor.execute("SELECT id FROM users")
    users_id = mycursor.fetchall()
    users_id = [{'id': item[0]} for item in users_id]


    mycursor.execute("SELECT id FROM products")
    products_id = mycursor.fetchall()
    products_id = [{'id': item[0]} for item in products_id]

    payments = generate_payments(users_id, products_id, num_orders)



    for payment in payments:
        sql = "INSERT INTO payments (user_id, product_id, amount, order_time) VALUES (%s, %s, %s, %s)"
        val = (payment['user_id'], payment['product_id'], payment['amount'],  payment['order_time'])
        mycursor.execute(sql, val)
        mydb.commit()

    print("Data inserted successfully!")

