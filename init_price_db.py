from app import app, db


with app.app_context():
    # Flask-SQLAlchemy v3+ ใช้ bind_key
    db.create_all(bind_key="price")
    print("✅ created price.db and tables (bind_key=price)")
