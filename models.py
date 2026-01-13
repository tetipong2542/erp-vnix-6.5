
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, date
from sqlalchemy import Index, UniqueConstraint

from utils import TH_TZ, now_thai

db = SQLAlchemy()

class Shop(db.Model):
    __tablename__ = "shops"
    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(db.String(64), index=True)
    name = db.Column(db.String(128), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ))

    __table_args__ = (
        db.UniqueConstraint("platform", "name", name="uq_shops_platform_name"),
    )


class ShopNameAlias(db.Model):
    __tablename__ = "shop_name_aliases"

    id = db.Column(db.Integer, primary_key=True)

    platform = db.Column(db.String(64), index=True, nullable=False)
    alias_name = db.Column(db.String(128), nullable=False)
    alias_key = db.Column(db.String(160), index=True, nullable=False)

    master_shop_id = db.Column(
        db.Integer,
        db.ForeignKey("shops.id"),
        nullable=False,
        index=True,
    )

    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)

    __table_args__ = (
        UniqueConstraint("platform", "alias_key", name="uq_shop_alias_platform_key"),
    )


class LogisticAlias(db.Model):
    __tablename__ = "logistic_aliases"

    id = db.Column(db.Integer, primary_key=True)

    alias_text = db.Column(db.String(255), nullable=False)
    alias_key = db.Column(db.String(300), index=True, nullable=False)

    master_text = db.Column(db.String(255), nullable=False)

    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)

    __table_args__ = (
        UniqueConstraint("alias_key", name="uq_logistic_alias_key"),
    )

class Product(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True)
    sku = db.Column(db.String(64), unique=True, nullable=False)
    brand = db.Column(db.String(120))
    model = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ))

class Stock(db.Model):
    __tablename__ = "stocks"
    id = db.Column(db.Integer, primary_key=True)
    sku = db.Column(db.String(64), nullable=False, index=True)
    qty = db.Column(db.Integer, default=0)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ), onupdate=lambda: datetime.now(TH_TZ))

class Sales(db.Model):
    __tablename__ = "sales"
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.String(128), nullable=False, index=True)
    po_no = db.Column(db.String(128))
    status = db.Column(db.String(64))  # เปิดใบขายครบตามจำนวนแล้ว / ยังไม่มีการเปิดใบขาย
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ))

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(16), default="user")  # admin/user
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ))


class UserPreference(db.Model):
    __tablename__ = "user_preferences"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    key = db.Column(db.String(64), nullable=False, index=True)
    value = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(TH_TZ),
        onupdate=lambda: datetime.now(TH_TZ),
    )

    __table_args__ = (
        db.UniqueConstraint("user_id", "key", name="uq_user_pref"),
    )

class OrderLine(db.Model):
    __tablename__ = "order_lines"
    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(db.String(20), nullable=False)
    shop_id = db.Column(db.Integer, db.ForeignKey("shops.id"), nullable=False)
    order_id = db.Column(db.String(128), nullable=False)
    sku = db.Column(db.String(64), nullable=False)
    qty = db.Column(db.Integer, default=1)
    item_name = db.Column(db.String(512))
    order_time = db.Column(db.DateTime)  # tz-aware
    logistic_type = db.Column(db.String(255))
    imported_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ))
    import_date = db.Column(db.Date)  # วันที่นำเข้า (อ้างอิง พ.ศ. ในหน้า UI)
    accepted = db.Column(db.Boolean, default=False)
    accepted_at = db.Column(db.DateTime)
    accepted_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"))
    accepted_by_username = db.Column(db.String(64))
    dispatch_round = db.Column(db.Integer)  # จ่ายงาน(รอบที่)

    # สถานะการพิมพ์ Warehouse Job Sheet
    printed_warehouse = db.Column(db.Integer, default=0)  # จำนวนครั้งที่พิมพ์
    printed_warehouse_at = db.Column(db.DateTime)  # ครั้งล่าสุดที่พิมพ์
    printed_warehouse_by = db.Column(db.String(64))  # ผู้พิมพ์ครั้งล่าสุด

    # สถานะการพิมพ์ Picking List
    printed_picking = db.Column(db.Integer, default=0)  # จำนวนครั้งที่พิมพ์
    printed_picking_at = db.Column(db.DateTime)  # ครั้งล่าสุดที่พิมพ์
    printed_picking_by = db.Column(db.String(64))  # ผู้พิมพ์ครั้งล่าสุด

    __table_args__ = (
        db.UniqueConstraint('platform', 'shop_id', 'order_id', 'sku', name='uq_orderline'),
    )

    @property
    def is_printed_warehouse(self):
        """ตรวจสอบว่าพิมพ์ Warehouse แล้วหรือยัง"""
        return self.printed_warehouse and self.printed_warehouse > 0

    @property
    def is_printed_picking(self):
        """ตรวจสอบว่าพิมพ์ Picking แล้วหรือยัง"""
        return self.printed_picking and self.printed_picking > 0


class SkuPrintHistory(db.Model):
    """ประวัติการพิมพ์ต่อ SKU (Picking List) โดยแยกตาม context (platform, shop, logistic)"""
    __tablename__ = "sku_print_history"

    id = db.Column(db.Integer, primary_key=True)
    sku = db.Column(db.String(64), nullable=False, index=True)
    platform = db.Column(db.String(20), nullable=False)
    shop_id = db.Column(db.Integer, nullable=True)
    logistic = db.Column(db.String(255), nullable=True)

    print_count = db.Column(db.Integer, default=0)
    last_printed_at = db.Column(db.DateTime)
    last_printed_by = db.Column(db.String(64))

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(TH_TZ), onupdate=lambda: datetime.now(TH_TZ))

    __table_args__ = (
        db.UniqueConstraint('sku', 'platform', 'shop_id', 'logistic', name='uq_sku_print_context'),
    )


# ============================
# Price Marketing Models (Merged)
# ============================

class SkuPricing(db.Model):
    """ข้อมูลฝั่งเรา (Internal) ต่อ SKU"""

    __bind_key__ = "price"
    __tablename__ = "sku_pricing"

    sku = db.Column(db.String(64), primary_key=True)

    # denormalize เพื่อให้ Price Dashboard ใช้ price.db ได้ล้วน ๆ (ไม่ต้องพึ่ง Product ใน data.db)
    brand = db.Column(db.String(120))
    name = db.Column(db.String(255))

    spec_text = db.Column(db.Text)

    stock_qty = db.Column(db.Integer)

    # Stock Internal = สต๊อกจริงของเรา (ตาม Template)
    stock_internal_qty = db.Column(db.Integer, default=0)

    # Monthly Sales = ยอดขายต่อเดือน (ใช้ทำ Aging บน Dashboard)
    monthly_sales_qty = db.Column(db.Integer, default=0)

    cost = db.Column(db.Float, default=0.0)  # ต้นทุน/หน่วย
    our_price = db.Column(db.Float, default=0.0)  # ราคาเราปัจจุบัน

    floor_price = db.Column(db.Float)  # ราคาต่ำสุดที่ยอมขาย
    min_margin_pct = db.Column(db.Float, default=0.0)  # %กำไรขั้นต่ำ (อิงจากราคาขาย)

    pack_cost = db.Column(db.Float, default=0.0)  # ค่าแพ็ค/ชิ้น (ประมาณการ)
    ship_subsidy = db.Column(db.Float, default=0.0)  # ค่าเฉลี่ยที่เราช่วยค่าส่ง

    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)


class BrandControl(db.Model):
    """ราคาควบคุมโดยแบรนด์ (Brand Control / MAP) ต่อ SKU"""

    __bind_key__ = "price"
    __tablename__ = "brand_controls"

    sku = db.Column(db.String(64), primary_key=True)

    # denormalize เพื่อให้ Price Dashboard ใช้ price.db ได้ล้วน ๆ
    brand = db.Column(db.String(120))
    name = db.Column(db.String(255))

    # ราคาที่แบรนด์คุม
    price_control = db.Column(db.Float)

    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)


class PlatformFeeSetting(db.Model):
    """ค่าธรรมเนียมแพลตฟอร์ม เพื่อคำนวณกำไรใกล้ความจริง"""

    __bind_key__ = "price"
    __tablename__ = "platform_fee_settings"

    # platform เป็น key หลักของแต่ละแพลตฟอร์ม (อนุญาตให้เพิ่มเอง)
    platform = db.Column(db.String(50), primary_key=True)
    label = db.Column(db.String(100))  # ชื่อไว้โชว์บนหน้าเว็บ
    is_active = db.Column(db.Boolean, default=True)  # เปิด/ปิดใช้งาน
    sort_order = db.Column(db.Integer, default=0)  # ลำดับ

    fee_pct = db.Column(db.Float, default=0.0)  # % (รวม VAT แล้วตามที่กรอก)
    fixed_fee = db.Column(db.Float, default=0.0)  # ค่าคงที่/ชิ้น

    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)


class PriceConfig(db.Model):
    """Config ฝั่งระบบราคา (price.db) เช่น Google Sheet URL"""

    __bind_key__ = "price"
    __tablename__ = "price_configs"

    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(db.String(64), nullable=False, index=True)
    name = db.Column(db.String(128), nullable=False, index=True)
    url = db.Column(db.Text)
    worksheet = db.Column(db.String(128))
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)

    __table_args__ = (
        UniqueConstraint("platform", "name", name="uq_price_configs_platform_name"),
    )


class PriceUserPreference(db.Model):
    """User preferences stored in price.db (bind: price)."""

    __bind_key__ = "price"
    __tablename__ = "price_user_preferences"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    key = db.Column(db.String(64), nullable=False, index=True)
    value = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)

    __table_args__ = (
        UniqueConstraint("user_id", "key", name="uq_price_user_pref"),
    )


class MarketItem(db.Model):
    """รายการคู่แข่งที่ติดตาม (ปักหมุดคู่แข่งไว้ต่อ SKU) + latest snapshot"""

    __bind_key__ = "price"
    __tablename__ = "market_items"

    id = db.Column(db.Integer, primary_key=True)

    sku = db.Column(db.String(64), nullable=False, index=True)
    platform = db.Column(db.String(20), nullable=False, index=True)  # Shopee/Lazada/TikTok
    shop_name = db.Column(db.String(255), nullable=False)
    product_url = db.Column(db.String(1024))

    # ร้าน Mall / Official store (สำหรับแสดงคอลัมน์ MALL ใน Dashboard)
    # เก็บเป็น boolean ใน DB แต่ SQLite อาจเป็น INTEGER 0/1
    is_mall = db.Column(db.Boolean, default=False)

    is_active = db.Column(db.Boolean, default=True)

    # latest snapshot (อัปเดตทุกครั้งที่มีการ import/บันทึกราคา)
    latest_listed_price = db.Column(db.Float)
    latest_shipping_fee = db.Column(db.Float)
    latest_voucher_discount = db.Column(db.Float)
    latest_coin_discount = db.Column(db.Float)
    latest_net_price = db.Column(db.Float)
    last_updated = db.Column(db.DateTime)

    note = db.Column(db.String(512))

    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)

    __table_args__ = (
        UniqueConstraint("sku", "platform", "shop_name", name="uq_market_item_sku_plat_shop"),
        Index("ix_market_item_sku_plat_net", "sku", "platform", "latest_net_price"),
    )


class MarketPriceLog(db.Model):
    """ประวัติราคา (เก็บทุกครั้งที่เช็ค/Import)"""

    __bind_key__ = "price"
    __tablename__ = "market_price_logs"

    id = db.Column(db.Integer, primary_key=True)

    market_item_id = db.Column(db.Integer, db.ForeignKey("market_items.id"), nullable=False, index=True)

    # denormalize ไว้ query ง่าย/เร็ว
    sku = db.Column(db.String(64), nullable=False, index=True)
    platform = db.Column(db.String(20), nullable=False, index=True)
    shop_name = db.Column(db.String(255), nullable=False)

    listed_price = db.Column(db.Float)
    shipping_fee = db.Column(db.Float)
    voucher_discount = db.Column(db.Float)
    coin_discount = db.Column(db.Float)
    net_price = db.Column(db.Float, nullable=False, index=True)

    captured_at = db.Column(db.DateTime, default=now_thai, index=True)
    checked_by = db.Column(db.String(64))
    note = db.Column(db.String(512))

    created_at = db.Column(db.DateTime, default=now_thai)

    __table_args__ = (
        Index("ix_market_log_sku_plat_net", "sku", "platform", "net_price"),
    )


class BrandOwnerSetting(db.Model):
    """ผูก Brand -> ผู้ดูแล (ใช้ใน Price Dashboard)"""

    __bind_key__ = "price"
    __tablename__ = "brand_owner_settings"

    # ใช้ brand เป็น key เพื่อให้แก้ไขง่าย และไม่มีการซ้ำ
    brand = db.Column(db.String(120), primary_key=True)
    owner = db.Column(db.String(64), nullable=False)

    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)


class PriceExportSetting(db.Model):
    """Global settings for Export Price tiers (price.db)."""

    __bind_key__ = "price"
    __tablename__ = "price_export_settings"

    id = db.Column(db.Integer, primary_key=True)

    # 5.1 step % (0-10)
    step_pct = db.Column(db.Float, default=5.0)

    # 5.2 min profit % (0-10) to prevent loss (based on cost)
    min_profit_pct = db.Column(db.Float, default=5.0)

    # 5.3 max loss % (0-50) for aging buckets (store as positive number)
    loss_aging3_pct = db.Column(db.Float, default=5.0)   # 3 เดือนขึ้นไป
    loss_aging6_pct = db.Column(db.Float, default=10.0)  # 6 เดือนขึ้นไป
    loss_aging12_pct = db.Column(db.Float, default=20.0) # 1 ปีขึ้นไป

    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)


# ============================
# Price Import Undo (Batches + Ops)
# ============================

class PriceImportBatch(db.Model):
    __bind_key__ = "price"
    __tablename__ = "price_import_batches"

    id = db.Column(db.Integer, primary_key=True)

    # internal / market / brand_control
    kind = db.Column(db.String(32), nullable=False, index=True)

    # file / gsheet
    source = db.Column(db.String(16), nullable=False)

    # file: filename | gsheet: sheet_url
    source_name = db.Column(db.String(1024))

    worksheet = db.Column(db.String(128))
    default_platform = db.Column(db.String(20))  # เฉพาะ market

    created_by = db.Column(db.String(64))
    created_at = db.Column(db.DateTime, default=now_thai, index=True)

    ok_rows = db.Column(db.Integer, default=0)
    skip_rows = db.Column(db.Integer, default=0)

    undone = db.Column(db.Boolean, default=False, index=True)
    undone_at = db.Column(db.DateTime)
    undone_by = db.Column(db.String(64))


class PriceImportOp(db.Model):
    __bind_key__ = "price"
    __tablename__ = "price_import_ops"

    id = db.Column(db.Integer, primary_key=True)

    batch_id = db.Column(
        db.Integer,
        db.ForeignKey("price_import_batches.id"),
        nullable=False,
        index=True,
    )

    # ลำดับเหตุการณ์ในรอบนั้น (ต้องใช้ undo แบบย้อนกลับ)
    seq = db.Column(db.Integer, nullable=False, index=True)

    # sku_pricing / brand_controls / market_items / market_price_logs
    table_name = db.Column(db.String(64), nullable=False)

    # pk ของตารางนั้น (sku หรือ id)
    pk = db.Column(db.String(255), nullable=False)

    # insert / update
    action = db.Column(db.String(16), nullable=False)

    # เก็บ "ก่อนแก้" เป็น JSON เพื่อ restore
    before_json = db.Column(db.Text)

    created_at = db.Column(db.DateTime, default=now_thai)

    __table_args__ = (
        Index("ix_price_import_ops_batch_seq", "batch_id", "seq"),
    )


# ============================
# Supplier Stock Models (bind: supplier)
# ============================


class SupplierSkuMaster(db.Model):
    __bind_key__ = "supplier"
    __tablename__ = "supplier_sku_master"

    id = db.Column(db.Integer, primary_key=True)

    sku = db.Column(db.String(64), nullable=False, index=True)
    sku_norm = db.Column(db.String(96), nullable=False, index=True)

    supplier = db.Column(db.String(64), nullable=False, index=True)
    supplier_norm = db.Column(db.String(96), nullable=False, index=True)

    sku_sup = db.Column(db.String(128), nullable=False)
    sku_sup_norm = db.Column(db.String(160), nullable=False, index=True)

    brand = db.Column(db.String(120), index=True)
    name = db.Column(db.String(255))

    stock_sup_qty = db.Column(db.Integer, default=0)
    stock_updated_at = db.Column(db.DateTime)

    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=now_thai)
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)

    __table_args__ = (
        UniqueConstraint("supplier_norm", "sku_sup_norm", name="uq_sup_supplier_skusup"),
        Index("ix_sup_sku_supplier_norm", "sku_norm", "supplier_norm"),
        Index("ix_sup_supplier_skusup_norm", "supplier_norm", "sku_sup_norm"),
    )


class SupplierConfig(db.Model):
    __bind_key__ = "supplier"
    __tablename__ = "supplier_configs"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True)  # GoogleSheet_SupplierSkuStock
    url = db.Column(db.Text)
    worksheet = db.Column(db.String(120))
    updated_at = db.Column(db.DateTime, default=now_thai, onupdate=now_thai)


class SupplierImportBatch(db.Model):
    __bind_key__ = "supplier"
    __tablename__ = "supplier_import_batches"

    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(64), nullable=False)  # supplier_sku_stock
    source = db.Column(db.String(32))  # file/gsheet
    source_name = db.Column(db.Text)
    worksheet = db.Column(db.String(120))
    ok_rows = db.Column(db.Integer, default=0)
    skip_rows = db.Column(db.Integer, default=0)
    created_by = db.Column(db.String(64))
    created_at = db.Column(db.DateTime, default=now_thai)
