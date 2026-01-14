
# importers.py
from __future__ import annotations

import json
import re
import pandas as pd
from datetime import datetime, date
from flask import flash
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from utils import (
    parse_datetime_guess,
    normalize_platform,
    normalize_shop_name,
    normalize_text_key,
    TH_TZ,
    now_thai,
)
from models import (
    db,
    Shop,
    ShopNameAlias,
    LogisticAlias,
    Product,
    Stock,
    Sales,
    OrderLine,
    SkuPricing,
    BrandControl,
    PlatformFeeSetting,
    MarketItem,
    MarketPriceLog,
    PriceImportOp,
    SupplierSkuMaster,
)

# ===== Column dictionaries =====
COMMON_ORDER_ID   = ["orderNumber","Order Number","order_id","Order ID","order_sn","Order No","เลข Order","No.","OrderNo"]
COMMON_SKU        = ["sellerSku","Seller SKU","SKU","Sku","Item SKU","SKU Reference No.","รหัสสินค้า"]
COMMON_ITEM_NAME  = ["itemName","Item Name","Product Name","ชื่อสินค้า","ชื่อรุ่น","title","name"]
COMMON_QTY        = ["quantity","Quantity","Qty","จำนวน","จำนวนที่สั่ง","Purchased Qty","Order Item Qty"]
COMMON_ORDER_TIME = ["order_time","createdAt","create_time","created_time","Order Time","OrderDate","Order Date","วันที่สั่งซื้อ","Paid Time","paid_time","Created Time","createTime","Created Time"]
COMMON_LOGISTICS  = ["logistics_service","logistic_type","Logistics Service","Shipping Provider","ประเภทขนส่ง","Shipment Method","Delivery Type"]

# เพิ่มคีย์หัวคอลัมน์สำหรับ "ชื่อร้าน"
COMMON_SHOP = ["ชื่อร้าน","Shop","Shop Name","Store","Store Name","ร้าน","ร้านค้า"]

# >>> ขยายตัวเลือกหัวคอลัมน์สต็อก (กันเคสหลากหลาย/ภาษาไทย-อังกฤษ)
COMMON_STOCK_SKU  = [
    "รหัสสินค้า","SKU","sku","รหัส","รหัส สินค้า","รหัสสินค้า*",
    "รหัสสินค้า Sabuy Soft","SKU Reference No.","รหัส/sku","รหัสสินค้า/sku"
]
COMMON_STOCK_QTY  = [
    "คงเหลือ","Stock","stock","Available","จำนวน","Qty","QTY","STOCK","ปัจจุบัน",
    "ยอดคงเหลือ","จำนวนคงเหลือ","คงเหลือในสต๊อก"
]

COMMON_PRODUCT_SKU   = ["รหัสสินค้า","SKU","sku"]
COMMON_PRODUCT_BRAND = ["Brand","แบรนด์"]
COMMON_PRODUCT_MODEL = ["ชื่อสินค้า","รุ่น","Model","Product"]

# ===== helpers =====
def first_existing(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    # fuzzy contains (lower)
    lower_cols = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        for col_lower, original in lower_cols.items():
            if key == col_lower or key in col_lower:
                return original
    return None

def clean_shop_name(s) -> str:
    return normalize_shop_name(s)


def resolve_logistic_master(raw: str | None) -> str:
    raw = (raw or "").strip()
    if not raw:
        return "-"
    k = normalize_text_key(raw)
    ali = LogisticAlias.query.filter_by(alias_key=k).first()
    if ali and ali.master_text:
        return (ali.master_text or "").strip() or "-"
    return raw

def get_or_create_shop(platform, shop_name):
    platform = normalize_platform(platform) or "อื่นๆ"
    name = normalize_shop_name(shop_name) or "-"

    # 1) alias mapping -> master_shop_id
    key = normalize_text_key(name)
    ali = ShopNameAlias.query.filter_by(platform=platform, alias_key=key).first()
    if ali:
        master = Shop.query.get(ali.master_shop_id)
        if master:
            return master

    # 2) find shop under platform (case-insensitive)
    shop = (
        Shop.query
        .filter(Shop.platform == platform)
        .filter(func.lower(Shop.name) == name.lower())
        .first()
    )
    if not shop:
        shop = Shop(platform=platform, name=name)
        db.session.add(shop)
        db.session.commit()
    return shop

# ===== Importers =====
# Optimized: ใช้ batch query แทน query ทีละแถว (แก้ปัญหา N+1 query กับ Turso)
def import_products(df: pd.DataFrame) -> int:
    sku_col   = first_existing(df, COMMON_PRODUCT_SKU)   or "รหัสสินค้า"
    brand_col = first_existing(df, COMMON_PRODUCT_BRAND) or "Brand"
    model_col = first_existing(df, COMMON_PRODUCT_MODEL) or "ชื่อสินค้า"

    # เตรียมข้อมูล SKU ทั้งหมด
    sku_list = []
    for _, row in df.iterrows():
        sku = str(row.get(sku_col, "")).strip()
        if sku:
            sku_list.append(sku)

    if not sku_list:
        return 0

    # Batch query: โหลด Product ทั้งหมดที่มีอยู่แล้วในครั้งเดียว
    existing_products = Product.query.filter(Product.sku.in_(sku_list)).all()
    product_map = {p.sku: p for p in existing_products}

    cnt = 0
    new_products = []

    for _, row in df.iterrows():
        sku = str(row.get(sku_col, "")).strip()
        if not sku:
            continue

        prod = product_map.get(sku)
        if not prod:
            prod = Product(sku=sku)
            product_map[sku] = prod  # เพิ่มเข้า map เพื่อกัน duplicate ใน batch
            new_products.append(prod)

        prod.brand = str(row.get(brand_col, "")).strip()
        prod.model = str(row.get(model_col, "")).strip()
        cnt += 1

    # Batch insert Product ใหม่
    if new_products:
        db.session.add_all(new_products)

    db.session.commit()
    return cnt

# >>> ฟังก์ชันนี้ถูกแพตช์ใหม่ให้ทน NaN/หัวคอลัมน์หลายแบบ + Full Sync Mode
# >>> Optimized: ใช้ batch query แทน query ทีละแถว (แก้ปัญหา N+1 query กับ Turso)
def import_stock(df: pd.DataFrame, full_replace: bool = True) -> int:
    """
    นำเข้าสต็อกจาก DataFrame:
    - รองรับหัวคอลัมน์หลายแบบ (ไทย/อังกฤษ)
    - Qty ว่าง/NaN จะถูกมองเป็น 0
    - รวมยอดเมื่อไฟล์มี SKU ซ้ำหลายบรรทัด
    - โหมด full_replace=True: SKU ที่ไม่อยู่ในไฟล์/ชีต ให้ถือว่าเป็น 0 (SabuySoft)
    คืนค่าจำนวน SKU ที่บันทึก (insert/update)
    """
    sku_col = first_existing(df, COMMON_STOCK_SKU)
    qty_col = first_existing(df, COMMON_STOCK_QTY)
    if not sku_col:
        raise ValueError("ไม่พบคอลัมน์ SKU/รหัสสินค้า ในไฟล์สต็อก")
    if not qty_col:
        raise ValueError("ไม่พบคอลัมน์ คงเหลือ/Qty/Stock ในไฟล์สต็อก")

    df = df.copy()
    df.rename(columns={sku_col: "sku", qty_col: "qty"}, inplace=True)

    df["sku"] = df["sku"].astype(str).fillna("").str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)

    # คัดแถวที่ไม่มี SKU
    df = df[df["sku"] != ""]

    # ✅ SabuySoft rule: ถ้า SKU หายไป ต้องถือว่าเป็น 0
    # ทำ full sync โดย reset ทั้งตารางเป็น 0 ก่อน แล้วค่อย update ตามไฟล์
    if full_replace:
        reset_data = {Stock.qty: 0, Stock.updated_at: datetime.now(TH_TZ)}
        Stock.query.update(reset_data, synchronize_session=False)

        # ถ้าไฟล์ว่างจริง ๆ = แปลว่าไม่มี SKU ไหนเหลือเลย → ทั้งหมดเป็น 0
        if df.empty:
            db.session.commit()
            return 0
    else:
        if df.empty:
            return 0

    # รวมยอดตาม SKU (กันไฟล์ซ้ำแถว)
    agg = df.groupby("sku", as_index=False)["qty"].sum()

    # ===== OPTIMIZED: Batch query แทน query ทีละแถว =====
    sku_list = agg["sku"].tolist()

    # 1. โหลด Stock ทั้งหมดที่ต้องการในครั้งเดียว
    existing_stocks = Stock.query.filter(Stock.sku.in_(sku_list)).all()
    stock_map = {st.sku: st for st in existing_stocks}

    # 2. โหลด Product ทั้งหมดที่ต้องการในครั้งเดียว (สำหรับ sync stock_qty)
    existing_products = Product.query.filter(Product.sku.in_(sku_list)).all()
    product_map = {p.sku: p for p in existing_products}

    # 3. เตรียมข้อมูลสำหรับ insert/update
    now_ts = datetime.now(TH_TZ)
    new_stocks = []

    saved = 0
    for _, row in agg.iterrows():
        sku = row["sku"]
        qty = int(row["qty"] or 0)

        st = stock_map.get(sku)
        if not st:
            # สร้าง Stock ใหม่
            new_stocks.append(Stock(sku=sku, qty=qty))
        else:
            # Update Stock ที่มีอยู่
            st.qty = qty
            st.updated_at = now_ts

        # ถ้ามีฟิลด์ product.stock_qty ให้ sync ด้วย
        prod = product_map.get(sku)
        if prod is not None and hasattr(prod, "stock_qty"):
            try:
                prod.stock_qty = qty
            except Exception:
                # กันชนิดคอลัมน์ไม่ใช่ int
                pass

        saved += 1

    # 4. Batch insert Stock ใหม่ทั้งหมด
    if new_stocks:
        db.session.add_all(new_stocks)

    db.session.commit()
    return saved

# Optimized: ใช้ batch query แทน query ทีละแถว (แก้ปัญหา N+1 query กับ Turso)
def import_sales(df: pd.DataFrame) -> dict:
    """
    นำเข้าข้อมูลใบสั่งขาย (Sales)
    Returns: Dict ที่มี {'ids': [...], 'skipped': [...]}
        - ids: List ของ Order ID ที่ทำการ Create/Update สำเร็จ
        - skipped: List ของ Dict ที่มีข้อมูลแถวที่ถูกข้าม
    """
    # 1. หาชื่อคอลัมน์
    col_oid = first_existing(df, ["เลข Order", "Order ID", "order_id", "Order No", "เลขที่คำสั่งซื้อ", "orderNumber", "Order Number"])
    col_po  = first_existing(df, ["เลขที่ PO", "PO", "PO No", "เลขที่เอกสาร", "Document No", "เอกสาร"])
    col_st  = first_existing(df, ["สถานะ", "Status", "สถานะการขาย", "Sales Status"])

    if not col_oid:
        raise ValueError("ไม่พบคอลัมน์ 'เลข Order' หรือ 'Order ID' ในไฟล์")

    processed_ids = []  # เก็บ Order ID ที่ทำสำเร็จ
    skipped_rows = []   # เก็บข้อมูลแถวที่ถูกข้าม

    # ===== OPTIMIZED: เตรียม Order ID ทั้งหมดก่อน แล้ว batch query =====
    # 2. แปลงข้อมูลและเก็บ Order ID ที่ valid
    valid_rows = []  # เก็บ (idx, oid, row) ที่ valid
    oid_list = []    # เก็บ Order ID ทั้งหมดสำหรับ batch query

    for idx, row in df.iterrows():
        raw_oid = row.get(col_oid, "")

        # กรณี Order ID เป็นตัวเลขขนาดใหญ่ (scientific notation)
        if pd.notna(raw_oid):
            try:
                if isinstance(raw_oid, (int, float)):
                    oid = str(int(raw_oid)).strip()
                else:
                    oid = str(raw_oid).strip()
            except (ValueError, OverflowError):
                oid = str(raw_oid).strip()
        else:
            oid = ""

        # ข้ามถ้าไม่มี Order ID
        if not oid or oid == 'nan' or oid == 'None':
            skipped_rows.append({
                "row_number": idx + 2,
                "reason": "Order ID ว่างเปล่า",
                "order_id": raw_oid if pd.notna(raw_oid) else "(ว่าง)",
                "po_no": row.get(col_po, "") if col_po else "",
                "status": row.get(col_st, "") if col_st else ""
            })
            continue

        valid_rows.append((idx, oid, row))
        oid_list.append(oid)

    if not oid_list:
        return {"ids": processed_ids, "skipped": skipped_rows}

    # 3. Batch query: โหลด Sales ทั้งหมดที่มีอยู่แล้วในครั้งเดียว
    existing_sales = Sales.query.filter(Sales.order_id.in_(oid_list)).all()
    sales_map = {s.order_id: s for s in existing_sales}

    # 4. ประมวลผลแต่ละ row
    new_sales = []

    for idx, oid, row in valid_rows:
        try:
            sale = sales_map.get(oid)

            if not sale:
                sale = Sales(order_id=oid)
                sales_map[oid] = sale  # เพิ่มเข้า map เพื่อกัน duplicate
                new_sales.append(sale)

            # อัปเดตข้อมูล
            if col_po and pd.notna(row.get(col_po)):
                val_po = str(row.get(col_po)).strip()
                if val_po:
                    sale.po_no = val_po

            if col_st and pd.notna(row.get(col_st)):
                val_st = str(row.get(col_st)).strip()
                if val_st:
                    sale.status = val_st

            processed_ids.append(oid)

        except Exception as e:
            skipped_rows.append({
                "row_number": idx + 2,
                "reason": f"เกิดข้อผิดพลาด: {str(e)}",
                "order_id": oid,
                "po_no": row.get(col_po, "") if col_po else "",
                "status": row.get(col_st, "") if col_st else ""
            })
            continue

    # 5. Batch insert Sales ใหม่
    if new_sales:
        db.session.add_all(new_sales)

    db.session.commit()

    return {
        "ids": processed_ids,
        "skipped": skipped_rows
    }

# ============================
# INSERT-ONLY ORDER IMPORTER
# ============================
# Optimized: ใช้ batch query แทน query ทีละแถว (แก้ปัญหา N+1 query กับ Turso)
def import_orders(df: pd.DataFrame, platform: str, shop_name: str | None, import_date: date) -> dict:
    """
    นำเข้าออเดอร์แบบ INSERT-ONLY พร้อมส่งคืนสถิติละเอียด

    Returns dict:
        {
            'added': int,           # จำนวน Order ID ที่เพิ่มสำเร็จ (ไม่ซ้ำ)
            'duplicates': int,      # จำนวน Order ID ที่ซ้ำ (ข้าม)
            'failed': int,          # จำนวน Order ID ที่ไม่สำเร็จ
            'errors': list,         # รายการสาเหตุที่ไม่สำเร็จ (สูงสุด 10 รายการ)
            'added_ids': list,      # รายชื่อ Order ID ที่เพิ่มสำเร็จ
            'duplicate_ids': list,  # รายชื่อ Order ID ที่ซ้ำ
            'failed_ids': list      # รายชื่อ Order ID ที่ไม่สำเร็จ
        }
    นับยอดตาม Order ID ไม่ซ้ำ (Unique Order IDs)
    """
    platform_std = normalize_platform(platform)

    # --- หา columns จากหลายแพลตฟอร์ม ---
    shop_col  = first_existing(df, COMMON_SHOP)
    order_col = first_existing(df, COMMON_ORDER_ID)
    sku_col   = first_existing(df, COMMON_SKU)
    name_col  = first_existing(df, COMMON_ITEM_NAME)
    qty_col   = first_existing(df, COMMON_QTY)
    time_col  = first_existing(df, COMMON_ORDER_TIME)
    logi_col  = first_existing(df, COMMON_LOGISTICS)

    stats = {
        "added": 0,
        "duplicates": 0,           # รวมซ้ำทั้งหมด (old + today)
        "duplicates_old": 0,       # ซ้ำข้ามวัน (แสดงในการ์ด)
        "duplicates_today": 0,     # ซ้ำในวันเดียวกัน (ไม่แสดงในการ์ด)
        "failed": 0,
        "errors": [],  # เก็บสาเหตุที่ไม่สำเร็จ (สูงสุด 10 รายการ)
        "added_ids": [],
        "duplicate_ids": [],
        "duplicate_old_ids": [],   # รายการ Order ID ที่ซ้ำข้ามวัน
        "duplicate_today_ids": [], # รายการ Order ID ที่ซ้ำในวัน
        "failed_ids": []
    }

    if not order_col or not sku_col:
        stats["errors"].append("ไม่พบคอลัมน์ Order ID หรือ SKU ในไฟล์")
        return stats

    # fallback ชื่อร้านจากฟอร์ม (ถ้ามี)
    fallback_shop = clean_shop_name(shop_name) if shop_name else ""

    # Group ข้อมูลตาม Order ID ก่อน (เพื่อจัดการเป็นราย Order)
    # key = (shop, order_id), value = list of items
    grouped: dict[tuple[str, str], list[dict]] = {}
    failed_oids_in_parsing: set[str] = set()

    # เก็บข้อมูลสำหรับ batch query
    all_shop_names: set[str] = set()
    all_skus: set[str] = set()
    all_logi_raw: set[str] = set()

    for idx, row in df.iterrows():
        oid = str(row.get(order_col, "")).strip()
        sku = str(row.get(sku_col, "")).strip()

        # เช็คข้อมูลสำคัญ
        if not oid or not sku:
            if oid and oid not in failed_oids_in_parsing:
                failed_oids_in_parsing.add(oid)
                if oid not in stats["failed_ids"]:
                    stats["failed_ids"].append(oid)
                    stats["failed"] += 1
            elif not oid:
                # ไม่มี OID เลย นับ failed แบบไม่มี ID
                stats["failed"] += 1
            if len(stats["errors"]) < 10:
                stats["errors"].append(f"แถว {idx+2}: ไม่มี Order ID หรือ SKU")
            continue

        sname = clean_shop_name(row.get(shop_col)) if shop_col else fallback_shop
        if not sname:
            if oid not in failed_oids_in_parsing:
                failed_oids_in_parsing.add(oid)
                if oid not in stats["failed_ids"]:
                    stats["failed_ids"].append(oid)
                    stats["failed"] += 1
            if len(stats["errors"]) < 10:
                stats["errors"].append(f"Order {oid}: ไม่ระบุชื่อร้าน")
            continue

        qty = pd.to_numeric(row.get(qty_col), errors="coerce") if qty_col else None
        qty = int(qty) if pd.notnull(qty) else 1

        logi_raw = str(row.get(logi_col, "") or "") if logi_col else ""

        key = (sname, oid)
        if key not in grouped:
            grouped[key] = []

        grouped[key].append({
            "sku": sku,
            "qty": max(qty, 0),
            "name": str(row.get(name_col, "") or ""),
            "time": row.get(time_col) if time_col else None,
            "logi": logi_raw,
        })

        # เก็บข้อมูลสำหรับ batch query
        all_shop_names.add(sname)
        all_skus.add(sku)
        if logi_raw:
            all_logi_raw.add(logi_raw)

    if not grouped and stats["failed"] == 0:
        return stats  # Empty but valid file structure

    has_product_fk = hasattr(OrderLine, "product_id")

    # ===== OPTIMIZED: Batch pre-load data =====

    # 1. Batch load/create Shops
    shop_map: dict[str, Shop] = {}  # name -> Shop
    if all_shop_names:
        existing_shops = (
            Shop.query
            .filter(Shop.platform == platform_std)
            .filter(func.lower(Shop.name).in_([n.lower() for n in all_shop_names]))
            .all()
        )
        for s in existing_shops:
            shop_map[s.name.lower()] = s

        # สร้าง Shops ที่ยังไม่มี
        new_shops = []
        for sname in all_shop_names:
            if sname.lower() not in shop_map:
                new_shop = Shop(platform=platform_std, name=sname)
                new_shops.append(new_shop)
                shop_map[sname.lower()] = new_shop

        if new_shops:
            db.session.add_all(new_shops)
            db.session.flush()  # ให้ได้ shop.id

    # 2. Batch load Products (ถ้าต้องการ product_id)
    product_map: dict[str, Product] = {}
    if has_product_fk and all_skus:
        existing_products = Product.query.filter(Product.sku.in_(list(all_skus))).all()
        product_map = {p.sku: p for p in existing_products}

    # 3. Batch load LogisticAlias
    logi_map: dict[str, str] = {}  # alias_key -> master_text
    if all_logi_raw:
        logi_keys = [normalize_text_key(r) for r in all_logi_raw if r]
        if logi_keys:
            existing_logi = LogisticAlias.query.filter(LogisticAlias.alias_key.in_(logi_keys)).all()
            for la in existing_logi:
                if la.master_text:
                    logi_map[la.alias_key] = la.master_text

    # 4. Batch check existing OrderLines (เช็ค duplicates)
    # เก็บ (shop_id, order_id) -> OrderLine ที่มีอยู่แล้ว
    existing_orderlines_map: dict[tuple[int, str], OrderLine] = {}
    order_ids_to_check = [oid for (sname, oid) in grouped.keys()]
    shop_ids_to_check = [shop_map.get(sname.lower()).id for (sname, oid) in grouped.keys() if shop_map.get(sname.lower())]

    if order_ids_to_check and shop_ids_to_check:
        # Query ทั้งหมดในครั้งเดียว
        existing_lines = (
            OrderLine.query
            .filter(OrderLine.shop_id.in_(set(shop_ids_to_check)))
            .filter(OrderLine.order_id.in_(set(order_ids_to_check)))
            .all()
        )
        for line in existing_lines:
            key = (line.shop_id, line.order_id)
            if key not in existing_orderlines_map:
                existing_orderlines_map[key] = line

    # Helper function สำหรับ resolve logistic (ใช้ cache)
    def resolve_logistic_cached(raw: str) -> str:
        raw = (raw or "").strip()
        if not raw:
            return "-"
        k = normalize_text_key(raw)
        if k in logi_map:
            return (logi_map[k] or "").strip() or "-"
        return raw

    # Process แต่ละ Order (ระดับ Transaction)
    new_orderlines = []

    for (sname, oid), items in grouped.items():
        try:
            shop = shop_map.get(sname.lower())
            if not shop:
                # กรณีหา shop ไม่เจอ (ไม่น่าเกิด)
                if oid not in stats["failed_ids"]:
                    stats["failed"] += 1
                    stats["failed_ids"].append(oid)
                if len(stats["errors"]) < 10:
                    stats["errors"].append(f"Order {oid}: ไม่พบร้าน {sname}")
                continue

            # เช็คว่า Order นี้เคยมีในระบบแล้วหรือยัง (ใช้ cached data)
            exists = existing_orderlines_map.get((shop.id, oid))
            if exists:
                if oid not in stats["duplicate_ids"]:
                    stats["duplicates"] += 1
                    stats["duplicate_ids"].append(oid)

                    # เช็คว่าซ้ำข้ามวันหรือซ้ำในวันเดียวกัน
                    is_old_duplicate = True
                    if exists.import_date and exists.import_date == import_date:
                        is_old_duplicate = False

                    if is_old_duplicate:
                        stats["duplicates_old"] += 1
                        stats["duplicate_old_ids"].append(oid)
                    else:
                        stats["duplicates_today"] += 1
                        stats["duplicate_today_ids"].append(oid)
                continue

            # ถ้ายังไม่มี -> เพิ่มสินค้าลง DB
            # รวม SKU ซ้ำใน Order เดียวกัน
            sku_agg: dict[str, dict] = {}
            for item in items:
                sku = item["sku"]
                if sku not in sku_agg:
                    sku_agg[sku] = {
                        "qty": 0,
                        "name": item.get("name", ""),
                        "time": item.get("time"),
                        "logi": item.get("logi", ""),
                    }
                sku_agg[sku]["qty"] += item.get("qty", 0)
                if not sku_agg[sku].get("name"):
                    sku_agg[sku]["name"] = item.get("name", "")
                if item.get("time"):
                    sku_agg[sku]["time"] = item.get("time")
                if item.get("logi"):
                    sku_agg[sku]["logi"] = item.get("logi")

            items_added_count = 0
            for sku, rec in sku_agg.items():
                order_time = parse_datetime_guess(rec.get("time")) if rec.get("time") is not None else None
                logistic_type = resolve_logistic_cached(rec.get("logi") or "")

                ol_kwargs = dict(
                    platform=platform_std,
                    shop_id=shop.id,
                    order_id=oid,
                    sku=sku,
                    item_name=rec.get("name", "")[:255],
                    qty=int(rec.get("qty") or 0) or 1,
                    order_time=order_time,
                    logistic_type=logistic_type[:255],
                    import_date=import_date,
                )

                # ผูก product ถ้าตารางมีและเจอสินค้า (ใช้ cached data)
                if has_product_fk:
                    prod = product_map.get(sku)
                    if prod:
                        ol_kwargs["product_id"] = prod.id

                line = OrderLine(**ol_kwargs)
                new_orderlines.append(line)
                items_added_count += 1

            # นับยอด Added (เฉพาะถ้ายังไม่เคยนับ)
            if items_added_count > 0 and oid not in stats["added_ids"]:
                stats["added"] += 1
                stats["added_ids"].append(oid)

        except Exception as e:
            if oid not in stats["failed_ids"]:
                stats["failed"] += 1
                stats["failed_ids"].append(oid)
            if len(stats["errors"]) < 10:
                stats["errors"].append(f"Order {oid}: {str(e)}")

    # Batch insert OrderLines ทั้งหมด
    if new_orderlines:
        db.session.add_all(new_orderlines)

    db.session.commit()
    return stats


# ============================
# Price Marketing importers (Merged)
# ============================

def _to_float(x):
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x):
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    if x is None:
        return None
    if isinstance(x, bool):
        return int(x)
    if isinstance(x, (int,)):
        return int(x)
    if isinstance(x, float):
        try:
            return int(x)
        except Exception:
            return None
    s = str(x).strip().replace(",", "")
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _is_blank(x) -> bool:
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except Exception:
        pass
    try:
        return str(x).strip() == ""
    except Exception:
        return True


def _get_cell(row, col):
    try:
        return row.get(col)
    except Exception:
        try:
            return row[col]
        except Exception:
            return None


def _set_attr(obj, attr: str, col: str | None, row, *, kind: str):
    """Patch semantics:
    - col missing => do nothing
    - cell blank  => set None
    - cell value  => convert + set (only if changed)
    """
    if not col:
        return

    raw = _get_cell(row, col)
    if _is_blank(raw):
        new_val = None
    else:
        if kind == "str":
            new_val = str(raw).strip()
        elif kind == "float":
            new_val = _to_float(raw)
            if new_val is None:
                return  # non-blank but parse failed -> keep old
        elif kind == "int":
            new_val = _to_int(raw)
            if new_val is None:
                return
        elif kind == "bool":
            new_val = _to_bool(raw)
            if new_val is None:
                return
            new_val = bool(new_val)
        elif kind == "dt":
            dt = parse_datetime_guess(raw)
            if dt is None:
                return
            new_val = dt
        else:
            return

    cur = getattr(obj, attr, None)
    if cur != new_val:
        setattr(obj, attr, new_val)


def _to_bool(x):
    """Parse boolean-like values from spreadsheets (1/0, yes/no, true/false, mall)."""
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    if x is None:
        return None
    if isinstance(x, bool):
        return bool(x)
    s = str(x).strip().lower()
    if s in ("1", "true", "yes", "y", "mall", "official", "official store", "official_store"):
        return True
    if s in ("0", "false", "no", "n", "", "-"):
        return False
    return None


def _dump_dt(dt):
    return dt.isoformat() if dt else None


def _add_op(batch_id: int, seq: int, table: str, pk, action: str, before: dict | None):
    op = PriceImportOp(
        batch_id=batch_id,
        seq=seq,
        table_name=table,
        pk=str(pk),
        action=action,
        before_json=(json.dumps(before, ensure_ascii=False) if before is not None else None),
    )
    db.session.add(op)
    return seq + 1


def import_sku_pricing(df: pd.DataFrame, batch_id: int | None = None):
    """
    Import ข้อมูลฝั่งเรา (Internal) ต่อ SKU
    รองรับหัวคอลัมน์:
      sku, brand, name/model, spec/spec_text, cost, our_price, floor_price, min_margin_pct, pack_cost, ship_subsidy
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    sku_col = first_existing(df, ["sku", "SKU", "รหัสสินค้า"])
    brand_col = first_existing(df, ["brand", "Brand", "ยี่ห้อ", "แบรนด์"])
    name_col = first_existing(df, ["name", "Name", "model", "Model", "ชื่อสินค้า", "สินค้า", "Product"])
    spec_col = first_existing(df, ["spec", "Spec", "spec_text", "สเปค", "รายละเอียด"])

    # Stock import semantics (ยึดตามไฟล์ ไม่คำนวณบวกในระบบ)
    stock_internal_col = first_existing(
        df,
        [
            "Stock Internal",
            "stock_internal",
            "stock_internal_qty",
            "Stock_Internal",
            "สต๊อกฝั่งเรา",
            "สต๊อกเรา",
            "คงเหลือ(ฝั่งเรา)",
        ],
    )

    stock_total_col = first_existing(
        df,
        [
            "Stock",
            "stock",
            "stock_qty",  # legacy: single stock column
            "สต๊อกรวม",
            "คงเหลือรวม",
            "ยอดคงเหลือ",
            "จำนวนคงเหลือ",
        ],
    )

    # รองรับหัวแบบ Dashboard ด้วย (Cost / Our Price / Floor Price / Min Margin % / Pack Cost / Ship Subsidy)
    cost_col = first_existing(df, ["cost", "Cost", "ต้นทุน"])
    our_col = first_existing(df, ["our_price", "Our Price", "ราคาเรา", "ราคาขายเรา"])
    floor_col = first_existing(df, ["floor_price", "Floor Price", "ราคาต่ำสุด", "floor"])
    minm_col = first_existing(df, ["min_margin_pct", "Min Margin %", "กำไรขั้นต่ำ%", "min_margin"])
    pack_col = first_existing(df, ["pack_cost", "Pack Cost", "ค่าแพ็ค", "pack"])
    ship_col = first_existing(df, ["ship_subsidy", "Ship Subsidy", "ช่วยค่าส่ง", "ship"])

    ok = 0
    skip = 0
    new_products = 0

    seq = 1

    for _, r in df.iterrows():
        sku = (str(r.get(sku_col, "")).strip() if sku_col else "").strip()
        if not sku:
            skip += 1
            continue

        pr = SkuPricing.query.get(sku)
        if not pr:
            pr = SkuPricing(sku=sku)
            db.session.add(pr)
            if batch_id:
                seq = _add_op(batch_id, seq, "sku_pricing", sku, "insert", None)
        else:
            if batch_id:
                before = {
                    "brand": pr.brand,
                    "name": pr.name,
                    "spec_text": pr.spec_text,
                    "stock_qty": getattr(pr, "stock_qty", None),
                    "stock_internal_qty": getattr(pr, "stock_internal_qty", None),
                    "cost": pr.cost,
                    "our_price": pr.our_price,
                    "floor_price": pr.floor_price,
                    "min_margin_pct": pr.min_margin_pct,
                    "pack_cost": pr.pack_cost,
                    "ship_subsidy": pr.ship_subsidy,
                }
                seq = _add_op(batch_id, seq, "sku_pricing", sku, "update", before)

        # ✅ Patch by SKU (col missing => no touch, blank => clear)
        _set_attr(pr, "brand", brand_col, r, kind="str")
        _set_attr(pr, "name", name_col, r, kind="str")
        _set_attr(pr, "spec_text", spec_col, r, kind="str")

        # ===== Stock import ตาม Template (ไม่บวกในระบบ) =====
        # 1) Stock Internal
        if stock_internal_col:
            raw_i = _get_cell(r, stock_internal_col)
            if _is_blank(raw_i):
                pr.stock_internal_qty = None
            else:
                v = _to_int(raw_i)
                if v is not None:
                    pr.stock_internal_qty = int(v)

        # 2) Stock Total
        if stock_total_col:
            raw_t = _get_cell(r, stock_total_col)
            if _is_blank(raw_t):
                pr.stock_qty = None
            else:
                v = _to_int(raw_t)
                if v is not None:
                    pr.stock_qty = int(v)

        # 3) Backward compatible: if only total exists -> internal = total
        if (not stock_internal_col) and stock_total_col and (getattr(pr, "stock_qty", None) is not None):
            pr.stock_internal_qty = pr.stock_qty

        # 4) If only internal exists -> total = internal
        if stock_internal_col and (not stock_total_col) and (getattr(pr, "stock_internal_qty", None) is not None):
            pr.stock_qty = pr.stock_internal_qty

        _set_attr(pr, "cost", cost_col, r, kind="float")
        _set_attr(pr, "our_price", our_col, r, kind="float")
        _set_attr(pr, "floor_price", floor_col, r, kind="float")
        _set_attr(pr, "min_margin_pct", minm_col, r, kind="float")
        _set_attr(pr, "pack_cost", pack_col, r, kind="float")
        _set_attr(pr, "ship_subsidy", ship_col, r, kind="float")

        ok += 1

    db.session.commit()
    return {"ok": ok, "skip": skip, "new_products": new_products}


def import_monthly_sales(df: pd.DataFrame, batch_id: int | None = None):
    """Import Monthly Sales ต่อ SKU

    รองรับหัวคอลัมน์ (Template):
      - SKU
      - Quantity (หรือ ยอดขาย/จำนวน)

    Semantics:
      - รีเซ็ต monthly_sales_qty ของทุก SKU เป็น 0 ก่อนนำเข้า (กันค่าค้างเดือนก่อน)
      - แถวที่ไม่มี SKU จะถูก skip
      - Quantity ว่าง/อ่านไม่ได้ => 0
    """

    if df is None or df.empty:
        return {"ok": 0, "skip": 0}

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    sku_col = first_existing(df, ["sku", "SKU", "Sku", "รหัสสินค้า"])
    qty_col = first_existing(df, ["quantity", "Quantity", "qty", "Qty", "QTY", "จำนวน", "ยอดขาย", "Monthly Sales", "MonthlySales"])

    if not sku_col:
        raise ValueError("Missing column: SKU")
    if not qty_col:
        raise ValueError("Missing column: Quantity")

    ok = 0
    skip = 0
    seq = 1

    # Reset all to 0 first (monthly semantics)
    try:
        SkuPricing.query.update({SkuPricing.monthly_sales_qty: 0})
        db.session.flush()
    except Exception:
        pass

    for _, r in df.iterrows():
        sku = (str(r.get(sku_col, "") or "").strip())
        if not sku:
            skip += 1
            continue

        raw_qty = _get_cell(r, qty_col)
        qty = _to_int(raw_qty)
        if qty is None:
            qty = 0

        pr = SkuPricing.query.get(sku)
        if not pr:
            pr = SkuPricing(sku=sku)
            db.session.add(pr)
            if batch_id:
                seq = _add_op(batch_id, seq, "sku_pricing", sku, "insert", None)
        else:
            if batch_id:
                before = {
                    "monthly_sales_qty": getattr(pr, "monthly_sales_qty", None),
                }
                seq = _add_op(batch_id, seq, "sku_pricing", sku, "update", before)

        pr.monthly_sales_qty = int(qty)
        ok += 1

    return {"ok": ok, "skip": skip}


def import_market_prices(
    df: pd.DataFrame,
    default_platform: str | None = None,
    checked_by: str | None = None,
    batch_id: int | None = None,
):
    """
    Import ราคาตลาด (Market) + อัปเดต MarketItem.latest_* + บันทึกประวัติ MarketPriceLog

    รองรับคอลัมน์:
      sku, platform, shop_name, listed_price, shipping_fee, voucher_discount, coin_discount, net_price, url, captured_at, note

    - ถ้าไม่มี net_price จะคำนวณ: net = listed_price + shipping_fee - voucher_discount - coin_discount
    - platform ใช้ normalize_platform() ของโปรเจกต์: Shopee/Lazada/TikTok
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    sku_col = first_existing(df, ["sku", "SKU", "รหัสสินค้า"])
    plat_col = first_existing(df, ["platform", "Platform", "แพลตฟอร์ม"])

    # Dashboard: Shop
    shop_col = first_existing(df, ["shop_name", "competitor_shop", "shop", "Shop", "ชื่อร้าน", "ร้าน"])

    # optional/legacy
    listed_col = first_existing(df, ["listed_price", "Listed Price", "ราคา", "ราคาหน้าร้าน"])
    ship_col = first_existing(df, ["shipping_fee", "Shipping Fee", "ค่าส่ง", "shipping"])

    # Dashboard: Voucher
    vou_col = first_existing(df, ["voucher_discount", "Voucher", "คูปอง", "voucher"])
    coin_col = first_existing(df, ["coin_discount", "Coin Discount", "coin", "coins"])

    # Dashboard: Market (best) -> net_price
    net_col = first_existing(df, ["net_price", "Market (best)", "Market", "ราคาสุทธิ", "ราคาตลาด", "net"])

    # Dashboard: URL
    url_col = first_existing(df, ["url", "URL", "link", "ลิงก์"])
    mall_col = first_existing(df, ["is_mall", "mall", "Mall", "MALL", "isMall", "official", "official_store"])

    # Dashboard: Updated -> captured_at
    cap_col = first_existing(df, ["captured_at", "Updated", "update", "อัปเดต", "เวลาเก็บ", "วันที่เก็บ", "date"])
    note_col = first_existing(df, ["note", "Note", "หมายเหตุ"])

    ok = 0
    skip = 0
    new_products = 0
    new_items = 0

    allowed = {"Shopee", "Lazada", "TikTok"}

    seq = 1

    for _, r in df.iterrows():
        sku = (str(r.get(sku_col, "")).strip() if sku_col else "").strip()
        if not sku:
            skip += 1
            continue

        plat_raw = (r.get(plat_col) if plat_col else None) or default_platform or ""
        plat = normalize_platform(str(plat_raw).strip())
        if plat not in allowed:
            skip += 1
            continue

        shop = (str(r.get(shop_col, "")).strip() if shop_col else "").strip()
        if not shop:
            skip += 1
            continue

        # กันข้อมูล placeholder (shop_name == platform)
        if shop and plat and shop.strip().lower() == plat.strip().lower():
            skip += 1
            continue

        # Extract with patch semantics
        listed = None
        shipping = None
        voucher = None
        coin = None
        net = None

        if listed_col:
            raw = _get_cell(r, listed_col)
            listed = None if _is_blank(raw) else _to_float(raw)

        if ship_col:
            raw = _get_cell(r, ship_col)
            shipping = None if _is_blank(raw) else _to_float(raw)

        if vou_col:
            raw = _get_cell(r, vou_col)
            voucher = None if _is_blank(raw) else _to_float(raw)

        if coin_col:
            raw = _get_cell(r, coin_col)
            coin = None if _is_blank(raw) else _to_float(raw)

        if net_col:
            raw = _get_cell(r, net_col)
            net = None if _is_blank(raw) else _to_float(raw)

        # Derive net only when net column missing but listed exists
        if net is None and not net_col and listed is not None:
            net = (listed or 0.0) + float(shipping or 0.0) - float(voucher or 0.0) - float(coin or 0.0)

        # Must have net to log (MarketPriceLog.net_price is NOT NULL)
        if net is None:
            skip += 1
            continue

        url = None
        if url_col:
            raw = _get_cell(r, url_col)
            url = None if _is_blank(raw) else str(raw).strip()

        note = None
        if note_col:
            raw = _get_cell(r, note_col)
            note = None if _is_blank(raw) else str(raw).strip()

        is_mall = None
        if mall_col:
            raw = _get_cell(r, mall_col)
            if _is_blank(raw):
                is_mall = None
            else:
                b = _to_bool(raw)
                is_mall = (bool(b) if b is not None else None)

        captured_at = None
        cap_blank = False
        if cap_col:
            raw = _get_cell(r, cap_col)
            if _is_blank(raw):
                cap_blank = True
                captured_at = None
            else:
                captured_at = parse_datetime_guess(raw)

        # upsert MarketItem (ตาม SKU+platform+shop) + ซ่อม placeholder (shop_name==platform)
        item = MarketItem.query.filter_by(sku=sku, platform=plat, shop_name=shop).first()
        if not item:
            placeholder = MarketItem.query.filter_by(sku=sku, platform=plat, shop_name=plat).first()
            if placeholder is not None:
                placeholder.shop_name = shop
                item = placeholder
            else:
                item = MarketItem(sku=sku, platform=plat, shop_name=shop)
                db.session.add(item)
                new_items += 1

            if url_col:
                item.product_url = url

            db.session.flush()  # ให้ item.id พร้อมใช้
            if batch_id:
                seq = _add_op(batch_id, seq, "market_items", item.id, "insert", None)
        else:
            if batch_id:
                before = {
                    "sku": item.sku,
                    "platform": item.platform,
                    "shop_name": item.shop_name,
                    "product_url": item.product_url,
                    "is_mall": getattr(item, "is_mall", False),
                    "is_active": item.is_active,
                    "latest_listed_price": item.latest_listed_price,
                    "latest_shipping_fee": item.latest_shipping_fee,
                    "latest_voucher_discount": item.latest_voucher_discount,
                    "latest_coin_discount": item.latest_coin_discount,
                    "latest_net_price": item.latest_net_price,
                    "last_updated": _dump_dt(item.last_updated),
                    "note": item.note,
                }
                seq = _add_op(batch_id, seq, "market_items", item.id, "update", before)

        # Patch only when corresponding columns exist
        _set_attr(item, "latest_listed_price", listed_col, r, kind="float")
        _set_attr(item, "latest_shipping_fee", ship_col, r, kind="float")
        _set_attr(item, "latest_voucher_discount", vou_col, r, kind="float")
        _set_attr(item, "latest_coin_discount", coin_col, r, kind="float")

        # net_price: from column if exists, otherwise derived (listed-based) and treated as update
        if net_col:
            _set_attr(item, "latest_net_price", net_col, r, kind="float")
        elif listed_col:
            if item.latest_net_price != net:
                item.latest_net_price = net

        _set_attr(item, "product_url", url_col, r, kind="str")
        _set_attr(item, "note", note_col, r, kind="str")

        if mall_col:
            _set_attr(item, "is_mall", mall_col, r, kind="bool")

        # last_updated follows patch semantics
        if cap_col:
            if cap_blank:
                if item.last_updated is not None:
                    item.last_updated = None
            else:
                if captured_at is not None and item.last_updated != captured_at:
                    item.last_updated = captured_at

        # add log history
        log = MarketPriceLog(
            market_item_id=item.id,
            sku=sku,
            platform=plat,
            shop_name=shop,
            listed_price=listed,
            shipping_fee=shipping,
            voucher_discount=voucher,
            coin_discount=coin,
            net_price=net,
            captured_at=(captured_at if cap_col else now_thai()),
            checked_by=checked_by or "admin",
            note=note,
        )
        db.session.add(log)
        if batch_id:
            db.session.flush()  # ให้ log.id พร้อม
            seq = _add_op(batch_id, seq, "market_price_logs", log.id, "insert", None)

        ok += 1

    db.session.commit()
    return {"ok": ok, "skip": skip, "new_products": new_products, "new_items": new_items}


def import_brand_control(df: pd.DataFrame, batch_id: int | None = None):
    """Import ข้อมูล Brand Control (ราคาแบรนด์คุม)

    รองรับคอลัมน์: sku, brand, name, price control
    """

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    sku_col = first_existing(df, ["sku", "SKU", "รหัสสินค้า"])
    brand_col = first_existing(df, ["brand", "Brand", "แบรนด์", "ยี่ห้อ"])
    name_col = first_existing(df, ["name", "Name", "model", "Model", "ชื่อสินค้า", "สินค้า"])
    price_col = first_existing(
        df,
        [
            "price control",
            "price_control",
            "control_price",
            "brand_control",
            "Brand Control",
            "ราคาควบคุม",
            "ราคาแนะนำ",
            "MAP",
        ],
    )

    ok = 0
    skip = 0

    seq = 1

    for _, r in df.iterrows():
        sku = (str(r.get(sku_col, "")).strip() if sku_col else "").strip()
        if not sku:
            skip += 1
            continue

        price = _to_float(r.get(price_col)) if price_col else None

        bc = BrandControl.query.get(sku)
        if not bc:
            bc = BrandControl(sku=sku)
            db.session.add(bc)
            if batch_id:
                seq = _add_op(batch_id, seq, "brand_controls", sku, "insert", None)
        else:
            if batch_id:
                before = {
                    "brand": bc.brand,
                    "name": bc.name,
                    "price_control": bc.price_control,
                }
                seq = _add_op(batch_id, seq, "brand_controls", sku, "update", before)

        _set_attr(bc, "brand", brand_col, r, kind="str")
        _set_attr(bc, "name", name_col, r, kind="str")
        _set_attr(bc, "price_control", price_col, r, kind="float")

        ok += 1

    db.session.commit()
    return {"ok": ok, "skip": skip}


# ============================
# Supplier SKU + Stock Importer
# ============================


def _norm_key(s) -> str:
    if s is None:
        return ""
    s = str(s).strip().upper()
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "").replace("/", "").replace("#", "")
    return s


def _norm_supplier(s) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    return s.upper()


def _parse_stock_int(v) -> int:
    try:
        if pd.isna(v):
            return 0
    except Exception:
        pass
    if v is None:
        return 0
    s = str(v).strip()
    if not s or s in {"-", "—"}:
        return 0
    s = s.replace(",", "")
    if s.endswith("+"):
        s = s[:-1]
    try:
        return int(float(s))
    except Exception:
        return 0


def import_supplier_sku_stock(df: pd.DataFrame):
    """Import Supplier SKU + Stock

    Columns (template): SKU, SKU SUP, Supplier, Brand, Name, Stock
    Dedup rule: (Supplier + SKU SUP) duplicate -> Stock=MAX (do not sum)

    Notes:
    - Conflict rule: if same (Supplier, SKU SUP) maps to different SKU -> skip that key.
    """

    if df is None or df.empty:
        return {"ok": 0, "skip": 0, "insert": 0, "update": 0, "conflict": 0, "conflicts": []}

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    sku_col = first_existing(df, ["SKU", "sku", "รหัสสินค้า"])
    sku_sup_col = first_existing(df, ["SKU SUP", "SKU_SUP", "sku_sup", "Supplier SKU", "Article No."])
    supplier_col = first_existing(df, ["Supplier", "supplier", "ซัพ", "ผู้จำหน่าย", "Distributor"])
    brand_col = first_existing(df, ["Brand", "brand", "แบรนด์", "ยี่ห้อ"])
    name_col = first_existing(df, ["Name", "name", "ชื่อสินค้า", "Item Description", "Article Description"])
    stock_col = first_existing(
        df,
        [
            "Stock",
            "stock",
            "Stock Sup",
            "QTY",
            "Qty",
            "Available",
            "Available QTY",
            "จำนวน",
            "คงเหลือ",
        ],
    )

    if not sku_col:
        raise ValueError("Missing column: SKU")
    if not supplier_col:
        raise ValueError("Missing column: Supplier")
    if not sku_sup_col:
        raise ValueError("Missing column: SKU SUP")
    if not stock_col:
        raise ValueError("Missing column: Stock")

    df["_sku"] = df[sku_col].astype(str).fillna("").str.strip()
    df["_sku_norm"] = df["_sku"].apply(_norm_key)

    df["_supplier"] = df[supplier_col].astype(str).fillna("").str.strip()
    df["_supplier_norm"] = df["_supplier"].apply(_norm_supplier)

    df["_sku_sup"] = df[sku_sup_col].astype(str).fillna("").str.strip()
    df["_sku_sup_norm"] = df["_sku_sup"].apply(_norm_key)

    if brand_col:
        df["_brand"] = df[brand_col].astype(str).fillna("").str.strip()
    else:
        df["_brand"] = ""

    if name_col:
        df["_name"] = df[name_col].astype(str).fillna("").str.strip()
    else:
        df["_name"] = ""

    df["_stock"] = df[stock_col].apply(_parse_stock_int)

    # require keys
    df = df[(df["_sku_norm"] != "") & (df["_supplier_norm"] != "") & (df["_sku_sup_norm"] != "")]
    if df.empty:
        return {"ok": 0, "skip": 0, "insert": 0, "update": 0, "conflict": 0, "conflicts": []}

    # Dedup key=(supplier_norm, sku_sup_norm)
    bucket: dict[tuple[str, str], dict] = {}
    conflict_keys: set[tuple[str, str]] = set()
    conflict_rows: list[dict] = []

    for _, r in df.iterrows():
        key = (r["_supplier_norm"], r["_sku_sup_norm"])
        if key not in bucket:
            bucket[key] = {
                "sku": r["_sku"],
                "sku_norm": r["_sku_norm"],
                "supplier": r["_supplier"],
                "supplier_norm": r["_supplier_norm"],
                "sku_sup": r["_sku_sup"],
                "sku_sup_norm": r["_sku_sup_norm"],
                "brand": r["_brand"],
                "name": r["_name"],
                "stock": int(r["_stock"] or 0),
            }
            continue

        prev = bucket[key]
        if prev["sku_norm"] != r["_sku_norm"]:
            conflict_keys.add(key)
            conflict_rows.append(
                {
                    "Supplier": r["_supplier"],
                    "SKU SUP": r["_sku_sup"],
                    "SKU(เดิม)": prev["sku"],
                    "SKU(ใหม่)": r["_sku"],
                }
            )
            continue

        # do NOT sum duplicates -> use MAX
        prev["stock"] = max(int(prev.get("stock") or 0), int(r["_stock"] or 0))
        if (not prev.get("brand")) and r["_brand"]:
            prev["brand"] = r["_brand"]
        if (not prev.get("name")) and r["_name"]:
            prev["name"] = r["_name"]

    for k in conflict_keys:
        bucket.pop(k, None)

    ok = 0
    skip = 0
    ins = 0
    upd = 0
    now = now_thai()

    for rec in bucket.values():
        row = SupplierSkuMaster.query.filter_by(
            supplier_norm=rec["supplier_norm"],
            sku_sup_norm=rec["sku_sup_norm"],
        ).first()

        # If the same (supplier, sku_sup) maps to a different SKU, skip this key (do not overwrite)
        if row and row.sku_norm != rec["sku_norm"]:
            skip += 1
            continue

        if not row:
            row = SupplierSkuMaster(
                sku=rec["sku"],
                sku_norm=rec["sku_norm"],
                supplier=rec["supplier"],
                supplier_norm=rec["supplier_norm"],
                sku_sup=rec["sku_sup"],
                sku_sup_norm=rec["sku_sup_norm"],
                brand=(rec.get("brand") or None),
                name=(rec.get("name") or None),
                stock_sup_qty=int(rec.get("stock") or 0),
                stock_updated_at=now,
            )
            db.session.add(row)
            ins += 1
            ok += 1
        else:
            row.sku = rec["sku"]
            row.sku_norm = rec["sku_norm"]
            row.supplier = rec["supplier"]
            row.supplier_norm = rec["supplier_norm"]
            row.sku_sup = rec["sku_sup"]
            row.sku_sup_norm = rec["sku_sup_norm"]
            if rec.get("brand"):
                row.brand = rec["brand"]
            if rec.get("name"):
                row.name = rec["name"]
            row.stock_sup_qty = int(rec.get("stock") or 0)
            row.stock_updated_at = now
            upd += 1
            ok += 1

    try:
        db.session.commit()
    except IntegrityError:
        # If unique constraints are violated (rare edge cases), roll back and re-raise.
        db.session.rollback()
        raise

    return {
        "ok": ok,
        "skip": skip,
        "insert": ins,
        "update": upd,
        "conflict": len(conflict_keys),
        "conflicts": conflict_rows,
    }