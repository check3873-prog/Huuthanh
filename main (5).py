import os
import sqlite3
import random
import string
import asyncio
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from datetime import datetime, date
from typing import List, Tuple

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
import qrcode

# ================== CẤU HÌNH ==================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "PUT-YOUR-TOKEN-HERE")
ADMIN_ID = 6194220235

MIN_BET = 2_000
MAX_BET = 20_000_000

MIN_DEPOSIT = 50_000
MIN_WITHDRAW = 100_000

START_BALANCE = 2_000

BASE_DIR = os.path.dirname(__file__)
PLAYERS_DB_FILE = os.path.join(BASE_DIR, "players.db")
BETS_DB_FILE = os.path.join(BASE_DIR, "bets.db")
FINANCE_DB_FILE = os.path.join(BASE_DIR, "finance.db")

PLAYERS_CONN = None
BETS_CONN = None
FINANCE_CONN = None

# Tỷ lệ trả thưởng (tổng tiền nhận, gồm vốn)
TAIXIU_MULTIPLIER_TOTAL = Decimal("1.95")
XX_GROUP_MULTIPLIER_TOTAL = Decimal("1.95")
XX_SINGLE_MULTIPLIER_TOTAL = Decimal("5.0")
BOWLING_MULTIPLIER_TOTAL = Decimal("1.95")
BASKETBALL_MULTIPLIER_TOTAL = Decimal("2.0")

# VIP
VIP_LEVELS = [
    (1, 0, "🦐 (Tôm)"),
    (2, 10, "🦞 (Tôm hùm)"),
    (3, 50, "🦑 (Mực)"),
    (4, 100, "🦀 (Cua)"),
    (5, 500, "🐙 (Bạch tuộc)"),
    (6, 1000, "🐠 (Cá ngừ)"),
    (7, 5000, "🐬 (Cá heo)"),
    (8, 15000, "🦈 (Cá mập)"),
    (9, 50000, "🐳 (Cá voi)"),
]

VIP_EXCHANGE_RATE = {
    1: 100,
    2: 200,
    3: 300,
    4: 400,
    5: 500,
    6: 600,
    7: 700,
    8: 800,
    9: 1000,
}

BANK_ACCOUNTS = [
    {
        "bank": "VIB",
        "account": "086909549",
        "owner": "NGUYEN VAN LUC",
        "code": "VIB",
    },
]

BANK_CODE_MAP = {
    "ACB": "ACB - NH TMCP A CHAU",
    "BIDV": "BIDV - NH DAU TU VA PHAT TRIEN VIET NAM",
    "MBB": "MB - NH TMCP QUAN DOI",
    "MSB": "MSB - NH TMCP HANG HAI",
    "TCB": "TECHCOMBANK - NH TMCP KY THUONG VIET NAM",
    "TPB": "TPBANK - NH TMCP TIEN PHONG",
    "VCB": "VIETCOMBANK - NH TMCP NGOAI THUONG VIET NAM",
    "VIB": "VIB - NH TMCP QUOC TE VIET NAM",
    "VPB": "VPBANK - NH TMCP VIET NAM THINH VUONG",
    "VTB": "VIETINBANK - NH TMCP CONG THUONG VIET NAM",
    "SHIB": "SHINHANBANK - NH TNHH SHINHAN VIET NAM",
    "ABB": "ABBANK - NH TMCP AN BINH",
    "AGR": "AGRIBANK - NH NN & PTNT VIET NAM",
    "VCCB": "BANVIET - NH TMCP BAN VIET",
    "BVB": "BAOVIETBANK - NH TMCP BAO VIET (BVB)",
    "DAB": "DONGABANK - NH TMCP DONG A",
    "EIB": "EXIMBANK - NH TMCP XUAT NHAP KHAU VIET NAM",
    "GPB": "GPBANK - NH TMCP DAU KHI TOAN CAU",
    "HDB": "HDBANK - NH TMCP PHAT TRIEN TP.HCM",
    "KLB": "KIENLONGBANK - NH TMCP KIEN LONG",
    "NAB": "NAMABANK - NH TMCP NAM A",
    "NCB": "NCB - NH TMCP QUOC DAN",
    "OCB": "OCB - NH TMCP PHUONG DONG",
    "OJB": "OCEANBANK - NH TMCP DAI DUONG (OJB)",
    "PGB": "PGBANK - NH TMCP XANG DAU PETROLIMEX",
    "PVB": "PVCOMBANK - NH TMCP DAI CHUNG VIET NAM",
    "STB": "SACOMBANK - NH TMCP SAI GON THUONG TIN",
    "SGB": "SAIGONBANK - NH TMCP SAI GON CONG THUONG",
    "SCB": "SCB - NH TMCP SAI GON",
    "SAB": "SEABANK - NH TMCP DONG NAM A",
    "SHB": "SHB - NH TMCP SAI GON HA NOI",
}
# Bảng khuyến mãi nạp đầu (K = 1.000đ)
FIRST_DEPOSIT_PROMO_TABLE = {
    100_000: 88_000,
    200_000: 188_000,
    500_000: 228_000,
    1_000_000: 288_000,
    3_000_000: 388_000,
    5_000_000: 488_000,
    10_000_000: 888_000,
    20_000_000: 1_888_000,
    50_000_000: 3_888_000,
    100_000_000: 8_888_000,
}

# BIN VietQR / NAPAS cho các ngân hàng Việt Nam
BANK_BIN_MAP = {
    # Quốc doanh
    "VCB": "970436",   # Vietcombank
    "BIDV": "970418",  # BIDV
    "VTB": "970415",   # VietinBank
    "AGR": "970405",   # Agribank

    # Cổ phần lớn
    "TCB": "970407",   # Techcombank
    "ACB": "970416",   # ACB
    "MBB": "970422",   # MB Bank
    "VPB": "970432",   # VPBank
    "TPB": "970423",   # TPBank
    "SHB": "970443",   # SHB
    "VIB": "970441",   # VIB
    "OCB": "970448",   # OCB
    "MSB": "970426",   # MSB
    "SCB": "970429",   # SCB
    "PVCB": "970412",  # PVcomBank
    "HDB": "970437",   # HDBank
    "SEAB": "970440",  # SeABank
    "ABB": "970425",   # ABBank
    "BAOVIET": "970438",  # BaoVietBank
    "NAMABANK": "970428", # Nam A Bank
    "KIENLONGBANK": "970452", # KienLongBank
    "VIETBANK": "970427",    # VietBank
    "SAIGONBANK": "970400",  # SaigonBank
    "BVB": "970454",         # BanVietBank
    "NCB": "970419",         # NCB

    # Một số ngân hàng khác
    "UOB": "970458",
    "CIMB": "970452",
}

NEWBIE_CODE = "EKKNJXIWW"
NEWBIE_CODE_VALUE = 79_000
REQUIRE_DEPOSIT_FOR_NEWBIE = 79_000


# BXH ảo
FAKE_DAILY_DATE = None
FAKE_DAILY_PLAYERS: List[dict] = []
FAKE_DAILY_LAST_UPDATE = None
FAKE_WEEK_KEY = None
FAKE_WEEKLY_PLAYERS: List[dict] = []
FAKE_WEEK_LAST_UPDATE = None


# ================== DB ==================


def get_players_db():
    global PLAYERS_CONN
    if PLAYERS_CONN is None:
        PLAYERS_CONN = sqlite3.connect(PLAYERS_DB_FILE, check_same_thread=False)
        PLAYERS_CONN.row_factory = sqlite3.Row
    return PLAYERS_CONN


def get_bets_db():
    global BETS_CONN
    if BETS_CONN is None:
        BETS_CONN = sqlite3.connect(BETS_DB_FILE, check_same_thread=False)
        BETS_CONN.row_factory = sqlite3.Row
    return BETS_CONN


def get_finance_db():
    global FINANCE_CONN
    if FINANCE_CONN is None:
        FINANCE_CONN = sqlite3.connect(FINANCE_DB_FILE, check_same_thread=False)
        FINANCE_CONN.row_factory = sqlite3.Row
    return FINANCE_CONN


def init_db():
    # DB người chơi
    db_p = get_players_db()
    db_p.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance INTEGER NOT NULL DEFAULT 0,
            vip_points_earned REAL NOT NULL DEFAULT 0,
            vip_points_spent REAL NOT NULL DEFAULT 0,
            wager_required INTEGER NOT NULL DEFAULT 0,
            wager_done INTEGER NOT NULL DEFAULT 0,
            referrer_id INTEGER,
            total_deposit INTEGER NOT NULL DEFAULT 0,
            last_bet_json TEXT,
            pending_withdraw_json TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS disciples (
            referrer_id INTEGER,
            disciple_id INTEGER,
            PRIMARY KEY (referrer_id, disciple_id)
        );

        CREATE TABLE IF NOT EXISTS giftcodes (
            code TEXT PRIMARY KEY,
            amount INTEGER NOT NULL,
            used INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    
    # Bổ sung các cột phục vụ khuyến mãi nếu chưa tồn tại
    try:
        db_p.execute(
            "ALTER TABLE users ADD COLUMN first_deposit_bonus_used INTEGER NOT NULL DEFAULT 0"
        )
    except Exception:
        pass

    try:
        db_p.execute(
            "ALTER TABLE users ADD COLUMN first_deposit_bonus_amount INTEGER NOT NULL DEFAULT 0"
        )
    except Exception:
        pass

    try:
        db_p.execute(
            "ALTER TABLE users ADD COLUMN used_newbie_code INTEGER NOT NULL DEFAULT 0"
        )
    except Exception:
        pass

    try:
        db_p.execute(
            "ALTER TABLE users ADD COLUMN newbie_code TEXT"
        )
    except Exception:
        pass

    try:
        db_p.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_newbie_code ON users(newbie_code)"
        )
    except Exception:
        pass

    db_p.commit()

    # DB cược
    db_b = get_bets_db()
    db_b.executescript(
        """
        CREATE TABLE IF NOT EXISTS bets_daily (
            user_id INTEGER,
            day TEXT,
            count INTEGER NOT NULL,
            total INTEGER NOT NULL,
            PRIMARY KEY (user_id, day)
        );

        CREATE TABLE IF NOT EXISTS commissions_daily (
            user_id INTEGER,
            day TEXT,
            amount INTEGER NOT NULL,
            PRIMARY KEY (user_id, day)
        );
        """
    )
    db_b.commit()

    # DB nạp/rút
    db_f = get_finance_db()
    db_f.executescript(
        """
        CREATE TABLE IF NOT EXISTS deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            bank TEXT NOT NULL,
            account TEXT NOT NULL,
            owner TEXT NOT NULL,
            code TEXT NOT NULL,
            status TEXT NOT NULL,
            time TEXT NOT NULL,
            display_id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            bank_code TEXT NOT NULL,
            bank_full TEXT NOT NULL,
            account_no TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            time TEXT NOT NULL
        );
        """
    )
    db_f.commit()


# ================== HÀM PHỤ ==================


def format_currency(amount: int) -> str:
    return f"{amount:,.0f} ₫".replace(",", ".")


def decimal_payout(bet: int, multiplier: Decimal) -> int:
    value = (Decimal(bet) * multiplier).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(value)


def today_str() -> str:
    return date.today().isoformat()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def mask_id(num: int) -> str:
    s = str(num)
    if len(s) <= 4:
        return "*" * len(s)
    return s[:3] + "****" + s[-3:]


def random_message_id() -> int:
    return random.randint(1_000_000, 9_999_999)


def random_deposit_code(length: int = 10) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def random_display_id() -> int:
    return random.randint(100_000_000, 999_999_999_999)



def _tlv(tag: str, value: str) -> str:
    """Tạo cặp TLV theo chuẩn EMVCo: TAG(2) + LEN(2) + VALUE."""
    length = len(value)
    return f"{tag}{length:02d}{value}"


def _crc16_ccitt(data: bytes) -> str:
    """CRC16-CCITT (0x1021), initial 0xFFFF, output 4 hex chữ hoa."""
    crc = 0xFFFF
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return f"{crc:04X}"


def remove_vietnamese_accents(text: str) -> str:
    """Loại bỏ dấu tiếng Việt để tương thích với VietQR."""
    import unicodedata
    text = unicodedata.normalize('NFD', text)
    result = []
    for char in text:
        if unicodedata.category(char) != 'Mn':  # Mn = Mark, Nonspacing
            result.append(char)
    # Xử lý đ/Đ riêng
    text = ''.join(result)
    text = text.replace('đ', 'd').replace('Đ', 'D')
    # Chỉ giữ ký tự ASCII
    return ''.join(c for c in text if ord(c) < 128)


def make_vietqr_payload(bank_code: str, account: str, owner: str, amount: int, add_info: str) -> str:
    """Tạo payload VietQR/NAPAS theo chuẩn EMVCo chính thức để app ngân hàng VN quét được."""
    bin_code = BANK_BIN_MAP.get(bank_code)
    if not bin_code:
        return f"{bank_code}|{account}|{owner}|{amount}|{add_info}"

    add_info_clean = remove_vietnamese_accents(add_info or "").upper()[:25]
    
    merchant_info = (
        _tlv("00", "A000000727") +
        _tlv("01", bin_code) +
        _tlv("02", account)
    )
    
    additional_data = _tlv("08", add_info_clean)

    payload_wo_crc = (
        _tlv("00", "01") +
        _tlv("01", "12") +
        _tlv("38", merchant_info) +
        _tlv("52", "5999") +
        _tlv("53", "704") +
        _tlv("54", str(amount)) +
        _tlv("58", "VN") +
        _tlv("62", additional_data)
    )

    to_crc = (payload_wo_crc + "6304").encode("ascii")
    crc = _crc16_ccitt(to_crc)
    return payload_wo_crc + "6304" + crc


def make_vietqr_url(bank_code: str, account: str, amount: int, add_info: str) -> str:
    """Tạo URL VietQR API để lấy ảnh QR chuẩn từ server VietQR chính thức."""
    bin_code = BANK_BIN_MAP.get(bank_code)
    if not bin_code:
        return None
    
    add_info_clean = remove_vietnamese_accents(add_info or "").upper()[:25]
    import urllib.parse
    description = urllib.parse.quote(add_info_clean)
    
    url = f"https://img.vietqr.io/image/{bin_code}-{account}-compact.png?amount={amount}&addInfo={description}"
    return url



def generate_qr_image(bank_code: str, account: str, owner: str, amount: int, code: str) -> BytesIO:
    """Tạo ảnh QR nạp tiền chuẩn VietQR/NAPAS - dùng API VietQR chính thức."""
    import urllib.request
    import urllib.parse
    
    bin_code = BANK_BIN_MAP.get(bank_code)
    if bin_code:
        add_info_clean = remove_vietnamese_accents(code or "").upper()[:25]
        description = urllib.parse.quote(add_info_clean)
        account_clean = account.replace(" ", "").replace("-", "")
        
        url = f"https://img.vietqr.io/image/{bin_code}-{account_clean}-compact.png?amount={amount}&addInfo={description}&accountName={urllib.parse.quote(remove_vietnamese_accents(owner or '').upper())}"
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as response:
                img_data = response.read()
                bio = BytesIO(img_data)
                bio.name = "deposit_qr.png"
                bio.seek(0)
                return bio
        except Exception:
            pass
    
    payload = make_vietqr_payload(bank_code, account, owner, amount, code)
    qr = qrcode.QRCode(version=None, box_size=10, border=4, error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(payload)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = BytesIO()
    bio.name = "deposit_qr.png"
    img.save(bio, "PNG")
    bio.seek(0)
    return bio



# ================== USER / DB LOGIC ==================


def generate_newbie_code() -> str:
    """Tạo code tân thủ random 9 ký tự."""
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choice(chars) for _ in range(9))


def generate_unique_newbie_code() -> str:
    """Tạo code tân thủ unique, kiểm tra không trùng trong DB."""
    db = get_players_db()
    for _ in range(100):
        code = generate_newbie_code()
        cur = db.execute("SELECT 1 FROM users WHERE newbie_code = ?", (code,))
        if cur.fetchone() is None:
            return code
    return generate_newbie_code()


def ensure_user(user_id: int, username: str | None) -> sqlite3.Row:
    db = get_players_db()
    cur = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row is None:
        for _ in range(10):
            try:
                newbie_code = generate_unique_newbie_code()
                db.execute(
                    "INSERT INTO users (user_id, username, balance, created_at, newbie_code) VALUES (?, ?, ?, ?, ?)",
                    (user_id, username, START_BALANCE, datetime.now().isoformat(), newbie_code),
                )
                db.commit()
                break
            except sqlite3.IntegrityError:
                continue
        cur = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
    elif row["newbie_code"] is None:
        for _ in range(10):
            try:
                newbie_code = generate_unique_newbie_code()
                db.execute("UPDATE users SET newbie_code = ? WHERE user_id = ?", (newbie_code, user_id))
                db.commit()
                break
            except sqlite3.IntegrityError:
                continue
        cur = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
    return row


def get_user(user_id: int) -> sqlite3.Row | None:
    db = get_players_db()
    cur = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    return cur.fetchone()


def change_balance(user_id: int, delta: int):
    db = get_players_db()
    db.execute(
        "UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?",
        (delta, user_id),
    )
    db.commit()


def add_vip_points_and_wager(user_id: int, bet_amount: int):
    db_p = get_players_db()
    db_b = get_bets_db()
    pts = bet_amount / 300_000.0
    today = today_str()

    # VIP & vòng cược
    db_p.execute(
        "UPDATE users SET vip_points_earned = vip_points_earned + ?, "
        "wager_done = wager_done + ? WHERE user_id = ?",
        (pts, bet_amount, user_id),
    )

    # Thống kê cược ngày
    cur = db_b.execute(
        "SELECT count, total FROM bets_daily WHERE user_id = ? AND day = ?",
        (user_id, today),
    )
    row = cur.fetchone()
    if row:
        db_b.execute(
            "UPDATE bets_daily SET count = ?, total = ? WHERE user_id = ? AND day = ?",
            (row["count"] + 1, row["total"] + bet_amount, user_id, today),
        )
    else:
        db_b.execute(
            "INSERT INTO bets_daily (user_id, day, count, total) VALUES (?, ?, ?, ?)",
            (user_id, today, 1, bet_amount),
        )

    # Hoa hồng ref
    cur = db_p.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
    ref_row = cur.fetchone()
    if ref_row and ref_row["referrer_id"]:
        ref_id = ref_row["referrer_id"]
        commission = int(bet_amount * 0.01)
        if commission > 0:
            change_balance(ref_id, commission)
            cur2 = db_b.execute(
                "SELECT amount FROM commissions_daily WHERE user_id = ? AND day = ?",
                (ref_id, today),
            )
            r2 = cur2.fetchone()
            if r2:
                db_b.execute(
                    "UPDATE commissions_daily SET amount = ? WHERE user_id = ? AND day = ?",
                    (r2["amount"] + commission, ref_id, today),
                )
            else:
                db_b.execute(
                    "INSERT INTO commissions_daily (user_id, day, amount) VALUES (?, ?, ?)",
                    (ref_id, today, commission),
                )

    db_p.commit()
    db_b.commit()


def get_today_bet_stats(user_id: int) -> Tuple[int, int]:
    db = get_bets_db()
    cur = db.execute(
        "SELECT count, total FROM bets_daily WHERE user_id = ? AND day = ?",
        (user_id, today_str()),
    )
    row = cur.fetchone()
    if row:
        return row["count"], row["total"]
    return 0, 0


def sum_week_bets(user_id: int) -> int:
    db = get_bets_db()
    cur = db.execute(
        "SELECT day, total FROM bets_daily WHERE user_id = ?",
        (user_id,),
    )
    total = 0
    today = date.today()
    iso_today = today.isocalendar()
    for row in cur.fetchall():
        d = date.fromisoformat(row["day"])
        if d.isocalendar()[:2] == iso_today[:2]:
            total += row["total"]
    return total


def sum_month_bets(user_id: int) -> int:
    db = get_bets_db()
    cur = db.execute(
        "SELECT day, total FROM bets_daily WHERE user_id = ?",
        (user_id,),
    )
    total = 0
    today = date.today()
    for row in cur.fetchall():
        d = date.fromisoformat(row["day"])
        if d.year == today.year and d.month == today.month:
            total += row["total"]
    return total


def sum_commission_period(user_id: int, period: str) -> int:
    db = get_bets_db()
    cur = db.execute(
        "SELECT day, amount FROM commissions_daily WHERE user_id = ?",
        (user_id,),
    )
    today = date.today()
    iso_today = today.isocalendar()
    total = 0
    for row in cur.fetchall():
        d = date.fromisoformat(row["day"])
        if period == "day" and d == today:
            total += row["amount"]
        elif period == "week" and d.isocalendar()[:2] == iso_today[:2]:
            total += row["amount"]
        elif period == "month" and d.year == today.year and d.month == today.month:
            total += row["amount"]
    return total


def get_vip_total_and_spent(user_id: int) -> Tuple[int, int]:
    db = get_players_db()
    cur = db.execute(
        "SELECT vip_points_earned, vip_points_spent FROM users WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        return 0, 0
    total = int(row["vip_points_earned"])
    spent = int(row["vip_points_spent"])
    return total, spent


def get_available_vip_points(user_id: int) -> int:
    total, spent = get_vip_total_and_spent(user_id)
    avail = total - spent
    if avail < 0:
        avail = 0
    return avail


def get_vip_level_and_symbol(points_int: int):
    level = 1
    symbol = VIP_LEVELS[0][2]
    for lvl, req, sym in VIP_LEVELS:
        if points_int >= req:
            level, symbol = lvl, sym
    next_req = None
    for lvl, req, sym in VIP_LEVELS:
        if lvl == level + 1:
            next_req = req
            break
    return level, symbol, next_req


def calculate_vip_exchange_rate(points_int: int) -> int:
    level, _, _ = get_vip_level_and_symbol(points_int)
    return VIP_EXCHANGE_RATE.get(level, 100)


def get_total_deposit(user_id: int) -> int:
    db = get_players_db()
    cur = db.execute(
        "SELECT total_deposit FROM users WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    return row["total_deposit"] if row else 0


# ================== MENU CHÍNH ==================


def build_main_menu_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton("🎲 Danh sách Game"), KeyboardButton("👤 Tài khoản")],
        [KeyboardButton("🥇 Bảng xếp hạng")],
        [KeyboardButton("👥 Giới thiệu bạn bè"), KeyboardButton("💵 Hoa hồng")],
        [KeyboardButton("🎁 Khuyến mãi game")],
        [KeyboardButton("Trung tâm hỗ trợ")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# ================== /START & REF ==================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or user.first_name)

    # Ref link
    if context.args:
        ref_str = context.args[0]
        if ref_str.isdigit():
            ref_id = int(ref_str)
            if ref_id != user.id:
                db_p = get_players_db()
                cur = db_p.execute(
                    "SELECT referrer_id FROM users WHERE user_id = ?", (user.id,)
                )
                row = cur.fetchone()
                if row and row["referrer_id"] is None:
                    db_p.execute(
                        "UPDATE users SET referrer_id = ? WHERE user_id = ?",
                        (ref_id, user.id),
                    )
                    cur2 = db_p.execute(
                        "INSERT OR IGNORE INTO disciples (referrer_id, disciple_id) VALUES (?, ?)",
                        (ref_id, user.id),
                    )
                    db_p.commit()
                    if cur2.rowcount > 0:
                        change_balance(ref_id, 2_000)
                        try:
                            await context.bot.send_message(
                                chat_id=ref_id,
                                text=(
                                    "🎉 Bạn vừa nhận 2.000đ thưởng giới thiệu!\n"
                                    f"Người chơi mới: ID {user.id}"
                                ),
                            )
                        except Exception:
                            pass

    text = (
        "Trải nghiệm phong cách chơi mới tại Game Tele\n"
        "🎮 Chơi game trực tiếp trên bot không cần tải app – Nhanh, tiện, cực dễ chơi!\n"
        f"💵 Số dư khởi đầu cho người chơi mới: {format_currency(START_BALANCE)}"
    )
    keyboard = [[InlineKeyboardButton("🎮 Chiến thôi!!!", callback_data="start_playing")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_start_playing_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        return
    text = (
        "🎁 Game Tele 100% uy tín sử dụng emoji của nên tảng Telegram để làm kết quả chơi không thể can thiệp\n"
        "📞 Hỗ trợ khách hàng 24/7:\n"
        "👉 Telegram: @jennybotforex"
    )
    await query.message.reply_text(text, reply_markup=build_main_menu_keyboard())


# ================== TÀI KHOẢN ==================


def build_account_inline_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("💸 Nạp tiền", callback_data="acc_deposit")],
        [InlineKeyboardButton("💸 Rút tiền", callback_data="acc_withdraw")],
        [
            InlineKeyboardButton("📈 Lịch sử nạp", callback_data="acc_deposit_history"),
            InlineKeyboardButton("📉 Lịch sử rút", callback_data="acc_withdraw_history"),
        ],
        [InlineKeyboardButton("📄 Đổi điểm Vip", callback_data="acc_vip_exchange")],
        [InlineKeyboardButton("🎁 Nhập Giftcode", callback_data="acc_giftcode_enter")],
        [InlineKeyboardButton("📄 Tóm tắt lịch sử cược", callback_data="acc_bet_summary")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def show_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    row = ensure_user(user.id, user.username or user.first_name)
    balance = row["balance"]
    vip_total_int, vip_spent_int = get_vip_total_and_spent(user.id)
    vip_level, vip_symbol, next_req = get_vip_level_and_symbol(vip_total_int)
    progress_text = f"{vip_total_int}/{next_req}" if next_req else f"{vip_total_int}/MAX"

    text = (
        f"👤 Tên tài khoản:  {user.username or user.first_name}\n"
        f"🧾 ID Tài khoản: {user.id}\n"
        f"💰 Số dư: {format_currency(balance)}\n"
        f"👑 Cấp Vip: {vip_level} {vip_symbol}\n"
        f"💎 Số điểm Vip: {vip_total_int}\n"
        f"🚀 Tiến trình điểm vip: {progress_text}\n"
        f"✋ Số điểm vip đã sử dụng: {vip_spent_int}"
    )
    await update.message.reply_text(text, reply_markup=build_account_inline_keyboard())


def build_quick_deposit_keyboard() -> InlineKeyboardMarkup:
    amounts = [
        50_000,
        100_000,
        200_000,
        500_000,
        1_000_000,
        2_000_000,
        5_000_000,
        10_000_000,
        20_000_000,
        50_000_000,
    ]
    rows = []
    row = []
    for a in amounts:
        label = f"{a:,.0f} đ".replace(",", ".")
        row.append(InlineKeyboardButton(label, callback_data=f"quick_deposit:{a}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


async def show_deposit_menu(query, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "💸 Nạp tiền qua Chuyển khoản Ngân hàng\n\n"
        "🔷 Cách lấy thông tin nạp:\n"
        "🔸 Gõ lệnh: /napbank số tiền\n"
        "Ví dụ: /napbank 100000\n\n"
        "🔸 Hoặc bấm nút số tiền bên dưới để lấy nhanh.\n\n"
        "⚠️ Lưu ý:\n"
        "✅ Chuyển đúng SỐ TIỀN và NỘI DUNG được cung cấp.\n"
        "✅ Mỗi lần nạp cần lấy thông tin MỚI.\n"
        "🚫 Không dùng thông tin cũ cho giao dịch sau.\n\n"
        f"💰 Nạp tối thiểu: {format_currency(MIN_DEPOSIT)}"
    )
    await query.message.reply_text(text, reply_markup=build_quick_deposit_keyboard())


async def handle_account_callbacks(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: str
):
    query = update.callback_query
    user = query.from_user
    ensure_user(user.id, user.username or user.first_name)
    db_f = get_finance_db()
    await query.answer()

    if data == "acc_deposit":
        await show_deposit_menu(query, context)
        return

    if data == "acc_withdraw":
        text = (
            "🏧 Vui lòng thực hiện theo hướng dẫn sau:\n\n"
            "👉 /rutbank [dấu cách] Số tiền muốn rút [dấu cách]  Mã ngân hàng [dấu cách] "
            "Số tài khoản [dấu cách] Tên chủ tài khoản\n"
            "👉 VD:  Muốn rút 100k đến TK số 01234567890 tại Ngân hàng Vietcombank. Thực hiện theo cú pháp sau:\n\n"
            "/rutbank 100000 VCB 01234567890 NGUYEN VAN A\n\n"
            "⚠️ Lưu ý: Không hỗ trợ hoàn tiền nếu bạn nhập sai thông tin Tài khoản.\n"
            f"👉 Rút tối thiểu {format_currency(MIN_WITHDRAW)}\n\n"
            "MÃ NGÂN HÀNG - TÊN NGÂN HÀNG\n\n"
            "📌 ACB ==> ACB - NH TMCP A CHAU\n"
            "📌 BIDV ==> BIDV - NH DAU TU VA PHAT TRIEN VIET NAM\n"
            "📌 MBB ==> MB - NH TMCP QUAN DOI\n"
            "📌 MSB ==> MSB - NH TMCP HANG HAI\n"
            "📌 TCB ==> TECHCOMBANK - NH TMCP KY THUONG VIET NAM\n"
            "📌 TPB ==> TPBANK - NH TMCP TIEN PHONG\n"
            "📌 VCB ==> VIETCOMBANK - NH TMCP NGOAI THUONG VIET NAM\n"
            "📌 VIB ==> VIB - NH TMCP QUOC TE VIET NAM\n"
            "📌 VPB ==> VPBANK - NH TMCP VIET NAM THINH VUONG\n"
            "📌 VTB ==> VIETINBANK - NH TMCP CONG THUONG VIET NAM\n"
            "📌 SHIB ==> SHINHANBANK - NH TNHH SHINHAN VIET NAM\n"
            "📌 ABB ==> ABBANK - NH TMCP AN BINH\n"
            "📌 AGR ==> AGRIBANK - NH NN & PTNT VIET NAM\n"
            "📌 VCCB ==> BANVIET - NH TMCP BAN VIET\n"
            "📌 BVB ==> BAOVIETBANK - NH TMCP BAO VIET (BVB)\n"
            "📌 DAB ==> DONGABANK - NH TMCP DONG A\n"
            "📌 EIB ==> EXIMBANK - NH TMCP XUAT NHAP KHAU VIET NAM\n"
            "📌 GPB ==> GPBANK - NH TMCP DAU KHI TOAN CAU\n"
            "📌 HDB ==> HDBANK - NH TMCP PHAT TRIEN TP.HCM\n"
            "📌 KLB ==> KIENLONGBANK - NH TMCP KIEN LONG\n"
            "📌 NAB ==> NAMABANK - NH TMCP NAM A\n"
            "📌 NCB ==> NCB - NH TMCP QUOC DAN\n"
            "📌 OCB ==> OCB - NH TMCP PHUONG DONG\n"
            "📌 OJB ==> OCEANBANK - NH TMCP DAI DUONG (OJB)\n"
            "📌 PGB ==> PGBANK - NH TMCP XANG DAU PETROLIMEX\n"
            "📌 PVB ==> PVCOMBANK - NH TMCP DAI CHUNG VIET NAM\n"
            "📌 STB ==> SACOMBANK - NH TMCP SAI GON THUONG TIN\n"
            "📌 SGB ==> SAIGONBANK - NH TMCP SAI GON CONG THUONG\n"
            "📌 SCB ==> SCB - NH TMCP SAI GON\n"
            "📌 SAB ==> SEABANK - NH TMCP DONG NAM A\n"
            "📌 SHB ==> SHB - NH TMCP SAI GON HA NOI\n"
        )
        await query.message.reply_text(text)
        return

    if data == "acc_deposit_history":
        cur = db_f.execute(
            "SELECT * FROM deposits WHERE user_id = ? ORDER BY id DESC LIMIT 5",
            (user.id,),
        )
        rows = cur.fetchall()
        lines = ["Người gửi - Loại - Số Tiền - Thời gian\n"]
        if not rows:
            lines.append("Chưa có lịch sử nạp.")
        else:
            for r in rows:
                sender = r["display_id"]
                typ = "BANK"
                amt = format_currency(r["amount"])
                ts = r["time"]
                lines.append(f"🌃 {sender}   -   {typ}   -   {amt}   -   {ts}")
        await query.message.reply_text("\n".join(lines))
        return

    if data == "acc_withdraw_history":
        cur = db_f.execute(
            "SELECT * FROM withdrawals WHERE user_id = ? ORDER BY id DESC LIMIT 5",
            (user.id,),
        )
        rows = cur.fetchall()
        lines = ["Người gửi - Loại - Số Tiền - Thời gian - Trạng Thái\n"]
        if not rows:
            lines.append("Chưa có lịch sử rút.")
        else:
            for r in rows:
                sender = r["id"]
                typ = "BANK"
                amt = format_currency(r["amount"])
                ts = r["time"]
                st = "Thành công" if r["status"] == "approved" else "Từ chối"
                lines.append(f"🌃 {sender}   -   {typ}   -   {amt}   -   {ts}   -   {st}")
        await query.message.reply_text("\n".join(lines))
        return

    if data == "acc_vip_exchange":
        text = (
            "Với mỗi 300K tiền cược. quý khách sẽ được tặng thêm 1 điểm cấp VIP.  Điểm này sẽ dùng để xét tăng cấp VIP và để đổi thưởng.\n\n"
            "🏆CẤP VIP VÀ BIỂU TƯỢNG ĐẠI DƯƠNG\n"
            "Vip 1: 🦐 (Tôm)\n"
            "Vip 2: 🦞 (Tôm hùm)\n"
            "Vip 3: 🦑 (Mực)\n"
            "Vip 4: 🦀 (Cua)\n"
            "Vip 5: 🐙 (Bạch tuộc)\n"
            "Vip 6: 🐠 (Cá ngừ)\n"
            "Vip 7: 🐬 (Cá heo)\n"
            "Vip 8: 🦈 (Cá mập)\n"
            "Vip 9: 🐳 (Cá voi)\n\n"
            "📌 ĐIỂM YÊU CẦU ĐỂ ĐẠT CẤP VIP\n"
            "Vip 1: 0\n"
            "Vip 2: 10\n"
            "Vip 3: 50\n"
            "Vip 4: 100\n"
            "Vip 5: 500\n"
            "Vip 6: 1000\n"
            "Vip 7: 5000\n"
            "Vip 8: 15000\n"
            "Vip 9: 50000\n\n"
            "💎 TỈ LỆ QUY ĐỔI ĐIỂM\n"
            "Hãy tích điểm và quy đổi chúng thành tiền mặt với tỉ lệ cực kỳ hấp dẫn:\n"
            "Vip 1: 1điểm = 100đ\n"
            "Vip 2: 1điểm = 200đ\n"
            "Vip 3: 1điểm = 300đ\n"
            "Vip 4: 1điểm = 400đ\n"
            "Vip 5: 1điểm = 500đ\n"
            "Vip 6: 1điểm = 600đ\n"
            "Vip 7: 1điểm = 700đ\n"
            "Vip 8: 1điểm = 800đ\n"
            "Vip 9: 1điểm = 1000đ\n\n"
            "❤️ CÁCH ĐỔI ĐIỂM VIP\n"
            "/doidiemvip [dấu cách] số điểm\n\n"
            "➡️ Vd:   /doidiemvip 100"
        )
        await query.message.reply_text(text)
        return

    if data == "acc_giftcode_enter":
        text = (
            "💝 Để nhập Giftcode, vui lòng thực hiện theo cú pháp sau:\n\n"
            "/code [dấu cách] mã giftcode\n\n"
            "➡️ Vd:   /code LCTX"
        )
        await query.message.reply_text(text)
        return

    if data == "acc_bet_summary":
        count, total = get_today_bet_stats(user.id)
        text = (
            f"✅ ID: {user.id}\n"
            f"✅ Hôm nay bạn đã chơi {count} lượt\n"
            f"✅ Tổng tiền cược: {format_currency(total)}"
        )
        await query.message.reply_text(text)
        return


# ================== NẠP TIỀN ==================


async def napbank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or user.first_name)

    if not context.args:
        await update.message.reply_text("Vui lòng nhập số tiền. Ví dụ: /napbank 100000")
        return

    digits = "".join(ch for ch in context.args[0] if ch.isdigit())
    if not digits:
        await update.message.reply_text("Số tiền không hợp lệ.")
        return
    amount = int(digits)
    await create_deposit_info(update, context, user.id, amount)


async def handle_quick_deposit(
    query, context: ContextTypes.DEFAULT_TYPE, amount_str: str
):
    user = query.from_user
    ensure_user(user.id, user.username or user.first_name)
    try:
        amount = int(amount_str)
    except ValueError:
        await query.message.reply_text("Số tiền không hợp lệ.")
        return
    await create_deposit_info(query, context, user.id, amount)


async def create_deposit_info(
    update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int, amount: int
):
    if amount < MIN_DEPOSIT:
        msg = f"Số tiền nạp tối thiểu là {format_currency(MIN_DEPOSIT)}."
        if isinstance(update_or_query, Update):
            await update_or_query.message.reply_text(msg)
        else:
            await update_or_query.message.reply_text(msg)
        return

    bank = random.choice(BANK_ACCOUNTS)
    code = random_deposit_code()
    display_id = random_display_id()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    db_f = get_finance_db()
    cur = db_f.execute(
        "INSERT INTO deposits (user_id, amount, bank, account, owner, code, status, time, display_id) "
        "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
        (user_id, amount, bank["bank"], bank["account"], bank["owner"], code, now_str, display_id),
    )
    dep_id = cur.lastrowid
    db_f.commit()

    bank_text = (
        f"⬅️ Chuyển khoản theo thông tin sau:\n\n"
        f"🏦 Ngân hàng: {bank['bank']}\n"
        f"💳 Số tài khoản: {bank['account']}\n"
        f"👤 Chủ tài khoản: {bank['owner']}\n"
        f"🧾 Nội dung chuyển khoản:\n{code}\n"
        f"💰 Số tiền: {format_currency(amount)}\n\n"
        "⚠️ Lưu ý:\n"
        "✅ Chuyển đúng SỐ TIỀN và NỘI DUNG.\n"
        "♻️ Mỗi giao dịch có thông tin chuyển khoản RIÊNG – "
        "hãy tạo lệnh nạp mới trước mỗi lần nạp."
    )

    qr_image = generate_qr_image(bank["code"], bank["account"], bank["owner"], amount, code)

    if isinstance(update_or_query, Update):
        chat = update_or_query.effective_chat
    else:
        chat = update_or_query.message.chat

    await chat.send_photo(photo=qr_image, caption=bank_text)

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "📥 Yêu cầu NẠP TIỀN mới\n"
                f"🧾 Lệnh ID nội bộ: {dep_id}\n"
                f"🧾 Mã hiển thị: {display_id}\n"
                f"👤 User ID: {user_id}\n"
                f"💰 Số tiền: {format_currency(amount)}\n"
                f"🏦 Ngân hàng nhận: {bank['bank']} {bank['account']} ({bank['owner']})\n"
                f"🧾 Nội dung CK: {code}\n"
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Duyệt nạp", callback_data=f"dep:{dep_id}:approve"
                        ),
                        InlineKeyboardButton(
                            "❌ Từ chối nạp", callback_data=f"dep:{dep_id}:reject"
                        ),
                    ]
                ]
            ),
        )
    except Exception:
        pass


async def process_deposit_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, dep_id: int, action: str
):
    query = update.callback_query
    await query.answer()
    db_f = get_finance_db()
    db_p = get_players_db()

    cur = db_f.execute("SELECT * FROM deposits WHERE id = ?", (dep_id,))
    dep = cur.fetchone()
    if not dep:
        await query.message.reply_text("Không tìm thấy lệnh nạp này.")
        return
    if dep["status"] != "pending":
        await query.message.reply_text("Lệnh nạp này đã được xử lý trước đó.")
        return

    user_id = dep["user_id"]
    ensure_user(user_id, None)

    if action == "approve":
        db_f.execute(
            "UPDATE deposits SET status = 'approved' WHERE id = ?",
            (dep_id,),
        )
        db_p.execute(
            "UPDATE users SET balance = balance + ?, wager_required = wager_required + ?, total_deposit = total_deposit + ? "
            "WHERE user_id = ?",
            (dep["amount"], dep["amount"], dep["amount"], user_id),
        )
        db_f.commit()
        db_p.commit()

        cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        bal = cur2.fetchone()["balance"]

        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"✅ Nạp tiền thành công!\n"
                    f"💰 Số tiền: {format_currency(dep['amount'])}\n"
                    f"💰 Số dư mới: {format_currency(bal)}"
                ),
            )
        except Exception:
            pass

        await query.message.reply_text(
            f"Đã duyệt nạp cho user {user_id}, số tiền {format_currency(dep['amount'])}."
        )

    elif action == "reject":
        db_f.execute(
            "UPDATE deposits SET status = 'rejected' WHERE id = ?",
            (dep_id,),
        )
        db_f.commit()
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "❌ Lệnh nạp của bạn đã bị từ chối.\n"
                    "Vui lòng liên hệ admin để biết thêm chi tiết."
                ),
            )
        except Exception:
            pass
        await query.message.reply_text(f"Đã từ chối lệnh nạp ID {dep_id}.")


# ================== RÚT TIỀN ==================


async def rutbank_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    row = ensure_user(user.id, user.username or user.first_name)

    if len(context.args) < 4:
        await update.message.reply_text(
            "Cú pháp không hợp lệ.\nVD: /rutbank 100000 VCB 01234567890 NGUYEN VAN A"
        )
        return

    digits = "".join(ch for ch in context.args[0] if ch.isdigit())
    if not digits:
        await update.message.reply_text("Số tiền không hợp lệ.")
        return
    amount = int(digits)

    bank_code = context.args[1].upper()
    account_no = context.args[2]
    name = " ".join(context.args[3:]).upper()

    if amount < MIN_WITHDRAW:
        await update.message.reply_text(
            f"Số tiền rút tối thiểu là {format_currency(MIN_WITHDRAW)}."
        )
        return

    total_dep = get_total_deposit(user.id)
    if total_dep <= 0:
        await update.message.reply_text(
            "Tài khoản của bạn cần có ít nhất 1 lệnh nạp tối thiểu trước khi rút tiền."
        )
        return

    db_p = get_players_db()
    cur = db_p.execute(
        "SELECT balance, wager_done, wager_required FROM users WHERE user_id = ?",
        (user.id,),
    )
    u = cur.fetchone()
    if u["wager_done"] < u["wager_required"]:
        await update.message.reply_text(
            "Bạn cần hoàn thành đủ 1 vòng cược tổng số tiền đã nạp trước khi rút."
        )
        return

    if amount > u["balance"]:
        await update.message.reply_text(
            f"Số dư không đủ để rút {format_currency(amount)}. "
            f"Số dư hiện tại: {format_currency(u['balance'])}"
        )
        return

    bank_full = BANK_CODE_MAP.get(bank_code, bank_code)

    text = (
        "🏧 Bạn vừa yêu cầu RÚT TIỀN\n"
        f"💰 Số tiền: {format_currency(amount)}\n"
        f"🏦 Ngân hàng: {bank_code} – {bank_full}\n"
        f"💳 Số tài khoản: {account_no}\n"
        f"👤 Chủ tài khoản: {name}\n"
        "⚠️ Vui lòng kiểm tra kỹ thông tin trên. Nếu nhập sai, admin không hỗ trợ hoàn tiền."
    )

    pending = {
        "amount": amount,
        "bank_code": bank_code,
        "bank_full": bank_full,
        "account_no": account_no,
        "name": name,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    db_p.execute(
        "UPDATE users SET pending_withdraw_json = ? WHERE user_id = ?",
        (str(pending), user.id),
    )
    db_p.commit()

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Xác nhận rút", callback_data="wdr_confirm"),
                InlineKeyboardButton("❌ Hủy lệnh", callback_data="wdr_cancel"),
            ]
        ]
    )
    await update.message.reply_text(text, reply_markup=keyboard)


def load_pending_withdraw(user_id: int):
    db_p = get_players_db()
    cur = db_p.execute(
        "SELECT pending_withdraw_json FROM users WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    if not row or not row["pending_withdraw_json"]:
        return None
    try:
        return eval(row["pending_withdraw_json"], {"__builtins__": {}})
    except Exception:
        return None


async def handle_withdraw_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_p = get_players_db()
    db_f = get_finance_db()

    pending = load_pending_withdraw(user.id)
    if not pending:
        await query.message.reply_text("Không có yêu cầu rút nào đang chờ xác nhận.")
        return

    cur = db_p.execute(
        "SELECT balance, wager_done, wager_required, total_deposit FROM users WHERE user_id = ?",
        (user.id,),
    )
    u = cur.fetchone()
    amount = pending["amount"]

    if amount < MIN_WITHDRAW:
        await query.message.reply_text(
            f"Số tiền rút tối thiểu là {format_currency(MIN_WITHDRAW)}."
        )
        db_p.execute(
            "UPDATE users SET pending_withdraw_json = NULL WHERE user_id = ?",
            (user.id,),
        )
        db_p.commit()
        return

    if u["total_deposit"] <= 0:
        await query.message.reply_text(
            "Tài khoản của bạn cần có ít nhất 1 lệnh nạp tối thiểu trước khi rút tiền."
        )
        db_p.execute(
            "UPDATE users SET pending_withdraw_json = NULL WHERE user_id = ?",
            (user.id,),
        )
        db_p.commit()
        return

    if u["wager_done"] < u["wager_required"]:
        await query.message.reply_text(
            "Bạn cần hoàn thành đủ 1 vòng cược tổng số tiền đã nạp trước khi rút."
        )
        db_p.execute(
            "UPDATE users SET pending_withdraw_json = NULL WHERE user_id = ?",
            (user.id,),
        )
        db_p.commit()
        return

    if amount > u["balance"]:
        await query.message.reply_text(
            f"Số dư không đủ để rút {format_currency(amount)}. "
            f"Số dư hiện tại: {format_currency(u['balance'])}"
        )
        db_p.execute(
            "UPDATE users SET pending_withdraw_json = NULL WHERE user_id = ?",
            (user.id,),
        )
        db_p.commit()
        return

    db_p.execute(
        "UPDATE users SET balance = balance - ?, pending_withdraw_json = NULL WHERE user_id = ?",
        (amount, user.id),
    )
    cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
    new_bal = cur2.fetchone()["balance"]

    cur3 = db_f.execute(
        "INSERT INTO withdrawals (user_id, amount, bank_code, bank_full, account_no, name, status, time) "
        "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
        (
            user.id,
            amount,
            pending["bank_code"],
            pending["bank_full"],
            pending["account_no"],
            pending["name"],
            pending["time"],
        ),
    )
    wdr_id = cur3.lastrowid
    db_p.commit()
    db_f.commit()

    await query.message.reply_text(
        "✅ Yêu cầu rút tiền đã được xác nhận. Vui lòng chờ admin duyệt.\n"
        f"💰 Số dư còn lại: {format_currency(new_bal)}"
    )

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "📤 Yêu cầu RÚT TIỀN mới\n"
                f"🧾 Lệnh ID: {wdr_id}\n"
                f"👤 User ID: {user.id}\n"
                f"💰 Số tiền: {format_currency(amount)}\n"
                f"🏦 Ngân hàng: {pending['bank_code']} – {pending['bank_full']}\n"
                f"💳 Số tài khoản: {pending['account_no']}\n"
                f"👤 Chủ tài khoản: {pending['name']}\n"
            ),
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Duyệt rút", callback_data=f"wdr:{wdr_id}:approve"
                        ),
                        InlineKeyboardButton(
                            "❌ Từ chối rút", callback_data=f"wdr:{wdr_id}:reject"
                        ),
                    ]
                ]
            ),
        )
    except Exception:
        pass


async def handle_withdraw_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_p = get_players_db()
    db_p.execute(
        "UPDATE users SET pending_withdraw_json = NULL WHERE user_id = ?",
        (user.id,),
    )
    db_p.commit()
    await query.message.reply_text("❌ Bạn đã hủy yêu cầu rút tiền này.")


async def process_withdraw_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE, wdr_id: int, action: str
):
    query = update.callback_query
    await query.answer()
    db_f = get_finance_db()
    db_p = get_players_db()

    cur = db_f.execute("SELECT * FROM withdrawals WHERE id = ?", (wdr_id,))
    wdr = cur.fetchone()
    if not wdr:
        await query.message.reply_text("Không tìm thấy lệnh rút này.")
        return
    if wdr["status"] != "pending":
        await query.message.reply_text("Lệnh rút này đã được xử lý trước đó.")
        return

    user_id = wdr["user_id"]

    if action == "approve":
        db_f.execute(
            "UPDATE withdrawals SET status = 'approved' WHERE id = ?",
            (wdr_id,),
        )
        db_f.commit()

        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ Lệnh rút của bạn đã được duyệt. Vui lòng kiểm tra tài khoản ngân hàng.",
            )
        except Exception:
            pass

        await query.message.reply_text(
            f"Đã duyệt rút {format_currency(wdr['amount'])} cho user {user_id}."
        )

    elif action == "reject":
        db_f.execute(
            "UPDATE withdrawals SET status = 'rejected' WHERE id = ?",
            (wdr_id,),
        )
        db_p.execute(
            "UPDATE users SET balance = balance + ? WHERE user_id = ?",
            (wdr["amount"], user_id),
        )
        db_f.commit()
        db_p.commit()

        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "❌ Yêu cầu rút bị từ chối.\n"
                    "💬 Tiền thưởng yêu cầu nạp = số tiền rút kèm 1 vòng cược.\n"
                    "📞 Liên hệ admin để biết thêm chi tiết."
                ),
            )
        except Exception:
            pass

        await query.message.reply_text(
            f"Đã từ chối lệnh rút ID {wdr_id} và hoàn tiền cho user {user_id}."
        )


# ================== GAME: TÀI XỈU ==================


def parse_taixiu_bet(text: str):
    parts = text.upper().split()
    if len(parts) != 2:
        return None, None
    code, amt_raw = parts
    if code not in {"C", "L", "X", "T"}:
        return None, None
    digits = "".join(ch for ch in amt_raw if ch.isdigit())
    if not digits:
        return None, None
    return code, int(digits)


def is_taixiu_win(code: str, total: int) -> bool:
    if code == "C":
        return total in {4, 6, 8, 10, 12, 14, 16, 18}
    if code == "L":
        return total in {1, 3, 5, 7, 9, 11, 13, 15, 17}
    if code == "X":
        return 3 <= total <= 10
    if code == "T":
        return 11 <= total <= 18
    return False


async def show_taixiu_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎲 TÀI XỈU TELEGRAM 🎲\n\n"
        "BOT sẽ tung xúc xắc\n\n"
        "Nội dung |  Tổng điểm 3 xúc xắc  |  Tỷ lệ ăn\n"
        "C  |  4,6,8,10,12,14,16,18  |  x1.95\n"
        "L  |  1,3,5,7,9,11,13,15,17  |  x1.95\n"
        "X  |  3,4,5,6,7,8,9,10  |  x1.95\n"
        "T  |  11,12,13,14,15,16,17,18  |  x1.95\n\n"
        f"👉 Tối thiểu là {format_currency(MIN_BET)} và tối đa là {format_currency(MAX_BET)}.\n\n"
        "🔖 Cách chơi: [Nội dung] [tiền cược]\n"
        "VD: T 10000 hoặc X 10000"
    )
    await update.effective_message.reply_text(text)


async def play_taixiu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, code: str, amount: int
):
    user = update.effective_user
    db_p = get_players_db()
    row = ensure_user(user.id, user.username or user.first_name)
    balance = row["balance"]

    if amount < MIN_BET or amount > MAX_BET:
        await update.effective_message.reply_text(
            f"Số tiền cược phải từ {format_currency(MIN_BET)} đến {format_currency(MAX_BET)}."
        )
        return
    if amount > balance:
        await update.effective_message.reply_text(
            f"Bạn không đủ số dư để cược {format_currency(amount)}. "
            f"Số dư hiện tại: {format_currency(balance)}"
        )
        return

    db_p.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ?",
        (amount, user.id),
    )
    db_p.commit()

    total = 0
    chat = update.effective_chat
    for _ in range(3):
        msg = await chat.send_dice(emoji="🎲")
        total += msg.dice.value
        await asyncio.sleep(1.2)

    await asyncio.sleep(1.5)

    mid = random_message_id()
    win = is_taixiu_win(code, total)

    cur = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
    bal_now = cur.fetchone()["balance"]

    if win:
        payout = decimal_payout(amount, TAIXIU_MULTIPLIER_TOTAL)
        db_p.execute(
            "UPDATE users SET balance = balance + ?, last_bet_json = ? WHERE user_id = ?",
            (payout, str({"game": "taixiu", "code": code, "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
        bal_now = cur2.fetchone()["balance"]
        text = (
            "🏆🏆🏆 THẮNG RỒI 🏆🏆🏆\n"
            f"💶 ND cược: {code}\n"
            f"💶 Tiền cược: {format_currency(amount)}\n"
            f"💶 Tiền nhận: {format_currency(payout)}\n"
            f"💶 Số dư: {format_currency(bal_now)}\n"
            f"💶 MessageID: {mid}"
        )
    else:
        db_p.execute(
            "UPDATE users SET last_bet_json = ? WHERE user_id = ?",
            (str({"game": "taixiu", "code": code, "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        text = (
            "😭😭😭 THUA MẤT RỒI 😭😭😭\n"
            f"💢 ND cược: {code}\n"
            f"💢 Tiền cược: {format_currency(amount)}\n"
            "💢 Tiền nhận: 0 ₫\n"
            f"💢 Số dư: {format_currency(bal_now)}\n"
            f"💢 MessageID: {mid}"
        )

    keyboard = [[InlineKeyboardButton("🔁 Chơi lại", callback_data="rebet")]]
    await chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
    await chat.send_message("🎮 Chiến tiếp thôi!!!")


# ================== GAME: XÚC XẮC ==================


def parse_xucxac_bet(text: str):
    parts = text.upper().split()
    if len(parts) != 2:
        return None, None
    code, amt_raw = parts
    valid = {"XXC", "XXL", "XXT", "XXX", "D1", "D2", "D3", "D4", "D5", "D6"}
    if code not in valid:
        return None, None
    digits = "".join(ch for ch in amt_raw if ch.isdigit())
    if not digits:
        return None, None
    return code, int(digits)


def is_xucxac_win(code: str, v: int) -> bool:
    if code == "XXC":
        return v in {2, 4, 6}
    if code == "XXL":
        return v in {1, 3, 5}
    if code == "XXT":
        return v in {4, 5, 6}
    if code == "XXX":
        return v in {1, 2, 3}
    if code == "D1":
        return v == 1
    if code == "D2":
        return v == 2
    if code == "D3":
        return v == 3
    if code == "D4":
        return v == 4
    if code == "D5":
        return v == 5
    if code == "D6":
        return v == 6
    return False


def get_xucxac_multiplier_total(code: str) -> Decimal:
    if code in {"XXC", "XXL", "XXT", "XXX"}:
        return XX_GROUP_MULTIPLIER_TOTAL
    return XX_SINGLE_MULTIPLIER_TOTAL


async def show_xucxac_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎲 XÚC XẮC TELEGRAM 🎲\n\n"
        "👉 Khi BOT trả lời mới được tính là đã đặt cược thành công. "
        "Nếu BOT không trả lời => Lượt chơi không hợp lệ và không bị trừ tiền trong tài khoản.\n"
        "👉 Xúc xắc được quay random bởi Telegram nên hoàn toàn xanh chín.\n\n"
        "❗️❗️❗️ Lưu ý: Các biểu tượng Emoji của Telegram click vào có thể tương tác được "
        "tránh bị nhầm lẫn các đối tượng giả mạo bằng ảnh gif ❗️❗️❗️\n\n"
        "🔖 Thể lệ:\n"
        "👍 Kết quả được tính bằng mặt Xúc Xắc Telegram trả về sau khi người chơi đặt cược:\n"
        "XXC  ➤   x1.95  ➤ Xúc Xắc: 2,4,6\n"
        "XXL  ➤   x1.95  ➤ Xúc Xắc: 1,3,5\n"
        "XXT  ➤   x1.95  ➤ Xúc Xắc: 4,5,6\n"
        "XXX  ➤   x1.95  ➤ Xúc Xắc: 1,2,3\n"
        "D1   ➤   x5  ➤ Xúc Xắc: 1\n"
        "D2   ➤   x5  ➤ Xúc Xắc: 2\n"
        "D3   ➤   x5  ➤ Xúc Xắc: 3\n"
        "D4   ➤   x5  ➤ Xúc Xắc: 4\n"
        "D5   ➤   x5  ➤ Xúc Xắc: 5\n"
        "D6   ➤   x5  ➤ Xúc Xắc: 6\n\n"
        "🎮 Cách chơi:\n"
        "👉 Chat tại đây nội dung như sau:\n"
        "\"Nội dung\" dấu cách \"Số tiền cược(VD: D1 10000)"
    )
    await update.effective_message.reply_text(text)


async def play_xucxac(
    update: Update, context: ContextTypes.DEFAULT_TYPE, code: str, amount: int
):
    user = update.effective_user
    db_p = get_players_db()
    row = ensure_user(user.id, user.username or user.first_name)
    balance = row["balance"]

    if amount < MIN_BET or amount > MAX_BET:
        await update.effective_message.reply_text(
            f"Số tiền cược phải từ {format_currency(MIN_BET)} đến {format_currency(MAX_BET)}."
        )
        return
    if amount > balance:
        await update.effective_message.reply_text(
            f"Bạn không đủ số dư để cược {format_currency(amount)}. "
            f"Số dư hiện tại: {format_currency(balance)}"
        )
        return

    db_p.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ?",
        (amount, user.id),
    )
    db_p.commit()

    chat = update.effective_chat
    dice_msg = await chat.send_dice(emoji="🎲")
    await asyncio.sleep(3)
    v = dice_msg.dice.value

    mid = random_message_id()
    win = is_xucxac_win(code, v)

    cur = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
    bal_now = cur.fetchone()["balance"]

    if win:
        multiplier = get_xucxac_multiplier_total(code)
        payout = decimal_payout(amount, multiplier)
        db_p.execute(
            "UPDATE users SET balance = balance + ?, last_bet_json = ? WHERE user_id = ?",
            (payout, str({"game": "xucxac", "code": code, "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
        bal_now = cur2.fetchone()["balance"]
        text = (
            "🏆🏆🏆 THẮNG RỒI 🏆🏆🏆\n"
            f"💶 ND cược: {code}\n"
            f"💶 Tiền cược: {format_currency(amount)}\n"
            f"💶 Tiền nhận: {format_currency(payout)}\n"
            f"💶 Số dư: {format_currency(bal_now)}\n"
            f"💶 MessageID: {mid}"
        )
    else:
        db_p.execute(
            "UPDATE users SET last_bet_json = ? WHERE user_id = ?",
            (str({"game": "xucxac", "code": code, "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        text = (
            "😭😭😭 THUA MẤT RỒI 😭😭😭\n"
            f"💢 ND cược: {code}\n"
            f"💢 Tiền cược: {format_currency(amount)}\n"
            "💢 Tiền nhận: 0 ₫\n"
            f"💢 Số dư: {format_currency(bal_now)}\n"
            f"💢 MessageID: {mid}"
        )

    keyboard = [[InlineKeyboardButton("🔁 Chơi lại", callback_data="rebet")]]
    await chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
    await chat.send_message("🎮 Chiến tiếp thôi!!!")


# ================== GAME: BOWLING ==================


def parse_bowling_bet(text: str):
    parts = text.upper().split()
    if len(parts) != 2:
        return None, None
    code, amt_raw = parts
    if code not in {"BC", "BL", "BX", "BT"}:
        return None, None
    digits = "".join(ch for ch in amt_raw if ch.isdigit())
    if not digits:
        return None, None
    return code, int(digits)


def is_bowling_win(code: str, val: int) -> bool:
    if code == "BC":
        return val in {0, 2, 6}
    if code == "BL":
        return val in {1, 3, 5}
    if code == "BX":
        return val in {0, 1, 2}
    if code == "BT":
        return val in {3, 5, 6}
    return False


async def show_bowling_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎳 Game Bowling 🎳\n\n"
        "🔖 Số ki Bowling còn đứng (không bị ném ngã) được dùng để tính kết quả!\n\n"
        "Nội dung |  Kết quả  |  Tỷ lệ ăn\n"
        "BC  |  0, 2, 6  |  x1.95\n"
        "BL  |  1, 3, 5  |  x1.95\n"
        "BX  |  0, 1, 2  |  x1.95\n"
        "BT  |  3, 5, 6  |  x1.95\n\n"
        f"👉 Tối thiểu là {format_currency(MIN_BET)} và tối đa là {format_currency(MAX_BET)}\n\n"
        "🔖 Cách chơi: [Nội dung] [tiền cược]\n"
        "VD: BC 10000 hoặc BL 10000"
    )
    await update.effective_message.reply_text(text)


async def play_bowling(
    update: Update, context: ContextTypes.DEFAULT_TYPE, code: str, amount: int
):
    user = update.effective_user
    db_p = get_players_db()
    row = ensure_user(user.id, user.username or user.first_name)
    balance = row["balance"]

    if amount < MIN_BET or amount > MAX_BET:
        await update.effective_message.reply_text(
            f"Số tiền cược phải từ {format_currency(MIN_BET)} đến {format_currency(MAX_BET)}."
        )
        return
    if amount > balance:
        await update.effective_message.reply_text(
            f"Bạn không đủ số dư để cược {format_currency(amount)}. "
            f"Số dư hiện tại: {format_currency(balance)}"
        )
        return

    db_p.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ?",
        (amount, user.id),
    )
    db_p.commit()

    chat = update.effective_chat
    dice_msg = await chat.send_dice(emoji="🎳")
    await asyncio.sleep(3)
    val = dice_msg.dice.value

    mid = random_message_id()
    win = is_bowling_win(code, val)

    cur = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
    bal_now = cur.fetchone()["balance"]

    if win:
        payout = decimal_payout(amount, BOWLING_MULTIPLIER_TOTAL)
        db_p.execute(
            "UPDATE users SET balance = balance + ?, last_bet_json = ? WHERE user_id = ?",
            (payout, str({"game": "bowling", "code": code, "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
        bal_now = cur2.fetchone()["balance"]
        text = (
            "🏆🏆🏆 THẮNG RỒI 🏆🏆🏆\n"
            f"💶 ND cược: {code}\n"
            f"💶 Tiền cược: {format_currency(amount)}\n"
            f"💶 Tiền nhận: {format_currency(payout)}\n"
            f"💶 Số dư: {format_currency(bal_now)}\n"
            f"💶 MessageID: {mid}"
        )
    else:
        db_p.execute(
            "UPDATE users SET last_bet_json = ? WHERE user_id = ?",
            (str({"game": "bowling", "code": code, "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        text = (
            "😭😭😭 THUA MẤT RỒI 😭😭😭\n"
            f"💢 ND cược: {code}\n"
            f"💢 Tiền cược: {format_currency(amount)}\n"
            "💢 Tiền nhận: 0 ₫\n"
            f"💢 Số dư: {format_currency(bal_now)}\n"
            f"💢 MessageID: {mid}"
        )

    keyboard = [[InlineKeyboardButton("🔁 Chơi lại", callback_data="rebet")]]
    await chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
    await chat.send_message("🎮 Chiến tiếp thôi!!!")


# ================== GAME: BÓNG RỔ ==================


def parse_bongro_bet(text: str):
    parts = text.upper().split()
    if len(parts) != 2:
        return None, None
    code, amt_raw = parts
    if code != "BR":
        return None, None
    digits = "".join(ch for ch in amt_raw if ch.isdigit())
    if not digits:
        return None, None
    return code, int(digits)


def is_bongro_win(v: int) -> bool:
    return v in (4, 5)


async def show_bongro_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🏀 Game Bóng Rổ 🏀\n\n"
        "🔖 Ném bóng vào rổ sẽ tính là chiến thắng, tỉ lệ trả thưởng x2.3\n\n"
        f"👉 Tối thiểu là {format_currency(MIN_BET)} và tối đa là {format_currency(MAX_BET)}\n\n"
        "🔖 Cách chơi: BR [tiền cược]\n"
        "VD: BR 10000\n"
        "Lưu ý bóng phải rơi vào hẳn rổ mới tính nha"
    )
    await update.effective_message.reply_text(text)


async def play_bongro(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int):
    user = update.effective_user
    db_p = get_players_db()
    row = ensure_user(user.id, user.username or user.first_name)
    balance = row["balance"]

    if amount < MIN_BET or amount > MAX_BET:
        await update.effective_message.reply_text(
            f"Số tiền cược phải từ {format_currency(MIN_BET)} đến {format_currency(MAX_BET)}."
        )
        return
    if amount > balance:
        await update.effective_message.reply_text(
            f"Bạn không đủ số dư để cược {format_currency(amount)}. "
            f"Số dư hiện tại: {format_currency(balance)}"
        )
        return

    db_p.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ?",
        (amount, user.id),
    )
    db_p.commit()

    chat = update.effective_chat
    dice_msg = await chat.send_dice(emoji="🏀")
    await asyncio.sleep(3)
    v = dice_msg.dice.value

    mid = random_message_id()
    win = is_bongro_win(v)

    cur = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
    bal_now = cur.fetchone()["balance"]

    if win:
        payout = decimal_payout(amount, BASKETBALL_MULTIPLIER_TOTAL)
        db_p.execute(
            "UPDATE users SET balance = balance + ?, last_bet_json = ? WHERE user_id = ?",
            (payout, str({"game": "bongro", "code": "BR", "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
        bal_now = cur2.fetchone()["balance"]
        text = (
            "🏆🏆🏆 THẮNG RỒI 🏆🏆🏆\n"
            "💶 ND cược: BR\n"
            f"💶 Tiền cược: {format_currency(amount)}\n"
            f"💶 Tiền nhận: {format_currency(payout)}\n"
            f"💶 Số dư: {format_currency(bal_now)}\n"
            f"💶 MessageID: {mid}"
        )
    else:
        db_p.execute(
            "UPDATE users SET last_bet_json = ? WHERE user_id = ?",
            (str({"game": "bongro", "code": "BR", "amount": amount}), user.id),
        )
        db_p.commit()
        add_vip_points_and_wager(user.id, amount)
        text = (
            "😭😭😭 THUA MẤT RỒI 😭😭😭\n"
            "💢 ND cược: BR\n"
            f"💢 Tiền cược: {format_currency(amount)}\n"
            "💢 Tiền nhận: 0 ₫\n"
            f"💢 Số dư: {format_currency(bal_now)}\n"
            f"💢 MessageID: {mid}"
        )

    keyboard = [[InlineKeyboardButton("🔁 Chơi lại", callback_data="rebet")]]
    await chat.send_message(text, reply_markup=InlineKeyboardMarkup(keyboard))
    await chat.send_message("🎮 Chiến tiếp thôi!!!")


# ================== LÔ ĐỀ (HIỂN THỊ LUẬT) ==================


async def show_lode_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🍀 Lô ĐỀ TELEGRAM 🍀\n"
        "🔖 Thể lệ:\n"
        "👉 Kết quả được xác định thông qua KẾT QUẢ XỔ SỐ MIỀN BẮC ngày hôm đó.\n"
        "Lô  ➤   x80\n"
        "Đề  ➤   x70\n"
        "Lô Xiên 2  ➤   x10\n"
        "Lô Xiên 3  ➤   x40\n"
        "Lô Xiên 4  ➤   x100\n"
        "👉 Tỉ lệ điểm:\n"
        "Lô  ➤   1 điểm   ➤   23.000 ₫\n"
        "Đề  ➤   1 điểm   ➤   1.000\n"
        "Lô Xiên  ➤   1 điểm   ➤   1.000\n"
        "🎮 Cách chơi:\n"
        "👉 Đánh Lô Đề theo cú pháp sau:\n"
        "/lo [dấu cách] số [dấu cách] điểm đánh\n"
        "/de [dấu cách] cặp số [dấu cách] điểm đánh\n"
        "/xienhai [dấu cách] cặp số [dấu cách] điểm đánh\n"
        "/xienba [dấu cách] cặp số [dấu cách] điểm đánh\n"
        "/xienbon [dấu cách] cặp số [dấu cách] điểm đánh\n"
        "Ví dụ:\n"
        "Bạn muốn đánh 10 điểm ĐỀ 00:\n"
        "/de 00 10\n\n"
        "Bạn muốn đánh 00, 01 mỗi con 10 điểm ĐỀ:\n"
        "/de 00,01 10\n\n"
        "Bạn muốn đánh 10 điểm LÔ 00:\n"
        "/lo 00 10\n\n"
        "Bạn muốn đánh 00, 99 mỗi con 10 điểm LÔ:\n"
        "/lo 00,99 10\n\n"
        "Bạn muốn đánh 10 điểm LÔ XIÊN:\n"
        "/xienhai 00,01 10\n"
        "/xienba 00,01,02 10\n"
        "/xienbon 00,01,02,03 10\n\n"
        "⚠️ Phần trả thưởng Lô Đề theo KQXS MB sẽ cần thêm code lấy kết quả SSMB (chưa làm tự động)."
    )
    await update.effective_message.reply_text(text)


# ================== CHƠI LẠI ==================


async def handle_rebet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db_p = get_players_db()
    cur = db_p.execute(
        "SELECT last_bet_json FROM users WHERE user_id = ?",
        (user.id,),
    )
    row = cur.fetchone()
    if not row or not row["last_bet_json"]:
        await query.message.reply_text("Không tìm thấy cược trước đó để chơi lại.")
        return
    try:
        last = eval(row["last_bet_json"], {"__builtins__": {}})
    except Exception:
        await query.message.reply_text("Dữ liệu cược trước đó không hợp lệ.")
        return

    game = last.get("game")
    code = last.get("code")
    amount = last.get("amount")

    fake_update = Update(update.update_id)
    fake_update._effective_chat = query.message.chat
    fake_update._effective_user = user
    fake_update._effective_message = query.message

    if game == "taixiu":
        await play_taixiu(fake_update, context, code, amount)
    elif game == "xucxac":
        await play_xucxac(fake_update, context, code, amount)
    elif game == "bowling":
        await play_bowling(fake_update, context, code, amount)
    elif game == "bongro":
        await play_bongro(fake_update, context, amount)
    else:
        await query.message.reply_text("Không tìm thấy game tương ứng cho lệnh chơi lại.")


# ================== DANH SÁCH GAME & TEXT MENU ==================


async def handle_main_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user = update.effective_user
    ensure_user(user.id, user.username or user.first_name)

    if text == "🎲 Danh sách Game":
        keyboard = [
            [InlineKeyboardButton("🎲 Tài Xỉu 🎲", callback_data="game_taixiu")],
            [InlineKeyboardButton("🎲 Xúc Xắc 🎲", callback_data="game_xucxac")],
            [
                InlineKeyboardButton("🎳 Bowling 🎳", callback_data="game_bowling"),
                InlineKeyboardButton("💰 Lô Đề 💰", callback_data="game_lode"),
            ],
            [InlineKeyboardButton("🏀 Bóng Rổ 🏀", callback_data="game_bongro")],
        ]
        await update.message.reply_text(
            "Vui lòng chọn Game", reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if text == "👤 Tài khoản":
        await show_account(update, context)
        return

    if text == "🥇 Bảng xếp hạng":
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🥇 BXH ngày", callback_data="rank_day"),
                    InlineKeyboardButton("🥇 BXH tuần", callback_data="rank_week"),
                ]
            ]
        )
        await update.message.reply_text(
            "Chọn loại bảng xếp hạng:", reply_markup=keyboard
        )
        return

    if text == "👥 Giới thiệu bạn bè":
        bot_username = context.bot.username or "yourbot"
        link = f"https://t.me/{bot_username}?start={user.id}"
        msg = (
            f"👉 Link mời bạn bè của bạn:  {link}\n\n"
            "🌺 Nhận ngay 2.000đ khi giới thiệu thành công!\n\n"
            "🌺 Nhận ngay HOA HỒNG bằng 1% số tiền đặt cược từ người chơi bạn giới thiệu."
        )
        await update.message.reply_text(msg)
        return

    if text == "💵 Hoa hồng":
        db_p = get_players_db()
        cur = db_p.execute(
            "SELECT COUNT(*) AS c FROM disciples WHERE referrer_id = ?",
            (user.id,),
        )
        disciples = cur.fetchone()["c"]
        comm_today = sum_commission_period(user.id, "day")
        comm_week = sum_commission_period(user.id, "week")
        comm_month = sum_commission_period(user.id, "month")
        msg = (
            f"🫂🫂🫂 Số lượng đệ tử của bạn  {disciples} 🫂🫂🫂\n"
            f"🤑 Hoa hồng nhận được hôm nay {format_currency(comm_today)}\n"
            f"🤑 Hoa hồng nhận được tuần này {format_currency(comm_week)}\n"
            f"🤑 Hoa hồng nhận được tháng này {format_currency(comm_month)}\n\n"
            "🔖 Tiền hoa hồng ĐÃ được cộng trực tiếp vào tài khoản ngay sau khi đệ tử đặt cược."
        )
        await update.message.reply_text(msg)
        return

    
    if text == "🎁 Khuyến mãi game":
        msg = (
            "🎁 KHUYẾN MÃI GAME 🎁\n\n"
            "🌟 Hiện tại các ưu đãi đang áp dụng:\n"
            "1️⃣ Thưởng 2.000đ cho mỗi người chơi mới bạn giới thiệu thành công.\n"
            "2️⃣ Hoa hồng 1% trên tổng số tiền đặt cược của đệ tử.\n"
            "3️⃣ Tích điểm VIP với mỗi 300K tiền cược để lên cấp và đổi thưởng.\n\n"
            "👉 Bạn có thể xem chi tiết code tân thủ và khuyến mãi nạp đầu tại các nút bên dưới."
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 Nhận Code Tân Thủ 🎁", callback_data="promo:newbie_code")],
            [InlineKeyboardButton("🎁 Khuyến mãi nạp đầu 🎁", callback_data="promo_first_deposit")],
        ])
        await update.message.reply_text(msg, reply_markup=keyboard)
        return

    if text == "Trung tâm hỗ trợ":
        await update.message.reply_text("Vui lòng liên hệ hỗ trợ tại: @jennybotforex")
        return

    # Không phải câu menu -> xử lý như cược game
    await handle_game_bet_text(update, context)


async def handle_game_bet_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    code, amount = parse_taixiu_bet(text)
    if code:
        await play_taixiu(update, context, code, amount)
        return

    code, amount = parse_xucxac_bet(text)
    if code:
        await play_xucxac(update, context, code, amount)
        return

    code, amount = parse_bowling_bet(text)
    if code:
        await play_bowling(update, context, code, amount)
        return

    code, amount = parse_bongro_bet(text)
    if code:
        await play_bongro(update, context, amount)
        return

    # Không match game nào -> im lặng cho đỡ spam


# ================== BXH ẢO + THẬT ==================



def get_fake_daily_players():
    global FAKE_DAILY_DATE, FAKE_DAILY_PLAYERS, FAKE_DAILY_LAST_UPDATE
    today = today_str()
    DAILY_MAX = 800_000_000  # trần 80tr

    now = datetime.now()

    # Ngày mới hoặc chưa có dữ liệu -> reset danh sách
    if FAKE_DAILY_DATE != today or not FAKE_DAILY_PLAYERS:
        FAKE_DAILY_DATE = today
        FAKE_DAILY_PLAYERS = []
        used_ids = set()
        
        # Tính số giờ đã trôi qua trong ngày để tạo giá trị ban đầu phù hợp
        hours_passed = now.hour + now.minute / 60.0
        # Mỗi giờ tăng khoảng 3-5 triệu cho top players
        base_multiplier = hours_passed * random.uniform(2_500_000, 4_500_000)
        
        # Tạo 10 người chơi với tổng cược tỉ lệ với thời gian trong ngày
        for i in range(10):
            fake_id = random.randint(100_000_000, 999_999_999)
            while fake_id in used_ids:
                fake_id = random.randint(100_000_000, 999_999_999)
            used_ids.add(fake_id)
            
            # Vị trí càng cao thì tổng cược càng lớn, với random để tự nhiên
            position_factor = (10 - i) / 10.0  # 1.0 cho top 1, 0.1 cho top 10
            base_total = int(base_multiplier * position_factor * random.uniform(0.6, 1.4))
            # Thêm random offset
            base_total += random.randint(500_000, 5_000_000)
            base_total = min(base_total, DAILY_MAX)
            
            # Mỗi người có tốc độ tăng khác nhau
            speed_factor = random.uniform(0.5, 2.0)
            FAKE_DAILY_PLAYERS.append({
                "id": fake_id, 
                "total": max(base_total, random.randint(500_000, 3_000_000)),
                "speed": speed_factor,
                "active_chance": random.uniform(0.5, 0.95)
            })

        # Bắt đầu tính từ thời điểm hiện tại
        FAKE_DAILY_LAST_UPDATE = now

    if FAKE_DAILY_LAST_UPDATE is None:
        FAKE_DAILY_LAST_UPDATE = now

    # Cập nhật mỗi 1 phút để thấy thay đổi nhanh hơn
    elapsed_minutes = int((now - FAKE_DAILY_LAST_UPDATE).total_seconds() // 60)
    if elapsed_minutes > 0:
        from datetime import timedelta
        for _ in range(elapsed_minutes):
            for p in FAKE_DAILY_PLAYERS:
                # Không phải lúc nào cũng tăng (random skip)
                if random.random() > p.get("active_chance", 0.7):
                    continue
                # Mức tăng ngẫu nhiên
                base_inc = random.randint(50_000, 800_000)
                inc = int(base_inc * p.get("speed", 1.0))
                # 8% cơ hội thắng lớn
                if random.random() < 0.08:
                    inc = int(inc * random.uniform(2.0, 5.0))
                p["total"] = min(p["total"] + inc, DAILY_MAX)
        FAKE_DAILY_LAST_UPDATE = now

    FAKE_DAILY_PLAYERS.sort(key=lambda x: x["total"], reverse=True)
    return FAKE_DAILY_PLAYERS


def get_fake_weekly_players():
    global FAKE_WEEK_KEY, FAKE_WEEKLY_PLAYERS, FAKE_WEEK_LAST_UPDATE
    today = date.today()
    iso = today.isocalendar()
    wk = (iso.year, iso.week)

    WEEK_MAX = 9_000_000_000   # 900tr

    now = datetime.now()

    # Tuần mới hoặc chưa có -> tạo mới
    if FAKE_WEEK_KEY != wk or not FAKE_WEEKLY_PLAYERS:
        FAKE_WEEK_KEY = wk
        FAKE_WEEKLY_PLAYERS = []
        used_ids = set()

        # Tính số ngày đã qua trong tuần (0 = thứ 2)
        days_passed = today.weekday() + (now.hour / 24.0)
        # Mỗi ngày tăng khoảng 80-120 triệu cho top players
        base_multiplier = days_passed * random.uniform(70_000_000, 110_000_000)

        for i in range(10):
            fake_id = random.randint(100_000_000, 999_999_999)
            while fake_id in used_ids:
                fake_id = random.randint(100_000_000, 999_999_999)
            used_ids.add(fake_id)

            # Vị trí càng cao thì tổng cược càng lớn
            position_factor = (10 - i) / 10.0
            base_total = int(base_multiplier * position_factor * random.uniform(0.5, 1.3))
            base_total += random.randint(30_000_000, 80_000_000)
            base_total = min(base_total, WEEK_MAX)
            
            speed_factor = random.uniform(0.6, 1.8)
            active_chance = random.uniform(0.5, 0.9)
            
            FAKE_WEEKLY_PLAYERS.append({
                "id": fake_id, 
                "total": max(base_total, random.randint(50_000_000, 100_000_000)),
                "speed": speed_factor,
                "active_chance": active_chance
            })

        # Bắt đầu tính từ thời điểm hiện tại
        FAKE_WEEK_LAST_UPDATE = now

    if FAKE_WEEK_LAST_UPDATE is None:
        FAKE_WEEK_LAST_UPDATE = now

    # Cập nhật mỗi 5 phút
    elapsed_minutes = int((now - FAKE_WEEK_LAST_UPDATE).total_seconds() // 60)
    if elapsed_minutes >= 5:
        blocks = elapsed_minutes // 5
        for _ in range(blocks):
            for p in FAKE_WEEKLY_PLAYERS:
                if random.random() > p.get("active_chance", 0.7):
                    continue
                base_inc = random.randint(200_000, 3_000_000)
                inc = int(base_inc * p.get("speed", 1.0))
                if random.random() < 0.08:
                    inc = int(inc * random.uniform(2.5, 5.0))
                p["total"] = min(p["total"] + inc, WEEK_MAX)
        FAKE_WEEK_LAST_UPDATE = now

    FAKE_WEEKLY_PLAYERS.sort(key=lambda x: x["total"], reverse=True)
    return FAKE_WEEKLY_PLAYERS



async def show_rank_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db_b = get_bets_db()
    today = today_str()

    cur = db_b.execute(
        "SELECT user_id, total FROM bets_daily WHERE day = ?",
        (today,),
    )
    real_entries = [(r["user_id"], r["total"], False) for r in cur.fetchall() if r["total"] > 0]

    fake_entries = [(p["id"], p["total"], True) for p in get_fake_daily_players()]
    all_entries = real_entries + fake_entries
    if not all_entries:
        await update.effective_message.reply_text("Chưa có dữ liệu cược ngày hôm nay.")
        return

    all_entries.sort(key=lambda x: x[1], reverse=True)

    lines = [f"🏆  Top  cược ngày {today}\n", "TOP - ID - Tổng cược"]
    max_show = min(10, len(all_entries))
    for rank in range(1, max_show + 1):
        uid, total, is_fake = all_entries[rank - 1]
        id_str = mask_id(uid)
        lines.append(f"{rank} - {id_str} - {format_currency(total)}")

    await update.effective_message.reply_text("\n".join(lines))


async def show_rank_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db_b = get_bets_db()
    today = date.today()
    iso = today.isocalendar()
    wk_key = (iso.year, iso.week)

    cur = db_b.execute("SELECT user_id, day, total FROM bets_daily")
    totals = {}
    for r in cur.fetchall():
        d = date.fromisoformat(r["day"])
        if d.isocalendar()[:2] == wk_key:
            totals[r["user_id"]] = totals.get(r["user_id"], 0) + r["total"]
    real_entries = [(uid, total, False) for uid, total in totals.items() if total > 0]

    fake_entries = [(p["id"], p["total"], True) for p in get_fake_weekly_players()]
    all_entries = real_entries + fake_entries
    if not all_entries:
        await update.effective_message.reply_text("Chưa có dữ liệu cược tuần này.")
        return

    all_entries.sort(key=lambda x: x[1], reverse=True)

    lines = [
        f"🏆  Top  cược tuần {iso.week} năm {iso.year}\n",
        "TOP - ID       -       Tổng cược",
    ]
    max_show = min(10, len(all_entries))
    for rank in range(1, max_show + 1):
        uid, total, is_fake = all_entries[rank - 1]
        id_str = mask_id(uid)
        lines.append(f"{rank} - {id_str} - {format_currency(total)}")

    await update.effective_message.reply_text("\n".join(lines))


# ================== VIP: /doidiemvip ==================


async def doidiemvip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or user.first_name)

    if not context.args:
        await update.message.reply_text("Vui lòng nhập số điểm cần đổi. VD: /doidiemvip 100")
        return

    arg = context.args[0]
    if not arg.isdigit():
        await update.message.reply_text("Số điểm không hợp lệ.")
        return

    points_to_use = int(arg)
    if points_to_use <= 0:
        await update.message.reply_text("Số điểm phải lớn hơn 0.")
        return

    avail = get_available_vip_points(user.id)
    if points_to_use > avail:
        await update.message.reply_text(
            f"Bạn không đủ điểm VIP để đổi. Điểm khả dụng: {avail}"
        )
        return

    total_int, spent_int = get_vip_total_and_spent(user.id)
    rate = calculate_vip_exchange_rate(total_int)
    money = points_to_use * rate

    db_p = get_players_db()
    db_p.execute(
        "UPDATE users SET vip_points_spent = vip_points_spent + ?, balance = balance + ? WHERE user_id = ?",
        (points_to_use, money, user.id),
    )
    db_p.commit()

    avail_after = get_available_vip_points(user.id)
    text = (
        "✅ Đổi điểm VIP thành công!\n"
        f"💎 Số điểm đã đổi: {points_to_use}\n"
        f"💰 Số tiền nhận được: {format_currency(money)}\n"
        f"💎 Điểm VIP còn lại: {avail_after}"
    )
    await update.message.reply_text(text)


# ================== GIFTCODE ==================


async def setcode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Bạn không có quyền sử dụng lệnh này.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("Cú pháp: /setcode MACODE SOTIEN")
        return

    code = context.args[0].upper()
    digits = "".join(ch for ch in context.args[1] if ch.isdigit())
    if not digits:
        await update.message.reply_text("Số tiền không hợp lệ.")
        return
    amount = int(digits)

    db_p = get_players_db()
    db_p.execute(
        "INSERT OR REPLACE INTO giftcodes (code, amount, used) VALUES (?, ?, 0)",
        (code, amount),
    )
    db_p.commit()

    await update.message.reply_text(
        f"Đã tạo giftcode {code} với giá trị {format_currency(amount)}."
    )



async def code_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or user.first_name)

    if not context.args:
        await update.message.reply_text("Cú pháp: /code MAGIFTCODE")
        return

    code = context.args[0].upper()
    db_p = get_players_db()

    # Lấy thông tin user
    cur_u = db_p.execute("SELECT * FROM users WHERE user_id = ?", (user.id,))
    user_row = cur_u.fetchone()

    # ƯU TIÊN XỬ LÝ CODE TÂN THỦ RIÊNG CỦA USER
    user_newbie_code = user_row["newbie_code"] if user_row else None
    if user_newbie_code and code == user_newbie_code.upper():
        if user_row and user_row["used_newbie_code"]:
            await update.message.reply_text("❌ Bạn đã sử dụng code tân thủ trước đó rồi.")
            return

        total_deposit = user_row["total_deposit"] if user_row else 0
        if total_deposit < REQUIRE_DEPOSIT_FOR_NEWBIE:
            msg = (
                "❌ BẠN CHƯA ĐỦ ĐIỀU KIỆN SỬ DỤNG CODE TÂN THỦ ❌\n\n"
                f"🎁 Mệnh giá code: {format_currency(NEWBIE_CODE_VALUE)}\n"
                f"💳 Yêu cầu nạp tối thiểu {format_currency(REQUIRE_DEPOSIT_FOR_NEWBIE)} để sử dụng code\n\n"
                "📞 Vui lòng liên hệ CSKH để biết thêm chi tiết."
            )
            await update.message.reply_text(msg)
            return

        bonus = NEWBIE_CODE_VALUE
        extra_wager = bonus * 3

        db_p.execute(
            "UPDATE users SET balance = balance + ?, wager_required = wager_required + ?, used_newbie_code = 1 WHERE user_id = ?",
            (bonus, extra_wager, user.id),
        )
        db_p.commit()

        await update.message.reply_text(
            "✅ Bạn đã sử dụng thành công Code Tân Thủ!\n"
            f"💰 Nhận ngay: {format_currency(bonus)} vào tài khoản.\n"
            f"🎯 Tiền thưởng cần quay 3 vòng cược ({format_currency(extra_wager)}) mới có thể rút."
        )
        return

    # CÁC GIFTCODE THƯỜNG
    cur = db_p.execute("SELECT * FROM giftcodes WHERE code = ?", (code,))
    info = cur.fetchone()
    if not info:
        await update.message.reply_text("Giftcode không hợp lệ.")
        return
    if info["used"]:
        await update.message.reply_text("Giftcode này đã được sử dụng.")
        return

    amount = info["amount"]
    db_p.execute(
        "UPDATE giftcodes SET used = 1 WHERE code = ?",
        (code,),
    )
    db_p.execute(
        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
        (amount, user.id),
    )
    db_p.commit()

    await update.message.reply_text(
        f"✅ Nhập giftcode thành công!\n💰 Bạn nhận được: {format_currency(amount)}."
    )



# ================== ADMIN: /kt, /ktall, /thongbao ==================



async def kt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Bạn không có quyền sử dụng lệnh này.")
        return

    if not context.args:
        await update.message.reply_text("Cú pháp: /kt ID_NGUOI_CHOI")
        return

    id_str = context.args[0]
    if not id_str.isdigit():
        await update.message.reply_text("ID không hợp lệ.")
        return

    target_id = int(id_str)
    db_p = get_players_db()
    db_b = get_bets_db()

    cur = db_p.execute("SELECT * FROM users WHERE user_id = ?", (target_id,))
    row = cur.fetchone()
    if not row:
        await update.message.reply_text("Không tìm thấy người chơi với ID này.")
        return

    balance = row["balance"]

    # Vòng cược
    wager_required = row["wager_required"] or 0
    wager_done = row["wager_done"] or 0
    wager_remaining = max(0, wager_required - wager_done)

    if wager_required <= 0:
        wager_status = "✅ Không yêu cầu vòng cược."
    elif wager_done >= wager_required:
        wager_status = "✅ ĐÃ ĐỦ điều kiện vòng cược để rút."
    else:
        wager_status = "⛔ CHƯA ĐỦ vòng cược để rút."

    vip_total_int, vip_spent_int = get_vip_total_and_spent(target_id)
    level, symbol, next_req = get_vip_level_and_symbol(vip_total_int)
    progress_text = f"{vip_total_int}/{next_req}" if next_req else f"{vip_total_int}/MAX"

    today_count, today_total = get_today_bet_stats(target_id)
    week_total = sum_week_bets(target_id)
    month_total = sum_month_bets(target_id)

    cur2 = db_p.execute(
        "SELECT COUNT(*) AS c FROM disciples WHERE referrer_id = ?",
        (target_id,),
    )
    disciples = cur2.fetchone()["c"]
    cur3 = db_b.execute(
        "SELECT SUM(amount) AS s FROM commissions_daily WHERE user_id = ?",
        (target_id,),
    )
    total_comm = cur3.fetchone()["s"] or 0

    text = (
        "👤 Thông tin người chơi\n\n"
        f"🧾 ID: {target_id}\n"
        f"💰 Số dư: {format_currency(balance)}\n"
        f"👑 Cấp VIP: {level} {symbol}\n"
        f"💎 Điểm VIP hiện có: {vip_total_int}\n"
        f"🚀 Tiến trình: {progress_text}\n"
        f"✋ Điểm VIP đã sử dụng: {vip_spent_int}\n\n"
        "🎯 Vòng cược:\n"
        f"- Đã cược: {format_currency(wager_done)}\n"
        f"- Yêu cầu: {format_currency(wager_required)}\n"
        f"- Còn thiếu: {format_currency(wager_remaining)}\n"
        f"{wager_status}\n\n"
        "🎮 Thống kê cược:\n"
        f"- Hôm nay: {today_count} lượt / {format_currency(today_total)}\n"
        f"- Tuần này: {format_currency(week_total)}\n"
        f"- Tháng này: {format_currency(month_total)}\n\n"
        "🫂 Giới thiệu:\n"
        f"- Số đệ tử: {disciples}\n"
        f"- Tổng hoa hồng đã nhận: {format_currency(total_comm)}"
    )
    await update.message.reply_text(text)


async def ktall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Bạn không có quyền sử dụng lệnh này.")
        return

    db_p = get_players_db()
    db_b = get_bets_db()

    cur = db_p.execute("SELECT user_id, balance FROM users")
    users = cur.fetchall()

    cur2 = db_b.execute(
        "SELECT user_id, SUM(total) AS total_bet FROM bets_daily GROUP BY user_id"
    )
    bet_map = {r["user_id"]: r["total_bet"] for r in cur2.fetchall() if r["total_bet"]}

    total_users = len(users)
    total_balance = sum(r["balance"] for r in users)
    total_bets = sum(bet_map.values())

    lines = [
        "📊 Tóm tắt tất cả người chơi\n",
        f"👥 Tổng số người chơi: {total_users}",
        f"💰 Tổng số dư: {format_currency(total_balance)}",
        f"🎮 Tổng tiền đã cược (all): {format_currency(total_bets)}",
        "",
        "Danh sách (ID - Số dư - Tổng cược):",
    ]
    for r in users[:200]:
        uid = r["user_id"]
        bal = r["balance"]
        tbet = bet_map.get(uid, 0)
        lines.append(
            f"{uid} - {format_currency(bal)} - {format_currency(tbet)}"
        )

    await update.message.reply_text("\n".join(lines))


async def thongbao_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Bạn không có quyền sử dụng lệnh này.")
        return

    if not context.args:
        await update.message.reply_text("Cú pháp: /thongbao NỘI_DUNG_THÔNG_BÁO")
        return

    message = " ".join(context.args)

    db_p = get_players_db()
    cur = db_p.execute("SELECT user_id FROM users")
    user_ids = [r["user_id"] for r in cur.fetchall()]

    success = 0
    fail = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=message)
            success += 1
        except Exception:
            fail += 1

    summary = (
        "📢 Kết quả gửi thông báo:\n\n"
        f"✅ Gửi thành công: {success} người\n"
        f"❌ Gửi thất bại: {fail} người\n"
        f"👥 Tổng số user: {len(user_ids)}"
    )
    await update.message.reply_text(summary)


# ================== CALLBACK ROUTER ==================


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data == "start_playing":
        await handle_start_playing_callback(update, context)
        return

    if data == "game_taixiu":
        await show_taixiu_info(update, context)
        return
    if data == "game_xucxac":
        await show_xucxac_info(update, context)
        return
    if data == "game_bowling":
        await show_bowling_info(update, context)
        return
    if data == "game_lode":
        await show_lode_info(update, context)
        return
    if data == "game_bongro":
        await show_bongro_info(update, context)
        return
    if data == "rebet":
        await handle_rebet(update, context)
        return

    if data.startswith("acc_"):
        await handle_account_callbacks(update, context, data)
        return

    if data.startswith("quick_deposit:"):
        amount_str = data.split(":", 1)[1]
        await handle_quick_deposit(query, context, amount_str)
        return

    if data == "wdr_confirm":
        await handle_withdraw_confirm(update, context)
        return

    if data == "wdr_cancel":
        await handle_withdraw_cancel(update, context)
        return

    if data.startswith("dep:"):
        try:
            _, s_id, action = data.split(":")
            dep_id = int(s_id)
            await process_deposit_callback(update, context, dep_id, action)
        except Exception:
            await query.message.reply_text("Lỗi xử lý lệnh nạp.")
        return

    if data.startswith("wdr:"):
        try:
            _, s_id, action = data.split(":")
            wdr_id = int(s_id)
            await process_withdraw_callback(update, context, wdr_id, action)
        except Exception:
            await query.message.reply_text("Lỗi xử lý lệnh rút.")
        return

    if data == "rank_day":
        await show_rank_day(update, context)
        return

    if data == "rank_week":
        await show_rank_week(update, context)
        return


    if data == "promo:newbie_code":
        user = query.from_user
        user_row = ensure_user(user.id, user.username or user.first_name)
        user_code = user_row["newbie_code"] or "N/A"
        used = user_row["used_newbie_code"]
        status = "✅ Đã sử dụng" if used else "⏳ Chưa sử dụng"
        
        msg = (
            "🎉 Code Tân Thủ của bạn:\n\n"
            f"🎁 Code: `{user_code}`\n"
            f"💵 Mệnh giá: {format_currency(NEWBIE_CODE_VALUE)}\n"
            f"⭐️ Trạng Thái: {status}\n\n"
            "📋 Cách sử dụng:\n"
            "• Nhấn giữ vào code để copy\n"
            "• Nhập lệnh: /code [mã code]\n\n"
            "🔹 Lưu ý:\n"
            "• Mỗi tài khoản chỉ được nhập một lần code tân thủ.\n"
            "• Gian lận, tạo nhiều tài khoản lạm dụng code sẽ bị xử lý theo quy định NPH.\n\n"
            f"💡 Lưu ý: Code chỉ sử dụng được khi bạn đã nạp tối thiểu {format_currency(REQUIRE_DEPOSIT_FOR_NEWBIE)}."
        )
        await query.message.reply_text(msg, parse_mode="Markdown")
        return

    if data == "promo_first_deposit":
        await query.answer()
        text = (
            "🎁 KHUYẾN MÃI NẠP ĐẦU 🎁\n\n"
            "📋 Bảng khuyến mãi nạp đầu (K = 1.000đ):\n"
            "SỐ TIỀN NẠP ĐẦU  ➝  TIỀN THƯỞNG\n"
            
            "100K      ➝  88K\n"
            "200K      ➝  188K\n"
            "500K      ➝  228K\n"
            "1000K     ➝  288K\n"
            "3000K     ➝  388K\n"
            "5000K     ➝  488K\n"
            "10000K    ➝  888K\n"
            "20000K    ➝  1888K\n"
            "50000K    ➝  3888K\n"
            "100000K   ➝  8888K\n\n"
            "⚠️ Lưu ý:\n"
            "- Áp dụng cho NẠP ĐẦU theo quy định của NPH.\n"
            "- Tiền thưởng sẽ được cộng sau khi hệ thống/admin kiểm tra.\n"
            "- Vui lòng liên hệ CSKH/admin để biết mức đang áp dụng.\n\n"
            "👆 Bấm nút bên dưới để hệ thống tự kiểm tra và cộng khuyến mãi nạp đầu (nếu đủ điều kiện)."
        )
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅ Check khuyến mãi nạp đầu", callback_data="promo_check_first_deposit")]]
        )
        await query.message.reply_text(text, reply_markup=keyboard)
        return

    if data == "promo_check_first_deposit":
        await query.answer()
        user = query.from_user

        ensure_user(user.id, user.username or user.first_name)
        db_p = get_players_db()
        db_f = get_finance_db()

        # Lấy thông tin user
        cur = db_p.execute("SELECT * FROM users WHERE user_id = ?", (user.id,))
        row = cur.fetchone()
        if not row:
            await query.message.reply_text("Không tìm thấy tài khoản của bạn trong hệ thống.")
            return

        # Đã nhận KM nạp đầu trước đó
        if row["first_deposit_bonus_used"]:
            msg = (
                "✅ Bạn đã nhận khuyến mãi nạp đầu trước đó rồi.\n"
                "Nếu có thắc mắc, vui lòng liên hệ CSKH."
            )
            await query.message.reply_text(msg)
            return

        # Tìm lệnh nạp đầu được duyệt
        cur_dep = db_f.execute(
            "SELECT amount FROM deposits "
            "WHERE user_id = ? AND status = 'approved' "
            "ORDER BY id ASC LIMIT 1",
            (user.id,),
        )
        dep = cur_dep.fetchone()

        if not dep:
            msg = (
                "❌ Bạn chưa có lệnh NẠP ĐẦU nào được duyệt.\n"
                "Vui lòng nạp theo các mốc trong bảng khuyến mãi để nhận thưởng."
            )
            await query.message.reply_text(msg)
            return

        first_amount = dep["amount"]

        # Check trong bảng KM
        bonus = FIRST_DEPOSIT_PROMO_TABLE.get(first_amount)
        if not bonus:
            msg = (
                "❌ Bạn chưa đủ điều kiện nhận khuyến mãi nạp đầu.\n\n"
                "⚠️ Điều kiện: Lệnh nạp đầu phải nằm trong các mốc của bảng khuyến mãi.\n"
                "Nếu bạn đã nạp đúng mốc mà chưa được cộng, vui lòng liên hệ CSKH."
            )
            await query.message.reply_text(msg)
            return

        # Cộng thưởng + cộng vòng cược x3
        extra_wager = bonus * 3

        db_p.execute(
            """UPDATE users
            SET first_deposit_bonus_used = 1,
                first_deposit_bonus_amount = ?,
                wager_required = wager_required + ?
            WHERE user_id = ?""",
            (bonus, extra_wager, user.id),
        )
        db_p.commit()

        change_balance(user.id, bonus)

        # Lấy số dư mới
        cur2 = db_p.execute("SELECT balance FROM users WHERE user_id = ?", (user.id,))
        bal = cur2.fetchone()["balance"]

        msg = (
            "✅ HỆ THỐNG ĐÃ CỘNG KHUYẾN MÃI NẠP ĐẦU CHO BẠN!\n\n"
            f"💰 Số tiền nạp đầu: {format_currency(first_amount)}\n"
            f"🎁 Tiền khuyến mãi: {format_currency(bonus)}\n"
            f"🎯 Vòng cược yêu cầu từ thưởng: {format_currency(extra_wager)} (x3 tiền thưởng)\n"
            f"💳 Số dư hiện tại: {format_currency(bal)}\n\n"
            "Lưu ý: Bạn cần hoàn thành đủ vòng cược yêu cầu mới có thể rút tiền.\n"
            "Chúc bạn chơi game vui vẻ và may mắn!"
        )
        await query.message.reply_text(msg)
        return

    try:
        await query.answer()
    except Exception:
        pass


# ================== MAIN ==================


def main():
    if BOT_TOKEN == "PUT-YOUR-TOKEN-HERE" or not BOT_TOKEN:
        raise RuntimeError("Bạn quên chưa đặt BOT_TOKEN trong biến môi trường!")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("napbank", napbank_command))
    app.add_handler(CommandHandler("rutbank", rutbank_command))
    app.add_handler(CommandHandler("doidiemvip", doidiemvip_command))
    app.add_handler(CommandHandler("setcode", setcode_command))
    app.add_handler(CommandHandler("code", code_command))
    app.add_handler(CommandHandler("kt", kt_command))
    app.add_handler(CommandHandler("ktall", ktall_command))
    app.add_handler(CommandHandler("thongbao", thongbao_command))

    app.add_handler(CallbackQueryHandler(callback_router))

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_main_menu_text,
        )
    )

    app.run_polling()


if __name__ == "__main__":
    main()
