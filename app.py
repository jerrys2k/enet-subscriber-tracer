#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ENET MSISDN CHECKER - FLASK APPLICATION
Complete production-ready version with viewer lookup auditing
"""

import os
import logging
import re
import psycopg2
import csv
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    url_for,
    send_file,
    send_from_directory,
    Response,
    flash
)
import pandas as pd
import requests
import urllib3
from Tools.helpers import generate_dashboard, load_prefix_map
from Tools.helpers import get_msisdn_location
from flask import make_response, render_template
from weasyprint import HTML
from dotenv import load_dotenv
from security import (
    get_user, get_all_users, create_user, update_user_password,
    toggle_user_status, delete_user, update_last_login,
    check_account_locked, increment_failed_attempts, reset_failed_attempts,
    log_audit, get_audit_logs, cleanup_old_logs,
    login_required, admin_required, superadmin_required
)
from werkzeug.security import check_password_hash, generate_password_hash
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import string
import secrets
from werkzeug.utils import secure_filename
from Tools.helpers import get_msisdn_location, get_imsi_location
from Tools.sandvine_client import SandvineClient
from Tools.ept_loader import get_cell_details, get_azimuth_direction, get_ept_info, convert_ept_to_pickle, delete_old_ept, clear_cache as clear_ept_cache
import time

load_dotenv()

# ====================== CONFIGURATION ======================
class Config:
    DB_CONFIG = {
        "dbname": "tracedb",
        "user": "enet",
        "password": os.environ.get("DB_PASSWORD", "changeme"),
        "host": "localhost",
        "port": "5432"
    }
    LOG_DIR = "logs"
    LOG_FILE = "logs/app.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    VALID_PROVIDERS = {"enet", "gtt", "digicel"}

# ====================== LOGGING CONFIGURATION ======================
def setup_logging():
    """Configure logging for the application"""
    os.makedirs(Config.LOG_DIR, exist_ok=True)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s'
    )
    
    # Create handlers
    file_handler = logging.FileHandler(Config.LOG_FILE)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Create specific loggers
    app_logger = logging.getLogger('app')
    db_logger = logging.getLogger('db')
    api_logger = logging.getLogger('api')
    
    return app_logger, db_logger, api_logger

# Initialize loggers
app_logger, db_logger, api_logger = setup_logging()

# ====================== DATABASE CONFIGURATION ======================
def get_db_connection():
    """Create a database connection with retry logic"""
    for attempt in range(Config.MAX_RETRIES):
        try:
            conn = psycopg2.connect(**Config.DB_CONFIG)
            return conn
        except Exception as e:
            if attempt == Config.MAX_RETRIES - 1:
                db_logger.error(f"Failed to connect to database after {Config.MAX_RETRIES} attempts: {e}")
                raise
            db_logger.warning(f"Database connection failed (attempt {attempt + 1}): {e}")
            time.sleep(Config.RETRY_DELAY)
    
    raise ConnectionError("Failed to establish database connection")

def init_viewer_logs_db():
    """Initialize PostgreSQL database for viewer lookup logs"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS viewer_logs (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        username VARCHAR(50) NOT NULL,
                        number VARCHAR(15) NOT NULL,
                        from_provider VARCHAR(20),
                        to_provider VARCHAR(20),
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_viewer_logs_timestamp ON viewer_logs(timestamp);
                    CREATE INDEX IF NOT EXISTS idx_viewer_logs_username ON viewer_logs(username);
                """)
                conn.commit()
                db_logger.info("Viewer logs database initialized successfully")
    except Exception as e:
        db_logger.error(f"Failed to initialize viewer logs database: {e}")
        raise

# ====================== VIEWER LOGS FUNCTIONS ======================
def log_viewer_query(number, from_provider, to_provider):
    """Log a viewer's NMP query to the database"""
    if not session.get("role") == "viewer":
        app_logger.debug("Not a viewer, skipping viewer query logging")
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO viewer_logs (timestamp, username, number, from_provider, to_provider)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        datetime.now(timezone.utc),
                        session.get("username", "anonymous"),
                        number,
                        from_provider,
                        to_provider,
                    ),
                )
                conn.commit()
                app_logger.info(f"Logged viewer lookup: {number}")
    except Exception as e:
        app_logger.error(f"Failed to log viewer query: {e}")

def log_viewer_lookup(username, number, from_provider=None, to_provider=None):
    """Log a viewer lookup for audit trail"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO viewer_logs (timestamp, username, number, from_provider, to_provider)
                    VALUES (NOW(), %s, %s, %s, %s)
                    """,
                    (username, number, from_provider, to_provider),
                )
                conn.commit()
                app_logger.info(f"Audit: {username} looked up {number}")
    except Exception as e:
        app_logger.error(f"Failed to log viewer lookup: {e}")


def get_viewer_logs(limit=50):
    """Retrieve recent viewer lookup logs"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT timestamp, username, number, from_provider, to_provider
                    FROM viewer_logs
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                
                return [
                    {
                        "timestamp": row[0],
                        "username": row[1],
                        "number": row[2],
                        "from_provider": row[3],
                        "to_provider": row[4],
                    }
                    for row in rows
                ]
    except Exception as e:
        app_logger.error(f"Failed to retrieve viewer logs: {e}")
        return []

# ====================== DATABASE ACCESS ======================
def get_latest_traces(limit=20):
    """Retrieve latest trace records"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT msisdn, tower_name, enodeb_id, cell_id, lat, lon, timestamp, source
                    FROM latest_traces
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

                return [
                    {
                        "msisdn": row[0],
                        "tower_name": row[1],
                        "enodeb_id": row[2],
                        "cell_id": row[3],
                        "lat": row[4],
                        "lon": row[5],
                        "timestamp": row[6],
                        "source": row[7],
                    }
                    for row in rows
                ]
    except Exception as e:
        app_logger.error(f"Failed to retrieve latest traces: {e}")
        return []

# ====================== PROVIDER FORMATTER ======================
def format_provider_name(provider):
    """Standardize provider names and return (formatted_name, color_class)"""
    if not provider:
        return "Unknown", "gray-600"

    provider_lower = provider.lower()

    if "digicel" in provider_lower:
        return "Digicel", "digicel"
    elif provider_lower == "gtt":
        return "GTT", "gtt"
    elif provider_lower == "enet":
        return "ENet", "enet"
    else:
        return provider, "gray-600"

# ====================== VALID PREFIX PROVIDERS ======================
VALID_PROVIDERS = {"enet", "gtt", "digicel"}  # Add others if needed

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====================== APPLICATION SETUP ======================
app = Flask(__name__, template_folder="templates")
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")
app.config.update(
    TEMPLATES_AUTO_RELOAD=True,
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,
    STATIC_FOLDER="static",
    PREFERRED_URL_SCHEME="https",
)

# ✅ ENABLE CSRF PROTECTION
from flask_wtf import CSRFProtect

csrf = CSRFProtect(app)

# Session security settings
app.config['SESSION_COOKIE_SECURE'] = False  # Set True if using HTTPS
app.config['SESSION_COOKIE_HTTPONLY'] = True  # Prevent XSS access to cookies
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # CSRF protection
app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # 1 hour timeout

# Rate limiting
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# ====================== GLOBAL DATA STORES ======================
NMP_DATA = {}  # {number: {from, to, date, source}}
PREFIX_MAP = {}  # Will be loaded from helpers.py

# ====================== PROVIDER HELPER CLASS ======================
class ProviderHelper:
    """Handles provider code normalization and formatting"""

    @staticmethod
    def normalize(code):
        if not code:
            return "Unknown"
        code = str(code).upper()
        mapping = {
            "GTTG": "GTT",
            "ENTG": "ENet",
            "DIGG": "Digicel",
        }
        normalized = mapping.get(code, code)
        return normalized

    @staticmethod
    def get_original(number):
        """Match a number against known prefix ranges to determine original provider"""
        global PREFIX_MAP
        try:
            if not PREFIX_MAP:
                PREFIX_MAP = load_prefix_map()

            number_int = int(str(number)[-7:])  # Last 7 digits only

            for entry in PREFIX_MAP:
                if entry["start"] <= number_int <= entry["end"]:
                    return ProviderHelper.normalize(entry["provider"])

            return ProviderHelper.normalize("Unknown")
        except Exception as e:
            app_logger.warning(f"Prefix range lookup failed for {number}: {e}")
            return ProviderHelper.normalize("Unknown")

# ====================== DATA MANAGEMENT ======================
class DataManager:
    """Handles all data loading and persistence operations"""

    @staticmethod
    def load_nmp_master():
        """Load ported numbers from master spreadsheet"""
        try:
            df = pd.read_excel("data/nmp_master.xlsx")
            df["number"] = df["number"].astype(str).str.strip().str.replace(".0", "")

            for _, row in df.iterrows():
                NMP_DATA[row["number"]] = {
                    "from": ProviderHelper.normalize(row["from"]),
                    "to": ProviderHelper.normalize(row["to"]),
                    "date": str(row["date"]),
                    "source": row.get("source", "master"),
                }
            app_logger.info(f"Loaded {len(df)} NMP records from master file")

        except Exception as e:
            app_logger.error(f"Failed loading NMP master: {str(e)}")

    @staticmethod
    def append_port_record(msisdn, from_code, to_code, port_date):
        """Add new porting record to master file and memory"""
        try:
            number = str(msisdn)[-7:]  # Last 7 digits

            # Check for existing record
            existing = NMP_DATA.get(number)
            if existing and existing["to"] == to_code:
                return False

            # Add to master file
            new_record = pd.DataFrame(
                [
                    {
                        "number": number,
                        "from": from_code,
                        "to": to_code,
                        "date": port_date.split("T")[0],
                        "source": "live_api",
                    }
                ]
            )

            master_path = "data/nmp_master.xlsx"
            df = pd.read_excel(master_path)
            df = pd.concat([df, new_record], ignore_index=True)
            df.to_excel(master_path, index=False)

            # Update in-memory store
            NMP_DATA[number] = {
                "from": ProviderHelper.normalize(from_code),
                "to": ProviderHelper.normalize(to_code),
                "date": port_date.split("T")[0],
                "source": "live_api",
            }

            app_logger.info(f"Added new port record: {number} → {to_code}")
            return True

        except Exception as e:
            app_logger.error(f"Failed adding port record: {str(e)}")
            return False

# ====================== EXTERNAL API INTEGRATIONS ======================
class NPMApiClient:
    """Client for interacting with Number Portability API"""

    @staticmethod
    def query_number(number):
        """Query live NMP API for portability info"""
        try:
            response = requests.get(
                f"https://pxs-number-portability.enetworks.gy/np_api/info/{number}",
                auth=("jerry_singh", "UfjGvZEW7NF6CFcp"),
                headers={"accept": "application/json"},
                timeout=10,
                verify=False,
            )

            if response.status_code == 200:
                item = response.json()["soap:Envelope"]["soap:Body"][
                    "PortingActionResponse"
                ]["PortingActionResult"]["message"]["messagebody"]["item"]
                return {
                    "from": item.get("originalnetworkoperator", "UNKNOWN"),
                    "to": item.get("currentnetworkoperator", "UNKNOWN"),
                    "date": item.get("effectivesince", "")
                    .replace("T", " ")
                    .split("+")[0],
                    "source": "live_api",
                }
            app_logger.warning(f"NMP API returned {response.status_code}")

        except Exception as e:
            app_logger.error(f"NMP API query failed: {str(e)}")
        return None

class SalesforceClient:
    """Client for Salesforce integration"""

    BASE_URL = "https://search-warrant-exp-api-prd-l7jdmr.8cursc.usa-e2.cloudhub.io:443/v1"
    CREDENTIALS = {
        "client_id": "fe29dd90803344c19ee6f213dfa635e6",
        "client_secret": os.environ.get("SF_CLIENT_SECRET", ""),
    }

    @classmethod
    def get_customer_info(cls, msisdn):
        """Retrieve customer profile from Search Warrant API"""
        import uuid
        try:
            # Ensure msisdn has 592 prefix
            if not msisdn.startswith("592"):
                msisdn = f"592{msisdn}"
            
            headers = {
                **cls.CREDENTIALS,
                "x-correlation-id": str(uuid.uuid4()),
                "Accept": "application/json",
            }
            
            response = requests.get(
                f"{cls.BASE_URL}/customers/{msisdn}",
                headers=headers,
                timeout=10,
            )
            
            app_logger.debug(f"SF API response: {response.status_code}")
            
            if response.status_code == 404:
                return {"error": "Subscriber not found"}
            
            if response.status_code != 200:
                app_logger.error(f"SF API error: {response.status_code}")
                return {"error": f"API error ({response.status_code})"}
            
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            if 'json' not in content_type:
                app_logger.error(f"SF API returned non-JSON: {content_type}")
                return {"error": "API unavailable"}
            
            data = response.json()
            
            # Debug: log actual field names
            if isinstance(data, dict):
                app_logger.debug(f"SF API fields: {list(data.keys())[:20]}")
            
            # Handle nested records if present
            if isinstance(data, list) and len(data) > 0:
                data = data[0]
            elif isinstance(data, dict) and data.get("records"):
                data = data["records"][0]
            
            # Parse name field (may be "FirstName LastName" format)
            name = data.get("name") or ""
            name_parts = name.split(" ", 1) if name else ["", ""]
            first_name = name_parts[0] if len(name_parts) > 0 else ""
            last_name = name_parts[1] if len(name_parts) > 1 else ""
            
            return {
                "subscriberId": data.get("Id") or data.get("subscriberId"),
                "name": name,
                "first_name": first_name or data.get("FirstName") or data.get("firstName"),
                "last_name": last_name or data.get("LastName") or data.get("lastName"),
                "email": data.get("PersonEmail") or data.get("email"),
                "type": data.get("SubscriberType") or data.get("type"),
                "dob": data.get("PersonBirthdate") or data.get("dob"),
                "gender": data.get("Gender__pc") or data.get("gender"),
                "address": data.get("address") or f"{data.get('BillingStreet', '')}, {data.get('BillingCity', '')}, {data.get('BillingRegion', '')}".strip(", "),
                "files": data.get("files", []),
            }
        except Exception as e:
            app_logger.error(f"Salesforce lookup failed: {str(e)}")
            return {"error": "Service unavailable"}


# ====================== ROUTES ======================
@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    if session.get("role") == "admin":
        return redirect(url_for("admin_dashboard"))
    else:
        return redirect(url_for("public_checker"))

@app.route("/static/<path:filename>")
def static_files(filename):
    """Serve static files"""
    return send_from_directory(app.config["STATIC_FOLDER"], filename)

@app.route("/admin/upload", methods=["POST"])
def upload_numbers():
    """Handle number data uploads"""
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    if "file" not in request.files:
        return redirect(url_for("admin_dashboard", tab="upload", error="No file"))

    file = request.files["file"]
    if file.filename.endswith(".xlsx"):
        file.save("data/uploaded_numbers.xlsx")
        DataManager.load_nmp_master()
        return redirect(
            url_for("admin_dashboard", tab="upload", success="File uploaded")
        )

    return redirect(url_for("admin_dashboard", tab="upload", error="Invalid file type"))

@app.route("/admin/export")
def export_data():
    """Generate dashboard metrics export"""
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    stats = generate_dashboard()
    with pd.ExcelWriter("data/export.xlsx") as writer:
        pd.DataFrame(stats["daily"]).to_excel(writer, sheet_name="Daily Metrics")
        pd.DataFrame(stats["routes"]).to_excel(writer, sheet_name="Porting Routes")
        pd.DataFrame(stats["prefixes"]).to_excel(writer, sheet_name="Prefix Blocks")
        pd.DataFrame(stats["providers"]).to_excel(writer, sheet_name="Providers")
        if "metadata" in stats:
            pd.DataFrame([stats["metadata"]]).to_excel(writer, sheet_name="Metadata")

    return send_file("data/export.xlsx", as_attachment=True)

@app.route("/admin/prefix", methods=["POST"])
def upload_prefix_ranges():
    """Handle uploading and applying new prefix-to-provider mappings"""
    if not session.get("logged_in") or session.get("role") != "admin":
        return redirect(url_for("login"))

    if "file" not in request.files:
        return redirect(
            url_for("admin_dashboard", tab="prefix", upload_message="No file selected")
        )

    file = request.files["file"]
    if file.filename == "":
        return redirect(
            url_for("admin_dashboard", tab="prefix", upload_message="No file selected")
        )

    if not file.filename.lower().endswith(".csv"):
        return redirect(
            url_for(
                "admin_dashboard",
                tab="prefix",
                upload_message="Invalid file type (must be .csv)",
            )
        )

    try:
        filename = secure_filename(file.filename)
        upload_path = os.path.join("data", filename)
        os.makedirs("data", exist_ok=True)
        file.save(upload_path)

        if not is_valid_prefix_file(upload_path):
            os.remove(upload_path)
            return redirect(
                url_for(
                    "admin_dashboard",
                    tab="prefix",
                    upload_message="Invalid file format",
                )
            )

        final_path = os.path.join("data", "ranges.csv")
        if os.path.exists(final_path):
            os.remove(final_path)
        os.rename(upload_path, final_path)

        load_prefix_map(refresh_cache=True)

        return redirect(
            url_for(
                "admin_dashboard",
                tab="prefix",
                upload_message="✅ Prefix list successfully updated.",
            )
        )
    except Exception as e:
        app_logger.error(f"Prefix upload failed: {str(e)}", exc_info=True)
        if "upload_path" in locals() and os.path.exists(upload_path):
            os.remove(upload_path)
        return redirect(
            url_for(
                "admin_dashboard",
                tab="prefix",
                upload_message="❌ Failed to update prefix list: " + str(e),
            )
        )

def is_valid_prefix_file(filepath):
    """Validate uploaded prefix file"""
    try:
        with open(filepath, newline="") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=";")
            required_headers = {"RANGE_START", "RANGE_END", "NETWORK_OPERATOR_CD"}

            if not reader.fieldnames or not required_headers.issubset(
                set(reader.fieldnames)
            ):
                return False

            for i, row in enumerate(reader):
                if i >= 10:
                    break
                try:
                    start = int(row["RANGE_START"].strip())
                    end = int(row["RANGE_END"].strip())
                    if start > end:
                        return False

                    provider = row["NETWORK_OPERATOR_CD"].strip().lower()
                    if not provider or provider not in VALID_PROVIDERS:
                        return False
                except (ValueError, KeyError) as e:
                    return False

            return True
    except Exception as e:
        app_logger.error(f"Prefix file validation failed: {e}", exc_info=True)
        return False

@app.route("/pdf_report")
def pdf_report():
    number = request.args.get("number", "").strip()
    if not number or len(number) != 7:
        return "Invalid number", 400

    msisdn = f"592{number}"
    live_data = NPMApiClient.query_number(number)
    stored_data = NMP_DATA.get(number)
    customer_info = SalesforceClient.get_customer_info(msisdn)

    if live_data and number not in NMP_DATA:
        DataManager.append_port_record(
            msisdn, live_data["from"], live_data["to"], live_data["date"]
        )
        stored_data = NMP_DATA.get(number)

    if session.get("role") == "viewer" and session.get("username") and live_data:
        log_viewer_query(
            number, live_data.get("from", "UNKNOWN"), live_data.get("to", "UNKNOWN")
        )

    result = {
        "number": msisdn,
        "customer_info": customer_info,
        "porting_history": {
            "from": (
                ProviderHelper.normalize(stored_data["from"])
                if stored_data
                else "Unknown"
            ),
            "to": (
                ProviderHelper.normalize(stored_data["to"])
                if stored_data
                else "Unknown"
            ),
            "date": stored_data["date"] if stored_data else "—",
            "source": stored_data.get("source", "master") if stored_data else "—",
        },
        "provider_info": {
            "original": ProviderHelper.get_original(number),
            "current": ProviderHelper.normalize(
                live_data["to"] if live_data else stored_data.get("to", "Unknown")
            ),
        },
    }

    try:
        from_name, from_color = format_provider_name(result["porting_history"]["from"])
        to_name, to_color = format_provider_name(result["porting_history"]["to"])
        result["porting_history"]["from_formatted"] = {
            "name": from_name,
            "color": from_color,
        }
        result["porting_history"]["to_formatted"] = {"name": to_name, "color": to_color}

        original_name, original_color = format_provider_name(
            result["provider_info"]["original"]
        )
        current_name, current_color = format_provider_name(
            result["provider_info"]["current"]
        )
        result["provider_info"]["original_formatted"] = {
            "name": original_name,
            "color": original_color,
        }
        result["provider_info"]["current_formatted"] = {
            "name": current_name,
            "color": current_color,
        }

    except KeyError as e:
        app_logger.error(f"Error processing data for PDF: missing key {e}")
        return f"Error processing data: missing key {e}", 400

    html = render_template(
        "pdf_template.html",
        result=result,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    pdf = HTML(string=html, base_url=request.root_url).write_pdf()

    response = make_response(pdf)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = (
        f"inline; filename={msisdn}_porting_report.pdf"
    )
    return response

@app.route("/logout")
def logout():
    user = session.get("role", "unknown")
    session.clear()
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
@limiter.limit("5 per minute")
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "").strip()
        
        user = get_user(username)
        if user:
            # Check if account is locked
            is_locked, mins_remaining = check_account_locked(username)
            if is_locked:
                log_audit("LOGIN BLOCKED (LOCKED)", username)
                return render_template("login.html", error=f"Account locked. Try again in {mins_remaining} minute(s).")
            
            if not user.get('enabled', True):
                log_audit("LOGIN BLOCKED (DISABLED)", username)
                return render_template("login.html", error="Account is disabled. Contact admin.")
            
            if check_password_hash(user['password'], password):
                # Successful login
                reset_failed_attempts(username)
                session["logged_in"] = True
                session["role"] = user.get('role', 'viewer')
                session["username"] = username
                session.permanent = True
                update_last_login(username)
                log_audit("LOGIN SUCCESS", username)
                
                if user.get('role') == 'viewer':
                    return redirect(url_for("public_checker"))
                else:
                    return redirect(url_for("admin_dashboard"))
            else:
                increment_failed_attempts(username)
        
        log_audit("LOGIN FAILED", username)
        return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/check", methods=["GET", "POST"])
def public_checker():
    """Unified handler for MSISDN lookups (GET + POST) with porting and location info."""
    if not session.get("logged_in") or session.get("role") not in ["viewer", "admin"]:
        return redirect(url_for("login"))

    number = request.args.get("number", "").strip()
    msisdn = (
        request.form.get("msisdn", "").strip() if request.method == "POST" else None
    )
    result = None
    location = None
    location_error = None

    lookup_number = msisdn or number

    if lookup_number and len(lookup_number) in [7, 10]:
        if len(lookup_number) == 7:
            lookup_number = f"592{lookup_number}"

        core_number = lookup_number[-7:]
        live_data = NPMApiClient.query_number(core_number)
        stored_data = NMP_DATA.get(core_number)
        customer_info = SalesforceClient.get_customer_info(lookup_number)

        if live_data and core_number not in NMP_DATA:
            DataManager.append_port_record(
                lookup_number, live_data["from"], live_data["to"], live_data["date"]
            )
            stored_data = NMP_DATA.get(core_number)

        if session.get("role") == "viewer" and live_data:
            log_viewer_query(
                core_number,
                live_data.get("from", "UNKNOWN"),
                live_data.get("to", "UNKNOWN"),
            )

        result = {
            "number": lookup_number,
            "customer_info": customer_info,
            "porting_history": {
                "from": (
                    ProviderHelper.normalize(stored_data.get("from", "Unknown"))
                    if stored_data
                    else "Unknown"
                ),
                "to": (
                    ProviderHelper.normalize(stored_data.get("to", "Unknown"))
                    if stored_data
                    else "Unknown"
                ),
                "date": stored_data.get("date") if stored_data else "—",
                "source": stored_data.get("source", "master") if stored_data else "—",
            },
            "provider_info": {
                "original": ProviderHelper.get_original(core_number),
                "current": ProviderHelper.normalize(
                    live_data["to"]
                    if live_data
                    else stored_data.get("to", "Unknown") if stored_data else "Unknown"
                ),
            },
        }

        from_name, from_color = format_provider_name(result["porting_history"]["from"])
        to_name, to_color = format_provider_name(result["porting_history"]["to"])
        result["porting_history"]["from_formatted"] = {
            "name": from_name,
            "color": from_color,
        }
        result["porting_history"]["to_formatted"] = {"name": to_name, "color": to_color}

        original_name, original_color = format_provider_name(
            result["provider_info"]["original"]
        )
        current_name, current_color = format_provider_name(
            result["provider_info"]["current"]
        )
        result["provider_info"]["original_formatted"] = {
            "name": original_name,
            "color": original_color,
        }
        result["provider_info"]["current_formatted"] = {
            "name": current_name,
            "color": current_color,
        }

        # Add real-time location
        try:
            from Tools.helpers import (
                get_msisdn_location,
            )  # ensure it's the trace_db version

            location = get_msisdn_location(lookup_number)
            app_logger.info(f"Trace location for {lookup_number}: {location}")
        except Exception as e:
            location = None
            location_error = f"Location lookup failed: {e}"
            print(location_error)

    # Ensure variables are defined to avoid template crashes
    if location is None:
        location = {}
    if location_error is None:
        location_error = ""

    return render_template(
        "admin/dashboard.html",
        result=result,
        location=location,
        error=location_error,
        tab="lookup",
    )



def save_sandvine_session(session_data):
    """Save Sandvine session data for historical IMEI searches"""
    if not session_data:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sandvine_sessions 
                    (msisdn, imsi, imei, ip_address, enodeb_id, cell_id, device_vendor, device_name, rat_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    session_data.get('msisdn'),
                    session_data.get('imsi'),
                    session_data.get('imei'),
                    session_data.get('ip_address'),
                    session_data.get('enodeb_id'),
                    session_data.get('cell_id'),
                    session_data.get('device_vendor'),
                    session_data.get('device_name'),
                    session_data.get('rat_type')
                ))
                conn.commit()
    except Exception as e:
        app_logger.error(f"Failed to save Sandvine session: {e}")

def search_by_imei(imei):
    """Search for MSISDNs associated with an IMEI"""
    results = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Search in radius_matches (has IMEI from RADIUS logs)
                cur.execute("""
                    SELECT DISTINCT ON (msisdn) msisdn, imsi, enodeb_id, cell_id, timestamp
                    FROM radius_matches 
                    WHERE imei = %s OR imei LIKE %s
                    ORDER BY msisdn, timestamp DESC
                    LIMIT 50
                """, (imei, imei[:15] + '%'))
                rows = cur.fetchall()
                for row in rows:
                    results.append({
                        'msisdn': row[0],
                        'imsi': row[1],
                        'enodeb_id': row[2],
                        'cell_id': row[3],
                        'timestamp': row[4],
                        'source': 'radius'
                    })
                
                # Also search sandvine_sessions for additional data
                cur.execute("""
                    SELECT DISTINCT ON (msisdn) msisdn, imsi, enodeb_id, cell_id, timestamp,
                           device_vendor, device_name
                    FROM sandvine_sessions 
                    WHERE imei = %s
                    ORDER BY msisdn, timestamp DESC
                    LIMIT 50
                """, (imei,))
                rows = cur.fetchall()
                existing_msisdns = {r['msisdn'] for r in results}
                for row in rows:
                    if row[0] not in existing_msisdns:
                        results.append({
                            'msisdn': row[0],
                            'imsi': row[1],
                            'enodeb_id': row[2],
                            'cell_id': row[3],
                            'timestamp': row[4],
                            'device': f"{row[5] or ''} {row[6] or ''}".strip(),
                            'source': 'sandvine'
                        })
    except Exception as e:
        app_logger.error(f"IMEI search error: {e}")
    return results


@app.route("/admin")
def admin_dashboard():
    """
    Render the admin dashboard with support for:
    - MSISDN lookup (live + porting + customer)
    - IMSI lookup (LEA tower trace)
    - Metrics, Live NMP, Prefix Upload
    """
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    role = session.get("role", "viewer")
    tab = request.args.get("tab", "lookup")
    number = request.args.get("number", "").strip()
    imsi = request.args.get("imsi", "").strip()
    imei_search = request.args.get("imei", "").strip()

    # Initialize outputs
    result = None
    live_result = None
    viewer_logs = []
    location = None
    location_error = None
    pdf_report_available = False
    live_traces = get_latest_traces()

    dashboard_stats = {
        "daily": [],
        "routes": [],
        "prefixes": [],
        "providers": [],
        "last_7_days": [],
        "metadata": {},
    }

    # 📊 Metrics Tab
    if tab == "metrics":
        dashboard_stats = generate_dashboard()
        if "generated_at" in dashboard_stats.get("metadata", {}):
            dashboard_stats["metadata"]["cache_age"] = (
                datetime.now()
                - datetime.fromisoformat(dashboard_stats["metadata"]["generated_at"])
            ).seconds // 60

    # 📡 Live Tab
    elif tab == "live" and role in ["admin", "superadmin"]:
        if number and len(number) == 7:
            live_result = NPMApiClient.query_number(number)
        viewer_logs = get_viewer_logs()

    # 📱 IMEI Search
    imei_results = None
    if imei_search and len(imei_search) >= 14:
        imei_results = search_by_imei(imei_search)
        app_logger.info(f"IMEI search for {imei_search}: {len(imei_results)} results")
    
    # 📞 MSISDN Lookup
    elif tab == "lookup" and number:
        msisdn = f"592{number}"
        live_data = NPMApiClient.query_number(number)
        stored_data = NMP_DATA.get(number)
        customer_info = SalesforceClient.get_customer_info(msisdn)

        if live_data and number not in NMP_DATA:
            DataManager.append_port_record(
                msisdn, live_data["from"], live_data["to"], live_data["date"]
            )
            stored_data = NMP_DATA.get(number)

        try:
            location = get_msisdn_location(msisdn)
            app_logger.info(f"Trace location for {msisdn}: {location}")
            pdf_report_available = True
        except Exception as e:
            location_error = f"Location service: {str(e)}"
            app_logger.error(location_error)

        # 🔴 LIVE SESSION from Sandvine
        live_session = None
        try:
            live_session = SandvineClient.get_live_session(msisdn)
            if "error" not in live_session:
                enodeb = live_session.get("enodeb_id")
                cell = live_session.get("cell_id")
                if enodeb and cell:
                    cell_info = get_cell_details(enodeb, cell)
                    if cell_info:
                        live_session["cell_details"] = cell_info
                        live_session["azimuth_direction"] = get_azimuth_direction(cell_info.get("azimuth"))
                app_logger.info(f"Sandvine: {msisdn} at {live_session.get('site_name')}")
            else:
                live_session = None
        except Exception as e:
            app_logger.warning(f"Sandvine failed: {e}")
            live_session = None

        # Log this lookup for audit trail
        username = session.get("username", "unknown")
        from_prov = stored_data.get("from") if stored_data else None
        to_prov = live_data["to"] if live_data else (stored_data.get("to") if stored_data else None)
        log_viewer_lookup(username, msisdn, from_prov, to_prov)
        
        result = {
            "number": msisdn,
            "original_provider": ProviderHelper.get_original(number),
            "current_provider": (
                ProviderHelper.normalize(live_data["to"])
                if live_data
                else stored_data.get("to", "Unknown") if stored_data else "Unknown"
            ),
            "customer_info": customer_info,
            "has_port_history": stored_data is not None,
            "porting_history": {
                "from": stored_data.get("from") if stored_data else "None",
                "to": stored_data.get("to") if stored_data else "None",
                "date": stored_data.get("date") if stored_data else "None",
            },
            "source": "live" if live_data else "cache",
            "location": location,
            "live_session": live_session,
        }

        # Save Sandvine session for IMEI search history
        if live_session:
            save_sandvine_session({
                'msisdn': msisdn,
                'imsi': live_session.get('imsi'),
                'imei': live_session.get('imei'),
                'ip_address': live_session.get('ip_address'),
                'enodeb_id': live_session.get('enodeb_id'),
                'cell_id': live_session.get('cell_id'),
                'device_vendor': live_session.get('device_vendor'),
                'device_name': live_session.get('device_name'),
                'rat_type': live_session.get('rat_type')
            })

        


    # 🕵️ IMSI Lookup (LEA Tower Trace)
    elif tab == "lookup" and imsi:
        try:
            location = get_imsi_location(imsi)
            pdf_report_available = True

            if location:
                msisdn = location.get("msisdn")
                if msisdn:
                    core_number = msisdn[-7:]
                    customer_info = SalesforceClient.get_customer_info(msisdn)
                    stored_data = NMP_DATA.get(core_number)

                    result = {
                        "number": msisdn,
                        "original_provider": ProviderHelper.get_original(core_number),
                        "current_provider": (
                            stored_data.get("to", "Unknown")
                            if stored_data
                            else "Unknown"
                        ),
                        "customer_info": customer_info,
                        "has_port_history": stored_data is not None,
                        "porting_history": {
                            "from": stored_data.get("from") if stored_data else "None",
                            "to": stored_data.get("to") if stored_data else "None",
                            "date": stored_data.get("date") if stored_data else "None",
                        },
                        "source": location.get("source", "trace_db"),
                        "location": location,
                    }
                else:
                    location_error = "MSISDN not found for this IMSI."
            else:
                location_error = "No match found for this IMSI."

        except Exception as e:
            location_error = f"IMSI lookup failed: {str(e)}"
            app_logger.error(location_error)

    # 🖼 Render Template
    app_logger.info(f"Live traces: {live_traces}")
    # Load users for users tab
    users = []
    if tab == "users" and role in ["admin", "superadmin"]:
        users = get_all_users()
        if role != "superadmin":
            users = [u for u in users if u.get("role") != "superadmin"]
    
    return render_template(
        "admin/dashboard.html",
        tab=tab,
        number=number,
        result=result,
        live_result=live_result,
        viewer_logs=viewer_logs,
        dashboard_stats=dashboard_stats,
        upload_message=None,
        location=location,
        location_error=location_error,
        pdf_report_available=pdf_report_available,
        live_traces=live_traces,
        users=users,
        timedelta=timedelta,
        imei_search=imei_search,
        imei_results=imei_results,
    )

@app.route("/export_kmz")
def export_kmz():
    from flask import send_file, request, flash
    import simplekml
    from geopy.distance import distance
    import os

    def create_sector(lat, lon, azimuth, beamwidth, radius_km):
        points = [(lon, lat)]
        start = azimuth - beamwidth / 2
        end = azimuth + beamwidth / 2
        step = max(1, int(beamwidth / 30))
        for angle in range(int(start), int(end) + 1, step):
            d = distance(kilometers=radius_km).destination((lat, lon), angle)
            points.append((d.longitude, d.latitude))
        points.append((lon, lat))
        return points

    msisdn = request.args.get("number")
    if not msisdn or not msisdn.isdigit():
        app_logger.warning(f"Invalid MSISDN provided for KMZ export: {msisdn}")
        return "Invalid MSISDN", 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT tower_name, lat, lon, enodeb_id, cell_id, timestamp
            FROM latest_traces
            WHERE msisdn = %s OR msisdn = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """,
            (msisdn, "592" + msisdn),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
    except Exception as e:
        db_logger.error(f"Failed to fetch trace data for KMZ export: {e}")
        return "Error fetching trace data", 500

    if not row:
        app_logger.warning(f"No trace data found for MSISDN: {msisdn}")
        return f"No trace data found for {msisdn}", 404

    tower, lat, lon, enodeb_id, cell_id, timestamp = row
    app_logger.info(f"Creating KMZ for MSISDN: {msisdn} at tower: {tower}")

    try:
        kml = simplekml.Kml()
        pnt = kml.newpoint(name=msisdn, coords=[(lon, lat)])
        pnt.description = (
            f"Tower: {tower}\neNodeB: {enodeb_id} | Cell: {cell_id}\nTime: {timestamp}"
        )
        pnt.style.iconstyle.icon.href = (
            "http://maps.google.com/mapfiles/kml/shapes/target.png"
        )
        pnt.style.labelstyle.scale = 1.2

        sector = kml.newpolygon(name=f"{tower} Sector")
        sector.outerboundaryis = create_sector(
            lat, lon, azimuth=0, beamwidth=120, radius_km=1.0
        )
        sector.style.polystyle.color = simplekml.Color.changealphaint(
            100, simplekml.Color.blue
        )
        sector.style.linestyle.color = simplekml.Color.white

        export_path = f"/tmp/trace_{msisdn}.kmz"
        kml.savekmz(export_path)
        app_logger.info(f"KMZ file created successfully at: {export_path}")

        return send_file(
            export_path,
            as_attachment=True,
            download_name=f"trace_{msisdn}.kmz",
            mimetype="application/vnd.google-earth.kmz"
        )
    except Exception as e:
        app_logger.error(f"Failed to create KMZ file: {e}")
        return "Error creating KMZ file", 500

# =========================
# /check route Flask logic
# =========================

@app.route("/check", methods=["GET", "POST"])
@login_required
def check():
    msisdn_result = None

    if request.method == "POST":
        msisdn = request.form.get("msisdn")

        if msisdn:
            try:
                # PostgreSQL connection
                import psycopg2

                conn = psycopg2.connect(
                    dbname=os.getenv("PG_DB", "msisdn_trace"),
                    user=os.getenv("PG_USER", "msisdn_user"),
                    password=os.getenv("PG_PASS", ""),
                    host=os.getenv("PG_HOST", "localhost"),
                    port=os.getenv("PG_PORT", "5432"),
                )
                cur = conn.cursor()

                cur.execute(
                    """
                    SELECT msisdn, imsi, enodeb_id, cell_id, tower_name, lat, lon, timestamp, device_model
                    FROM latest_traces
                    WHERE msisdn = %s
                """,
                    (msisdn,),
                )
                row = cur.fetchone()
                cur.close()
                conn.close()

                if row:
                    msisdn_result = {
                        "msisdn": row[0],
                        "imsi": row[1],
                        "enodeb_id": row[2],
                        "cell_id": row[3],
                        "tower_name": row[4],
                        "lat": row[5],
                        "lon": row[6],
                        "timestamp": row[7],
                        "device_model": row[8] or "Unknown Device",
                    }
                else:
                    flash("No record found for that MSISDN.", "warning")

            except Exception as e:
                flash(f"Error: {e}", "danger")

    return render_template("public/check.html", result=msisdn_result)

@app.route('/logs/stream')
def stream_logs():
    def generate():
        with open('logs/app.log') as f:
            # Go to the end of the file
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.5)
                    continue
                yield line
    return Response(generate(), mimetype='text/plain')

# ====================== INITIALIZATION ======================
def initialize_app():
    """Prepare application for first run"""
    # logging.debug("Initializing application")
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("static", exist_ok=True)
    init_viewer_logs_db()

    try:
        DataManager.load_nmp_master()
        global PREFIX_MAP
        PREFIX_MAP = load_prefix_map()
        app_logger.info("Application initialization complete")
    except Exception as e:
        app_logger.critical(f"Initialization failed: {str(e)}")
        raise


if __name__ == "__main__":
    initialize_app()
    # logging.info("Starting Flask application on port 7000")
    app.run(host="0.0.0.0", port=7000)  # Running on port 7000 for Gunicorn

# ─────────────────────────────────────────────────────────────────────────────
# 👥 User Management (Admin/Superadmin Only)
# ─────────────────────────────────────────────────────────────────────────────

def generate_complex_password(length=16):
    uppercase = string.ascii_uppercase
    lowercase = string.ascii_lowercase
    digits = string.digits
    symbols = "!@#$%^&*"
    password = [
        secrets.choice(uppercase), secrets.choice(uppercase),
        secrets.choice(lowercase), secrets.choice(lowercase),
        secrets.choice(digits), secrets.choice(digits),
        secrets.choice(symbols), secrets.choice(symbols),
    ]
    all_chars = uppercase + lowercase + digits + symbols
    password += [secrets.choice(all_chars) for _ in range(length - len(password))]
    secrets.SystemRandom().shuffle(password)
    return ''.join(password)

@app.route('/api/generate-password')
def api_generate_password():
    if not session.get('logged_in') or session.get('role') not in ['admin', 'superadmin']:
        return {"error": "Unauthorized"}, 403
    return {"password": generate_complex_password(16)}

@app.route('/users')
@admin_required
def manage_users():
    users = get_all_users()
    # Hide superadmin from regular admins
    if session.get('role') != 'superadmin':
        users = [u for u in users if u.get('role') != 'superadmin']
    users.sort(key=lambda x: (x['username'] != 'admin', x['username']))
    return render_template('users.html', users=users)

@app.route('/users/add', methods=['POST'])
@admin_required
def add_user_route():
    username = request.form.get('username', '').strip().lower()
    password = request.form.get('password', '')
    role = request.form.get('role', 'viewer')
    
    if not username or len(username) < 3:
        flash("Username must be at least 3 characters", "error")
        return redirect(url_for('manage_users'))
    
    if not password or len(password) < 12:
        flash("Password must be at least 12 characters", "error")
        return redirect(url_for('manage_users'))
    
    if create_user(username, password, role):
        log_audit("USER CREATED", session.get('username'), f"New user: {username}, Role: {role}")
        flash(f"User '{username}' created. Password: {password}", "success")
    else:
        flash(f"User '{username}' already exists", "error")
    
    return redirect(url_for('manage_users'))

@app.route('/users/toggle/<username>', methods=['POST'])
@admin_required
def toggle_user_route(username):
    if username in ['admin', 'superadmin']:
        flash("Cannot modify this account", "error")
        return redirect(url_for('manage_users'))
    
    target_user = get_user(username)
    if target_user and target_user.get('role') == 'admin' and session.get('role') != 'superadmin':
        flash("Only superadmin can modify admin accounts", "error")
        return redirect(url_for('manage_users'))
    
    new_status = toggle_user_status(username)
    if new_status is not None:
        status = "ENABLED" if new_status else "DISABLED"
        log_audit(f"USER {status}", session.get('username'), f"User: {username}")
        flash(f"User '{username}' {status.lower()}", "success")
    
    return redirect(url_for('manage_users'))

@app.route('/users/delete/<username>', methods=['POST'])
@admin_required
def delete_user_route(username):
    if username == 'superadmin':
        flash("Cannot delete superadmin", "error")
        return redirect(url_for('manage_users'))
    
    if username == 'admin' and session.get('role') != 'superadmin':
        flash("Only superadmin can delete admin", "error")
        return redirect(url_for('manage_users'))
    
    if username == session.get('username'):
        flash("Cannot delete yourself", "error")
        return redirect(url_for('manage_users'))
    
    delete_user(username)
    log_audit("USER DELETED", session.get('username'), f"Deleted: {username}")
    flash(f"User '{username}' deleted", "success")
    
    return redirect(url_for('manage_users'))

@app.route('/users/reset/<username>', methods=['POST'])
@admin_required  
def reset_password_route(username):
    if username == 'superadmin' and session.get('username') != 'superadmin':
        flash("Cannot reset superadmin password", "error")
        return redirect(url_for('manage_users'))
    
    target_user = get_user(username)
    if target_user and target_user.get('role') == 'admin' and session.get('role') != 'superadmin':
        flash("Only superadmin can reset admin password", "error")
        return redirect(url_for('manage_users'))
    
    new_password = request.form.get('new_password', '')
    if len(new_password) < 12:
        flash("Password must be at least 12 characters", "error")
        return redirect(url_for('manage_users'))
    
    update_user_password(username, new_password)
    log_audit("PASSWORD RESET", session.get('username'), f"User: {username}")
    flash(f"Password reset for '{username}'. New password: {new_password}", "success")
    
    return redirect(url_for('manage_users'))

@app.route('/users/unlock/<username>', methods=['POST'])
@admin_required
def unlock_user_route(username):
    reset_failed_attempts(username)
    log_audit("USER UNLOCKED", session.get('username'), f"User: {username}")
    flash(f"User '{username}' unlocked", "success")
    return redirect(url_for('manage_users'))

# ─────────────────────────────────────────────────────────────────────────────
# 📜 Audit Logs (Superadmin Only)
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/audit')
@superadmin_required
def view_audit():
    user_filter = request.args.get('user', '')
    action_filter = request.args.get('action', '')
    date_filter = request.args.get('date', '')
    
    logs = get_audit_logs(500, user_filter or None, action_filter or None, date_filter or None)
    
    return render_template('audit.html', logs=logs,
                          user_filter=user_filter,
                          action_filter=action_filter,
                          date_filter=date_filter)

@app.route('/health')
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}, 200

# ====================== EPT MANAGEMENT ======================
@app.route("/admin/ept", methods=["GET"])
@login_required
@admin_required
def ept_management():
    """EPT file management page"""
    ept_info = get_ept_info()
    error = request.args.get("error")
    success = request.args.get("success")
    return render_template("admin/ept.html", ept_info=ept_info, error=error, success=success)


@app.route("/admin/ept/upload", methods=["POST"])
@login_required
@admin_required
def upload_ept():
    """Upload new EPT file"""
    if "ept_file" not in request.files:
        return redirect(url_for("ept_management", error="No file selected"))
    
    file = request.files["ept_file"]
    if file.filename == "":
        return redirect(url_for("ept_management", error="No file selected"))
    
    if not file.filename.endswith(".xlsx"):
        return redirect(url_for("ept_management", error="Must be .xlsx file"))
    
    try:
        from werkzeug.utils import secure_filename
        filename = secure_filename(file.filename)
        filepath = os.path.join("data", filename)
        file.save(filepath)
        
        app_logger.info(f"EPT uploaded: {filename}")
        
        deleted = delete_old_ept()
        clear_ept_cache()
        convert_ept_to_pickle(filepath)
        
        log_audit("EPT UPLOAD", session.get("username"), f"Uploaded {filename}")
        
        return redirect(url_for("ept_management", success=f"EPT uploaded: {filename}"))
        
    except Exception as e:
        app_logger.error(f"EPT upload failed: {e}")
        return redirect(url_for("ept_management", error=f"Upload failed: {str(e)}"))


@app.route("/admin/ept/refresh", methods=["POST"])
@login_required
@admin_required  
def refresh_ept():
    """Re-convert current EPT to pickle"""
    try:
        clear_ept_cache()
        convert_ept_to_pickle()
        log_audit("EPT REFRESH", session.get("username"), "Cache refreshed")
        return redirect(url_for("ept_management", success="EPT cache refreshed"))
    except Exception as e:
        return redirect(url_for("ept_management", error=f"Refresh failed: {str(e)}"))


# ====================== LIVE SESSION API ======================
@app.route("/api/live-session/<msisdn>")
@login_required
def api_live_session(msisdn):
    """Get real-time session from Sandvine + EPT enrichment"""
    try:
        session_data = SandvineClient.get_live_session(msisdn)
        
        if "error" not in session_data:
            enodeb = session_data.get("enodeb_id")
            cell = session_data.get("cell_id")
            if enodeb and cell:
                cell_info = get_cell_details(enodeb, cell)
                if cell_info:
                    session_data["cell_details"] = cell_info
                    session_data["azimuth_direction"] = get_azimuth_direction(cell_info.get("azimuth"))
        
        return jsonify(session_data)
        
    except Exception as e:
        app_logger.error(f"Live session lookup failed: {e}")
        return jsonify({"error": str(e)}), 500

# ====================== CELL SECTOR MAP ======================
@app.route("/api/cell-map/<int:enodeb>/<int:cell>")
@login_required
def cell_sector_map(enodeb, cell):
    """Generate cell sector visualization data"""
    from Tools.ept_loader import get_cell_details, get_azimuth_direction
    
    cell_info = get_cell_details(enodeb, cell)
    if not cell_info:
        return jsonify({"error": "Cell not found"}), 404
    
    # Default beam width if not in EPT (typical LTE is 65-90 degrees)
    beam_width = 65
    cell_radius = 1.0  # km
    
    return jsonify({
        "lat": cell_info.get("latitude"),
        "lon": cell_info.get("longitude"),
        "azimuth": cell_info.get("azimuth"),
        "azimuth_direction": get_azimuth_direction(cell_info.get("azimuth")),
        "beam_width": beam_width,
        "radius_km": cell_radius,
        "cell_name": cell_info.get("cell_name"),
        "enodeb_name": cell_info.get("enodeb_name"),
        "technology": cell_info.get("technology"),
        "height": cell_info.get("height"),
    })
