"""
Security module for MSISDN Checker
Handles authentication, authorization, and audit logging
"""
import sqlite3
import threading
from datetime import datetime, timedelta
from werkzeug.security import check_password_hash, generate_password_hash
from functools import wraps
from flask import session, request, redirect, url_for, flash
import logging

# Configuration
DB_FILE = "users.db"
MAX_FAILED_ATTEMPTS = 5
LOCKOUT_DURATION = 15  # minutes
LOG_RETENTION_DAYS = 180  # 6 months

db_lock = threading.Lock()
audit_logger = logging.getLogger('audit')

def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

# ─────────────────────────────────────────────────────────────────────────────
# User Management
# ─────────────────────────────────────────────────────────────────────────────

def get_user(username):
    with db_lock:
        conn = get_db()
        user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        conn.close()
        return dict(user) if user else None

def get_all_users():
    with db_lock:
        conn = get_db()
        users = conn.execute('SELECT username, role, enabled, created_at, last_login, failed_attempts, locked_until FROM users ORDER BY username').fetchall()
        conn.close()
        return [dict(u) for u in users]

def create_user(username, password, role='viewer'):
    with db_lock:
        conn = get_db()
        try:
            conn.execute('INSERT INTO users (username, password, role, enabled) VALUES (?, ?, ?, 1)',
                        (username, generate_password_hash(password), role))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

def update_user_password(username, password):
    with db_lock:
        conn = get_db()
        conn.execute('UPDATE users SET password = ? WHERE username = ?',
                    (generate_password_hash(password), username))
        conn.commit()
        conn.close()

def toggle_user_status(username):
    with db_lock:
        conn = get_db()
        conn.execute('UPDATE users SET enabled = NOT enabled WHERE username = ?', (username,))
        conn.commit()
        user = conn.execute('SELECT enabled FROM users WHERE username = ?', (username,)).fetchone()
        conn.close()
        return user['enabled'] if user else None

def delete_user(username):
    with db_lock:
        conn = get_db()
        conn.execute('DELETE FROM users WHERE username = ?', (username,))
        conn.commit()
        conn.close()

def update_last_login(username):
    with db_lock:
        conn = get_db()
        conn.execute('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE username = ?', (username,))
        conn.commit()
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# Account Lockout
# ─────────────────────────────────────────────────────────────────────────────

def check_account_locked(username):
    """Check if account is locked. Returns (is_locked, minutes_remaining)"""
    with db_lock:
        conn = get_db()
        user = conn.execute('SELECT locked_until FROM users WHERE username = ?', (username,)).fetchone()
        conn.close()
        
        if user and user['locked_until']:
            locked_until = datetime.fromisoformat(user['locked_until'])
            now = datetime.now()
            if now < locked_until:
                remaining = int((locked_until - now).total_seconds() / 60) + 1
                return True, remaining
            else:
                reset_failed_attempts(username)
        return False, 0

def increment_failed_attempts(username):
    """Increment failed attempts and lock if threshold reached"""
    with db_lock:
        conn = get_db()
        conn.execute('UPDATE users SET failed_attempts = failed_attempts + 1 WHERE username = ?', (username,))
        user = conn.execute('SELECT failed_attempts FROM users WHERE username = ?', (username,)).fetchone()
        
        if user and user['failed_attempts'] >= MAX_FAILED_ATTEMPTS:
            lock_time = datetime.now() + timedelta(minutes=LOCKOUT_DURATION)
            conn.execute('UPDATE users SET locked_until = ? WHERE username = ?', (lock_time.isoformat(), username))
        
        conn.commit()
        conn.close()

def reset_failed_attempts(username):
    """Reset failed attempts and unlock account"""
    with db_lock:
        conn = get_db()
        conn.execute('UPDATE users SET failed_attempts = 0, locked_until = NULL WHERE username = ?', (username,))
        conn.commit()
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# Audit Logging
# ─────────────────────────────────────────────────────────────────────────────

def log_audit(action, username=None, details=None):
    """Log an action to the audit database"""
    try:
        with db_lock:
            conn = get_db()
            conn.execute('''INSERT INTO audit_logs (action, username, details, ip_address, user_agent)
                           VALUES (?, ?, ?, ?, ?)''',
                        (action, 
                         username or session.get('username', 'anonymous'),
                         details,
                         request.remote_addr if request else None,
                         request.user_agent.string[:200] if request and request.user_agent else None))
            conn.commit()
            conn.close()
        audit_logger.info(f"{action} | User: {username} | Details: {details} | IP: {request.remote_addr if request else 'N/A'}")
    except Exception as e:
        audit_logger.error(f"Failed to log audit: {e}")

def get_audit_logs(limit=500, username=None, action=None, date=None):
    """Get audit logs with optional filters"""
    with db_lock:
        conn = get_db()
        query = 'SELECT * FROM audit_logs WHERE 1=1'
        params = []
        
        if username:
            query += ' AND username LIKE ?'
            params.append(f'%{username}%')
        if action:
            query += ' AND action LIKE ?'
            params.append(f'%{action}%')
        if date:
            query += ' AND DATE(timestamp) = ?'
            params.append(date)
        
        query += ' ORDER BY timestamp DESC LIMIT ?'
        params.append(limit)
        
        logs = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(l) for l in logs]

def cleanup_old_logs():
    """Remove logs older than retention period"""
    with db_lock:
        conn = get_db()
        cutoff = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
        conn.execute('DELETE FROM audit_logs WHERE timestamp < ?', (cutoff.isoformat(),))
        conn.commit()
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# Decorators
# ─────────────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in') or session.get('role') not in ['admin', 'superadmin']:
            flash('Admin access required', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def superadmin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in') or session.get('role') != 'superadmin':
            flash('Superadmin access required', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function
