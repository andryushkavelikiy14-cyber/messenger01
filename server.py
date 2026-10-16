import os
import re
import sqlite3
import secrets
import time
import base64
import random
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

from flask import Flask, request, jsonify, send_from_directory, g
from flask_socketio import SocketIO, emit, join_room
from flask_cors import CORS

# ==================================================
# КОНФИГ
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
DATABASE = os.path.join(BASE_DIR, 'messenger.db')
INDEX_HTML = os.path.join(BASE_DIR, 'index.html')
MAX_FILE_SIZE = 20 * 1024 * 1024

ALLOWED_EXTENSIONS = {
    'png','jpg','jpeg','gif','webp','svg','bmp',
    'mp4','webm','mov','avi','mkv',
    'mp3','wav','ogg','m4a',
    'pdf','doc','docx','xls','xlsx','ppt','pptx','txt','rtf',
    'zip','rar','7z','tar','gz',
    'crx','apk','exe','json','xml','csv'
}

USERNAME_REGEX = re.compile(r'^[a-zA-Z0-9]+$')
USERNAME_MIN = 2
USERNAME_MAX = 20
PASSWORD_MIN = 4
PASSWORD_MAX = 8

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(32)
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
CORS(app)

socketio = SocketIO(app, cors_allowed_origins='*', async_mode='threading',
                    max_http_buffer_size=MAX_FILE_SIZE,
                    ping_timeout=60, ping_interval=25)

active_sessions = {}
online_users = {}


# ==================================================
# БАЗА ДАННЫХ
# ==================================================
def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        avatar TEXT,
        created_at REAL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender TEXT NOT NULL,
        receiver TEXT NOT NULL,
        type TEXT NOT NULL,
        content TEXT,
        file_name TEXT,
        file_size INTEGER,
        duration REAL,
        timestamp REAL,
        read INTEGER DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS blocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        blocker TEXT NOT NULL,
        blocked TEXT NOT NULL,
        created_at REAL,
        UNIQUE(blocker, blocked)
    )''')

    # ---- Миграции: добавляем новые колонки в messages если их нет ----
    existing = [row[1] for row in c.execute('PRAGMA table_info(messages)').fetchall()]
    new_cols = [
        ('edited', 'INTEGER DEFAULT 0'),
        ('deleted', 'INTEGER DEFAULT 0'),
        ('reply_to_id', 'INTEGER'),
        ('reply_to_sender', 'TEXT'),
        ('reply_to_preview', 'TEXT'),
    ]
    for col_name, col_def in new_cols:
        if col_name not in existing:
            try:
                c.execute(f'ALTER TABLE messages ADD COLUMN {col_name} {col_def}')
                print(f'[migration] + messages.{col_name}')
            except Exception as e:
                print(f'[migration] failed {col_name}: {e}')

    c.execute('CREATE INDEX IF NOT EXISTS idx_msg_pair ON messages(sender, receiver)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_msg_time ON messages(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_blocks ON blocks(blocker, blocked)')

    conn.commit()
    conn.close()


# ---------- USERS ----------
def user_exists(username):
    return get_db().execute('SELECT 1 FROM users WHERE username = ?', (username,)).fetchone() is not None


def create_user(username, password, avatar):
    db = get_db()
    db.execute('INSERT INTO users (username, password_hash, avatar, created_at) VALUES (?, ?, ?, ?)',
               (username, generate_password_hash(password), avatar, time.time()))
    db.commit()


def get_user(username):
    return get_db().execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()


def get_all_users(exclude=None):
    db = get_db()
    if exclude:
        rows = db.execute('SELECT username, avatar FROM users WHERE username != ? ORDER BY username',
                          (exclude,)).fetchall()
    else:
        rows = db.execute('SELECT username, avatar FROM users ORDER BY username').fetchall()
    return [dict(r) for r in rows]


# ---------- MESSAGES ----------
def save_message(sender, receiver, msg_type, content, file_name=None, file_size=None,
                 duration=None, reply_to=None):
    db = get_db()
    ts = time.time()
    reply_id = reply_to.get('id') if reply_to else None
    reply_sender = reply_to.get('sender') if reply_to else None
    reply_preview = reply_to.get('preview') if reply_to else None

    cur = db.execute('''INSERT INTO messages
        (sender, receiver, type, content, file_name, file_size, duration, timestamp,
         reply_to_id, reply_to_sender, reply_to_preview)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (sender, receiver, msg_type, content, file_name, file_size, duration, ts,
         reply_id, reply_sender, reply_preview))
    db.commit()

    return {
        'id': cur.lastrowid,
        'sender': sender,
        'receiver': receiver,
        'type': msg_type,
        'content': content,
        'file_name': file_name,
        'file_size': file_size,
        'duration': duration,
        'timestamp': ts,
        'read': 0,
        'edited': 0,
        'reply_to': reply_to
    }


def get_messages(u1, u2, limit=300):
    rows = get_db().execute('''SELECT * FROM messages
        WHERE ((sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?))
        AND (deleted IS NULL OR deleted = 0)
        ORDER BY timestamp ASC LIMIT ?''', (u1, u2, u2, u1, limit)).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        if d.get('reply_to_id'):
            d['reply_to'] = {
                'id': d['reply_to_id'],
                'sender': d['reply_to_sender'],
                'preview': d['reply_to_preview']
            }
        result.append(d)
    return result


def mark_as_read(sender, receiver):
    db = get_db()
    db.execute('UPDATE messages SET read = 1 WHERE sender = ? AND receiver = ? AND read = 0',
               (sender, receiver))
    db.commit()


def edit_message(msg_id, sender, new_content):
    db = get_db()
    row = db.execute('SELECT * FROM messages WHERE id = ? AND sender = ?',
                     (msg_id, sender)).fetchone()
    if not row:
        return None
    if row['type'] != 'text':
        return None
    db.execute('UPDATE messages SET content = ?, edited = 1 WHERE id = ?', (new_content, msg_id))
    db.commit()
    return {'id': msg_id, 'content': new_content, 'edited': 1,
            'sender': row['sender'], 'receiver': row['receiver']}


def delete_message(msg_id, sender):
    db = get_db()
    row = db.execute('SELECT * FROM messages WHERE id = ? AND sender = ?',
                     (msg_id, sender)).fetchone()
    if not row:
        return None
    db.execute('UPDATE messages SET deleted = 1 WHERE id = ?', (msg_id,))
    db.commit()
    return {'id': msg_id, 'sender': row['sender'], 'receiver': row['receiver']}


# ---------- BLOCKS ----------
def is_blocked(blocker, blocked):
    row = get_db().execute('SELECT 1 FROM blocks WHERE blocker = ? AND blocked = ?',
                           (blocker, blocked)).fetchone()
    return row is not None


def add_block(blocker, blocked):
    db = get_db()
    try:
        db.execute('INSERT INTO blocks (blocker, blocked, created_at) VALUES (?, ?, ?)',
                   (blocker, blocked, time.time()))
        db.commit()
    except sqlite3.IntegrityError:
        pass


def remove_block(blocker, blocked):
    db = get_db()
    db.execute('DELETE FROM blocks WHERE blocker = ? AND blocked = ?', (blocker, blocked))
    db.commit()


def get_blocked_list(blocker):
    rows = get_db().execute('SELECT blocked FROM blocks WHERE blocker = ?', (blocker,)).fetchall()
    return [r['blocked'] for r in rows]


# ==================================================
# ДЕКОРАТОРЫ / УТИЛИТЫ
# ==================================================
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token or token not in active_sessions:
            return jsonify({'error': 'Не авторизован'}), 401
        g.current_user = active_sessions[token]
        return f(*args, **kwargs)
    return decorated


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def generate_default_avatar(letter):
    colors = ['#8b5cf6', '#ef4565', '#4dabf7', '#06d6a0', '#ffd43b', '#ff922b']
    color = random.choice(colors)
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100">
        <rect width="100" height="100" fill="{color}"/>
        <text x="50" y="68" font-size="50" text-anchor="middle" fill="#fff"
              font-family="Arial" font-weight="bold">{letter}</text>
    </svg>'''
    return 'data:image/svg+xml;base64,' + base64.b64encode(svg.encode()).decode()


# ==================================================
# HTTP API
# ==================================================
@app.route('/')
def index():
    try:
        with open(INDEX_HTML, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return '<h1>index.html not found</h1><p>Положи index.html рядом с server.py</p>', 404


@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json() or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    avatar = data.get('avatar') or ''

    if len(username) < USERNAME_MIN:
        return jsonify({'error': f'Логин мин. {USERNAME_MIN} символа'}), 400
    if len(username) > USERNAME_MAX:
        return jsonify({'error': f'Логин макс. {USERNAME_MAX}'}), 400
    if not USERNAME_REGEX.match(username):
        return jsonify({'error': 'Только a-z, A-Z и 0-9'}), 400
    if len(password) < PASSWORD_MIN:
        return jsonify({'error': f'Пароль мин. {PASSWORD_MIN}'}), 400
    if len(password) > PASSWORD_MAX:
        return jsonify({'error': f'Пароль макс. {PASSWORD_MAX}'}), 400
    if user_exists(username):
        return jsonify({'error': 'Такой логин занят'}), 400

    if not avatar or len(avatar) > 500_000:
        avatar = generate_default_avatar(username[0].upper())

    create_user(username, password, avatar)
    return jsonify({'ok': True})


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json() or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''

    if not username or not password:
        return jsonify({'error': 'Введите логин и пароль'}), 400

    user = get_user(username)
    if not user:
        return jsonify({'error': 'Пользователь не найден'}), 401
    if not check_password_hash(user['password_hash'], password):
        return jsonify({'error': 'Неверный пароль'}), 401

    token = secrets.token_urlsafe(32)
    active_sessions[token] = username
    return jsonify({
        'ok': True,
        'token': token,
        'user': {'username': user['username'], 'avatar': user['avatar']}
    })


@app.route('/api/me', methods=['GET'])
@token_required
def me():
    user = get_user(g.current_user)
    if not user:
        return jsonify({'error': 'Не найден'}), 404
    return jsonify({'username': user['username'], 'avatar': user['avatar']})


@app.route('/api/users', methods=['GET'])
@token_required
def users():
    return jsonify(get_all_users(exclude=g.current_user))


@app.route('/api/messages/<other>', methods=['GET'])
@token_required
def messages(other):
    msgs = get_messages(g.current_user, other)
    mark_as_read(other, g.current_user)
    return jsonify(msgs)


@app.route('/api/chats', methods=['GET'])
@token_required
def chats():
    db = get_db()
    me = g.current_user

    rows = db.execute('''
        SELECT
            CASE WHEN sender = ? THEN receiver ELSE sender END AS other,
            MAX(timestamp) as last_time
        FROM messages
        WHERE (sender = ? OR receiver = ?) AND (deleted IS NULL OR deleted = 0)
        GROUP BY other
        ORDER BY last_time DESC
    ''', (me, me, me)).fetchall()

    result = []
    for row in rows:
        other = row['other']
        u = get_user(other)
        if not u:
            continue

        last = db.execute('''
            SELECT * FROM messages
            WHERE ((sender = ? AND receiver = ?) OR (sender = ? AND receiver = ?))
            AND (deleted IS NULL OR deleted = 0)
            ORDER BY timestamp DESC LIMIT 1
        ''', (me, other, other, me)).fetchone()

        unread = db.execute('''
            SELECT COUNT(*) as cnt FROM messages
            WHERE sender = ? AND receiver = ? AND read = 0
            AND (deleted IS NULL OR deleted = 0)
        ''', (other, me)).fetchone()['cnt']

        last_dict = dict(last) if last else None
        if last_dict and last_dict.get('reply_to_id'):
            last_dict['reply_to'] = {
                'id': last_dict['reply_to_id'],
                'sender': last_dict['reply_to_sender'],
                'preview': last_dict['reply_to_preview']
            }

        result.append({
            'username': u['username'],
            'avatar': u['avatar'],
            'blocked': is_blocked(me, other),
            'unread': unread,
            'last_message': last_dict
        })
    return jsonify(result)


@app.route('/api/blocked', methods=['GET'])
@token_required
def blocked_list():
    return jsonify(get_blocked_list(g.current_user))


@app.route('/api/upload', methods=['POST'])
@token_required
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Нет файла'}), 400
    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'Пустое имя'}), 400
    if not allowed_file(file.filename):
        return jsonify({'error': 'Тип не разрешён'}), 400

    ext = file.filename.rsplit('.', 1)[1].lower()
    unique = f"{secrets.token_hex(16)}.{ext}"
    filepath = os.path.join(UPLOAD_FOLDER, unique)
    file.save(filepath)
    size = os.path.getsize(filepath)

    return jsonify({
        'url': f'/uploads/{unique}',
        'file_name': secure_filename(file.filename),
        'file_size': size
    })


@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'time': time.time()})


# ==================================================
# SOCKET.IO
# ==================================================
@socketio.on('connect')
def handle_connect():
    token = request.args.get('token', '')
    if token not in active_sessions:
        return False
    username = active_sessions[token]
    online_users[username] = request.sid
    join_room(f'user_{username}')
    emit('user_online', {'username': username}, broadcast=True)
    emit('online_list', list(online_users.keys()), broadcast=True)
    print(f'[+] {username} подключился')


@socketio.on('disconnect')
def handle_disconnect():
    username = None
    for uname, sid in list(online_users.items()):
        if sid == request.sid:
            username = uname
            del online_users[uname]
            break
    if username:
        emit('user_offline', {'username': username}, broadcast=True)
        emit('online_list', list(online_users.keys()), broadcast=True)
        print(f'[-] {username} отключился')


@socketio.on('send_message')
def handle_send_message(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    sender = active_sessions[token]
    receiver = data.get('to')
    if not receiver:
        return

    # Проверка блокировок в обе стороны
    if is_blocked(receiver, sender):
        emit('message_blocked', {'reason': 'Вы заблокированы получателем'}, room=f'user_{sender}')
        return
    if is_blocked(sender, receiver):
        emit('message_blocked', {'reason': 'Вы заблокировали этого пользователя'}, room=f'user_{sender}')
        return

    msg_type = data.get('type', 'text')
    content = data.get('content', '')
    file_name = data.get('file_name')
    file_size = data.get('file_size')
    duration = data.get('duration')
    reply_to = data.get('reply_to')

    msg = save_message(sender, receiver, msg_type, content,
                       file_name, file_size, duration, reply_to)

    emit('new_message', msg, room=f'user_{receiver}')
    emit('new_message', msg, room=f'user_{sender}')


@socketio.on('edit_message')
def handle_edit_message(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    sender = active_sessions[token]
    msg_id = data.get('msg_id')
    new_content = (data.get('content') or '').strip()
    if not msg_id or not new_content:
        return

    result = edit_message(msg_id, sender, new_content)
    if not result:
        return

    emit('message_edited', result, room=f'user_{sender}')
    emit('message_edited', result, room=f'user_{result["receiver"]}')


@socketio.on('delete_message')
def handle_delete_message(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    sender = active_sessions[token]
    msg_id = data.get('msg_id')
    if not msg_id:
        return

    result = delete_message(msg_id, sender)
    if not result:
        return

    emit('message_deleted', result, room=f'user_{sender}')
    emit('message_deleted', result, room=f'user_{result["receiver"]}')


@socketio.on('block_user')
def handle_block_user(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    blocker = active_sessions[token]
    blocked = data.get('user')
    if not blocked or blocked == blocker:
        return

    add_block(blocker, blocked)
    emit('user_blocked', {'by': blocker, 'user': blocked}, room=f'user_{blocker}')
    emit('you_are_blocked', {'by': blocker}, room=f'user_{blocked}')


@socketio.on('unblock_user')
def handle_unblock_user(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    blocker = active_sessions[token]
    blocked = data.get('user')
    if not blocked:
        return

    remove_block(blocker, blocked)
    emit('user_unblocked', {'by': blocker, 'user': blocked}, room=f'user_{blocker}')


@socketio.on('typing')
def handle_typing(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    sender = active_sessions[token]
    receiver = data.get('to')
    if receiver:
        emit('user_typing', {'from': sender}, room=f'user_{receiver}')


@socketio.on('stop_typing')
def handle_stop_typing(data):
    token = data.get('token', '')
    if token not in active_sessions:
        return
    sender = active_sessions[token]
    receiver = data.get('to')
    if receiver:
        emit('user_stop_typing', {'from': sender}, room=f'user_{receiver}')


# ==================================================
# ИНИЦИАЛИЗАЦИЯ (для Render)
# ==================================================
init_db()


# ==================================================
# ЗАПУСК
# ==================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))

    if 'PORT' not in os.environ:
        print('=' * 55)
        print('  🚀 МЕССЕНДЖЕР ЗАПУЩЕН')
        print('=' * 55)
        import socket as _s
        try:
            s = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            local_ip = '127.0.0.1'
        print(f'  💻 Локально:   http://127.0.0.1:{port}')
        print(f'  🌐 По сети:    http://{local_ip}:{port}')
        print('=' * 55)

    socketio.run(app, host='0.0.0.0', port=port,
                 debug=False, allow_unsafe_werkzeug=True)