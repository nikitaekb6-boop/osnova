import asyncio
import logging
import csv
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile


# --- Функция для экранирования Markdown ---
def escape_markdown(text):
    """Экранирует специальные символы для MarkdownV2"""
    if text is None:
        return ""
    
    # Проверяем, что text - это строка
    if not isinstance(text, str):
        text = str(text)
    
    # Экранируем все спецсимволы MarkdownV2
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    
    return text

# --- ФУНКЦИИ ДЛЯ ПОСТЕПЕННОГО УМЕНЬШЕНИЯ ФЕЙКОВОЙ ОЧЕРЕДИ ---

def decrease_fake_queue_gradually():
    """Постепенно уменьшить фейковую очередь"""
    current_fake = db.get_fake_queue()
    if current_fake > 0:
        # Уменьшаем на 1-3 номера (случайно)
        import random
        decrease_amount = random.randint(1, min(3, current_fake))
        new_value = max(0, current_fake - decrease_amount)
        db.set_fake_queue(new_value)
        return True, decrease_amount, new_value
    return False, 0, current_fake

def decrease_fake_queue_on_number_taken():
    """Уменьшить фейковую очередь при взятии реального номера"""
    current_fake = db.get_fake_queue()
    if current_fake > 0:
        # Получаем количество реальных номеров в очереди
        real_count = db.get_real_queue_count()
        
        # Если реальных номеров меньше 3, уменьшаем фейковую очередь медленнее
        if real_count < 3:
            # 50% шанс уменьшить на 1
            import random
            if random.random() < 0.5:
                new_value = max(0, current_fake - 1)
                db.set_fake_queue(new_value)
                return True, 1, new_value
        else:
            # Если реальных номеров много, уменьшаем быстрее
            new_value = max(0, current_fake - 1)
            db.set_fake_queue(new_value)
            return True, 1, new_value
    
    return False, 0, current_fake

def decrease_fake_queue_on_number_completion():
    """Уменьшить фейковую очередь при завершении номера"""
    current_fake = db.get_fake_queue()
    if current_fake > 0:
        # 30% шанс уменьшить на 1 при завершении номера
        import random
        if random.random() < 0.3:
            new_value = max(0, current_fake - 1)
            db.set_fake_queue(new_value)
            return True, 1, new_value
    
    return False, 0, current_fake

# --- КЛАСС БАЗЫ ДАННЫХ ---

class Database:
    def __init__(self, db_file):
        self.connection = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.create_tables()
        self.patch_database()

    def create_tables(self):
        with self.connection:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    balance REAL DEFAULT 0.0,
                    priority INTEGER DEFAULT 0,
                    total_numbers INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    referrer_id INTEGER DEFAULT NULL,
                    has_received_referral_bonus INTEGER DEFAULT 0,
                    referral_bonus_earned REAL DEFAULT 0.0
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tariffs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    price REAL,
                    duration_min INTEGER DEFAULT 25,
                    is_active INTEGER DEFAULT 1
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS numbers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    phone TEXT,
                    tariff_id INTEGER,
                    status TEXT DEFAULT 'Ожидание', 
                    is_priority INTEGER DEFAULT 0,
                    created_at TIMESTAMP,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    amount REAL,
                    status TEXT DEFAULT 'pending', -- pending, approved, rejected
                    payment_method TEXT,
                    payment_details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    admin_id INTEGER,
                    admin_comment TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY, 
                    value TEXT
                )
            """)
            # Настройки реферальной системы
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER,
                    referred_id INTEGER,
                    has_completed_first_number INTEGER DEFAULT 0,
                    bonus_paid INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (referrer_id) REFERENCES users (user_id),
                    FOREIGN KEY (referred_id) REFERENCES users (user_id)
                )
            """)
            # Таблица для скрытых настроек времени
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tariff_hidden_bonus (
                    tariff_id INTEGER PRIMARY KEY,
                    hidden_bonus_minutes INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tariff_id) REFERENCES tariffs (id)
                )
            """)
            
            # Инициализация настроек
            settings = [
                ('priority_price', '0.5'),
                ('priority_name', 'ПРИОРИТЕТ'),
                ('fake_queue', '0'),
                ('night_mode', '0'),
                ('weekend_mode', '0'),
                ('system_message', ''),
                ('min_withdrawal', '1.0'),
                ('payment_methods', 'CryptoBot'),
                ('referral_bonus', '0.5'),  # Бонус за реферала ($)
                ('referral_enabled', '1')   # Включена ли реферальная система
            ]
            
            for key, value in settings:
                self.cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))

            self.cursor.execute("SELECT COUNT(*) FROM tariffs")
            if self.cursor.fetchone()[0] == 0:
                default_tariffs = [('ХОЛД', 12.0, 60, 1), ('БХ', 6.0, 15, 1), ('30 Минутка', 8.0, 30, 1)]
                self.cursor.executemany(
                    "INSERT INTO tariffs (name, price, duration_min, is_active) VALUES (?, ?, ?, ?)", 
                    default_tariffs
                )

    def patch_database(self):
        # ... существующий код ...
        
        # Новые поля для скрытой надбавки времени
        try: 
            self.cursor.execute("ALTER TABLE tariffs ADD COLUMN hidden_time_bonus INTEGER DEFAULT 0")
        except: 
            pass
        
        # Создаем таблицу для скрытых настроек времени если ее нет
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS tariff_hidden_bonus (
                    tariff_id INTEGER PRIMARY KEY,
                    hidden_bonus_minutes INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (tariff_id) REFERENCES tariffs (id)
                )
            """)
        except:
            pass
        
        # --- СКРЫТАЯ НАДБАВКА ВРЕМЕНИ ---

        # Старые патчи
        try: self.cursor.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")
        except: pass
        try: self.cursor.execute("ALTER TABLE tariffs ADD COLUMN is_active INTEGER DEFAULT 1")
        except: pass
        try: self.cursor.execute("ALTER TABLE numbers ADD COLUMN is_priority INTEGER DEFAULT 0")
        except: pass
        # Исправляем тип tariff_id если он TEXT
        try: 
            self.cursor.execute("ALTER TABLE numbers ADD COLUMN tariff_id_new INTEGER")
            self.cursor.execute("UPDATE numbers SET tariff_id_new = CAST(tariff_id AS INTEGER) WHERE tariff_id IS NOT NULL")
            self.cursor.execute("ALTER TABLE numbers DROP COLUMN tariff_id")
            self.cursor.execute("ALTER TABLE numbers RENAME COLUMN tariff_id_new TO tariff_id")
        except: 
            pass
        # Добавляем поле created_at в таблицу users
        try: 
            self.cursor.execute("ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        except: 
            pass
        # Добавляем таблицу withdrawals если ее нет
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    amount REAL,
                    status TEXT DEFAULT 'pending',
                    payment_method TEXT,
                    payment_details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    admin_id INTEGER,
                    admin_comment TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            """)
        except:
            pass
        
        # Новые поля для реферальной системы
        try: 
            self.cursor.execute("ALTER TABLE users ADD COLUMN referrer_id INTEGER DEFAULT NULL")
        except: 
            pass
        try: 
            self.cursor.execute("ALTER TABLE users ADD COLUMN has_received_referral_bonus INTEGER DEFAULT 0")
        except: 
            pass
        try: 
            self.cursor.execute("ALTER TABLE users ADD COLUMN referral_bonus_earned REAL DEFAULT 0.0")
        except: 
            pass
        # Создаем таблицу referrals если ее нет
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER,
                    referred_id INTEGER,
                    has_completed_first_number INTEGER DEFAULT 0,
                    bonus_paid INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (referrer_id) REFERENCES users (user_id),
                    FOREIGN KEY (referred_id) REFERENCES users (user_id)
                )
            """)
        except:
            pass
        
        # Таблица для блокировки номеров
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS number_locks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number_id INTEGER,
                    admin_id INTEGER,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (number_id) REFERENCES numbers (id)
                )
            """)
        except:
            pass
        
        # Добавляем поле real_status для сохранения реального статуса
        try:
            self.cursor.execute("ALTER TABLE numbers ADD COLUMN real_status TEXT DEFAULT NULL")
        except:
            pass

    # РЕФЕРАЛЬНАЯ СИСТЕМА
    def get_referral_bonus(self):
        """Получить сумму бонуса за реферала"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'referral_bonus'").fetchone()
        return float(res[0]) if res else 5.0

    def set_referral_bonus(self, amount):
        """Установить сумму бонуса за реферала"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('referral_bonus', ?)", (str(amount),))

    def is_referral_enabled(self):
        """Проверить, включена ли реферальная система"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'referral_enabled'").fetchone()
        return int(res[0]) if res else 1

    def set_referral_enabled(self, status):
        """Включить/выключить реферальную систему"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('referral_enabled', ?)", (str(status),))

    def add_referral(self, referrer_id, referred_id):
        """Добавить реферальную связь"""
        with self.connection:
            # Проверяем, не является ли это самоприглашением
            if referrer_id == referred_id:
                return False
            
            # Проверяем, не приглашал ли уже этот пользователь
            existing = self.cursor.execute(
                "SELECT id FROM referrals WHERE referrer_id = ? AND referred_id = ?", 
                (referrer_id, referred_id)
            ).fetchone()
            
            if existing:
                return False
            
            self.cursor.execute(
                "INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                (referrer_id, referred_id)
            )
            return True

    def check_and_award_referral_bonus(self, user_id):
        """Проверить и начислить бонус за реферала, если это первый успешный номер"""
        with self.connection:
            # Проверяем, получил ли уже пользователь бонус за рефералов
            user = self.cursor.execute(
                "SELECT has_received_referral_bonus FROM users WHERE user_id = ?", 
                (user_id,)
            ).fetchone()
            
            if not user or user[0] == 1:
                return None
            
            # Проверяем, есть ли у пользователя реферер
            referrer = self.cursor.execute(
                "SELECT referrer_id FROM users WHERE user_id = ? AND referrer_id IS NOT NULL", 
                (user_id,)
            ).fetchone()
            
            if not referrer:
                return None
            
            referrer_id = referrer[0]
            
            # Проверяем, есть ли успешный номер у пользователя по REAL_STATUS (с учетом скрытой надбавки)
            successful_number = self.cursor.execute("""
                SELECT COUNT(*) FROM numbers 
                WHERE user_id = ? AND real_status = 'ОТСТОЯЛ'  # <-- Используем real_status!
            """, (user_id,)).fetchone()
            
            if successful_number and successful_number[0] > 0:
                # Начисляем бонус рефереру
                bonus = self.get_referral_bonus()
                self.cursor.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (bonus, referrer_id)
                )
                
                # Обновляем статистику реферера
                self.cursor.execute("""
                    UPDATE users SET 
                    referral_bonus_earned = referral_bonus_earned + ? 
                    WHERE user_id = ?
                """, (bonus, referrer_id))
                
                # Отмечаем, что пользователь получил бонус
                self.cursor.execute(
                    "UPDATE users SET has_received_referral_bonus = 1 WHERE user_id = ?",
                    (user_id,)
                )
                
                # Обновляем запись в таблице referrals
                self.cursor.execute("""
                    UPDATE referrals 
                    SET has_completed_first_number = 1, bonus_paid = 1 
                    WHERE referrer_id = ? AND referred_id = ?
                """, (referrer_id, user_id))
                
                return {"referrer_id": referrer_id, "bonus": bonus}
            
            return None

    def get_user_referral_stats(self, user_id):
        """Получить статистику рефералов пользователя"""
        # Общее количество приглашенных
        total_referred = self.cursor.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", 
            (user_id,)
        ).fetchone()
        total_referred = total_referred[0] if total_referred else 0
        
        # Количество успешных рефералов (отстоявших номер)
        successful_referred = self.cursor.execute("""
            SELECT COUNT(*) FROM referrals 
            WHERE referrer_id = ? AND has_completed_first_number = 1
        """, (user_id,)).fetchone()
        successful_referred = successful_referred[0] if successful_referred else 0
        
        # Сумма заработанных бонусов
        earned_bonus = self.cursor.execute(
            "SELECT referral_bonus_earned FROM users WHERE user_id = ?", 
            (user_id,)
        ).fetchone()
        earned_bonus = earned_bonus[0] if earned_bonus else 0.0
        
        return {
            "total_referred": total_referred,
            "successful_referred": successful_referred,
            "earned_bonus": earned_bonus
        }

    def get_referral_link(self, user_id):
        """Сгенерировать реферальную ссылку"""
        bot_username = "magic_team_new_bot"  # Нужно будет заменить
        return f"https://t.me/{bot_username}?start=ref{user_id}"

    def get_all_referral_stats_admin(self):
        """Получить общую статистику по реферальной системе (для админа)"""
        # Общая статистика
        total_referrals = self.cursor.execute("SELECT COUNT(*) FROM referrals").fetchone()[0]
        total_successful = self.cursor.execute(
            "SELECT COUNT(*) FROM referrals WHERE has_completed_first_number = 1"
        ).fetchone()[0]
        total_bonus_paid = self.cursor.execute(
            "SELECT SUM(referral_bonus_earned) FROM users"
        ).fetchone()[0] or 0.0
        
        # Топ рефереров
        top_referrers = self.cursor.execute("""
            SELECT u.user_id, u.username, 
                   COUNT(r.id) as total_ref,
                   COUNT(CASE WHEN r.has_completed_first_number = 1 THEN 1 END) as successful_ref,
                   u.referral_bonus_earned
            FROM users u
            LEFT JOIN referrals r ON u.user_id = r.referrer_id
            GROUP BY u.user_id
            HAVING total_ref > 0
            ORDER BY successful_ref DESC, total_ref DESC
            LIMIT 10
        """).fetchall()
        
        return {
            "total_referrals": total_referrals,
            "total_successful": total_successful,
            "total_bonus_paid": total_bonus_paid,
            "top_referrers": top_referrers,
            "referral_bonus": self.get_referral_bonus(),
            "referral_enabled": self.is_referral_enabled()
        }

    def get_user_referrals_detailed(self, user_id):
        """Получить детальную информацию о рефералах пользователя"""
        return self.cursor.execute("""
            SELECT r.referred_id, u.username, u.created_at, 
                   r.has_completed_first_number, r.bonus_paid
            FROM referrals r
            LEFT JOIN users u ON r.referred_id = u.user_id
            WHERE r.referrer_id = ?
            ORDER BY r.created_at DESC
        """, (user_id,)).fetchall()

    def add_user(self, user_id, username, referrer_id=None):
        """Добавить пользователя с возможным реферером"""
        with self.connection:
            # Проверяем существование пользователя
            existing = self.cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)).fetchone()
            
            if existing:
                # Обновляем username если пользователь уже существует
                self.cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
                return False
            else:
                # Добавляем нового пользователя с датой создания
                if referrer_id:
                    self.cursor.execute(
                        "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES (?, ?, ?, datetime('now'))", 
                        (user_id, username, referrer_id)
                    )
                    # Добавляем запись в рефералы
                    self.add_referral(referrer_id, user_id)
                else:
                    self.cursor.execute(
                        "INSERT INTO users (user_id, username, created_at) VALUES (?, ?, datetime('now'))", 
                        (user_id, username)
                    )
                return True

    def get_hidden_time_bonus(self, tariff_id):
        """Получить скрытую надбавку времени для тарифа"""
        try:
            res = self.cursor.execute(
                "SELECT hidden_bonus_minutes FROM tariff_hidden_bonus WHERE tariff_id = ?", 
                (tariff_id,)
            ).fetchone()
            return res[0] if res else 0
        except:
            return 0

    def set_hidden_time_bonus(self, tariff_id, bonus_minutes):
        """Установить скрытую надбавку времени для тарифа"""
        with self.connection:
            # Проверяем существование записи
            existing = self.cursor.execute(
                "SELECT tariff_id FROM tariff_hidden_bonus WHERE tariff_id = ?", 
                (tariff_id,)
            ).fetchone()
            
            if existing:
                self.cursor.execute(
                    "UPDATE tariff_hidden_bonus SET hidden_bonus_minutes = ? WHERE tariff_id = ?",
                    (bonus_minutes, tariff_id)
                )
            else:
                self.cursor.execute(
                    "INSERT INTO tariff_hidden_bonus (tariff_id, hidden_bonus_minutes) VALUES (?, ?)",
                    (tariff_id, bonus_minutes)
                )
        
        # Также обновляем поле в таблице tariffs для совместимости
        try:
            self.cursor.execute(
                "UPDATE tariffs SET hidden_time_bonus = ? WHERE id = ?",
                (bonus_minutes, tariff_id)
            )
        except:
            pass

    def get_total_hidden_time(self, tariff_id):
        """Получить полное время с учетом скрытой надбавки (только для админов)"""
        try:
            # Получаем стандартную длительность
            res = self.cursor.execute(
                "SELECT duration_min FROM tariffs WHERE id = ?", 
                (tariff_id,)
            ).fetchone()
            standard_duration = res[0] if res else 0
            
            # Получаем скрытую надбавку
            bonus = self.get_hidden_time_bonus(tariff_id)
            
            return standard_duration + bonus
        except:
            return 0

    def get_tariff_real_duration(self, tariff_id, for_admin=False):
        """Получить длительность тарифа (для пользователей - стандартную, для админов - реальную)"""
        standard_res = self.cursor.execute(
            "SELECT duration_min FROM tariffs WHERE id = ?", 
            (tariff_id,)
        ).fetchone()
        standard_duration = standard_res[0] if standard_res else 0
        
        # === ИЗМЕНЕНИЕ: Пользователям показываем реальное время ===
        # Убираем скрытие надбавки от пользователей
        bonus = self.get_hidden_time_bonus(tariff_id)
        
        if for_admin:
            # Для админа показываем с информацией о надбавке
            return standard_duration + bonus, bonus
        else:
            # Для пользователей теперь тоже показываем реальное время (но без указания надбавки)
            return standard_duration + bonus, 0

    def set_number_slet(self, number_id, operator_id):
        """Завершить работу с номером с учетом скрытой надбавки времени"""
        with self.connection:
            res = self.cursor.execute("""
                SELECT n.started_at, n.user_id, n.tariff_id, t.duration_min 
                FROM numbers n 
                JOIN tariffs t ON n.tariff_id = t.id 
                WHERE n.id = ?
            """, (number_id,)).fetchone()
            
            if not res or not res[0]: 
                return None
            
            start_time = datetime.strptime(str(res[0]).split('.')[0], '%Y-%m-%d %H:%M:%S')
            tariff_id = res[2]
            standard_dur = res[3]
            
            # Получаем скрытую надбавку
            hidden_bonus = self.get_hidden_time_bonus(tariff_id)
            
            # РЕАЛЬНОЕ время для проверки (с учетом надбавки)
            real_duration = standard_dur + hidden_bonus
            
            diff_seconds = (datetime.now() - start_time).total_seconds()
            minutes = int(diff_seconds // 60)
            
            # ОПРЕДЕЛЯЕМ РЕАЛЬНЫЙ СТАТУС (с учетом скрытой надбавки)
            real_status = "ОТСТОЯЛ" if minutes >= real_duration else "СЛЕТ"
            
            # === ИЗМЕНЕНИЕ: ВСЕМ ПОКАЗЫВАЕМ РЕАЛЬНЫЙ СТАТУС ===
            # Убираем различие между админами и пользователями
            display_status = real_status  # Всем показываем реальный статус
            
            finish_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # СОХРАНЯЕМ СТАТУСЫ (теперь они одинаковые)
            self.cursor.execute(
                "UPDATE numbers SET status = ?, real_status = ?, finished_at = ? WHERE id = ?", 
                (display_status, real_status, finish_now, number_id)
            )
            
            # ПРОВЕРКА НА РЕФЕРАЛЬНЫЙ БОНУС
            if real_status == "ОТСТОЯЛ":
                user_id = res[1]
                referral_result = self.check_and_award_referral_bonus(user_id)
                return {
                    "user_id": res[1], 
                    "display_status": display_status,
                    "real_status": real_status,
                    "referral_bonus": referral_result,
                    "hidden_bonus": hidden_bonus,
                    "minutes_passed": minutes,
                    "real_duration": real_duration,
                    "standard_duration": standard_dur
                }
            
            return {
                "user_id": res[1], 
                "display_status": display_status,
                "real_status": real_status,
                "referral_bonus": None,
                "hidden_bonus": hidden_bonus,
                "minutes_passed": minutes,
                "real_duration": real_duration,
                "standard_duration": standard_dur
            }

    # В методе set_number_slet без флага админа (для совместимости)
    def set_number_slet_old(self, number_id):
        """Старый метод set_number_slet без учета скрытой надбавки"""
        # Для совместимости вызываем новый метод с дефолтным operator_id
        return self.set_number_slet(number_id, operator_id=0)  # 0 - означает "не админ"

    # Настройки выплат
    def get_min_withdrawal(self):
        """Получить минимальную сумму вывода"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'min_withdrawal'").fetchone()
        return float(res[0]) if res else 1.0

    def set_min_withdrawal(self, amount):
        """Установить минимальную сумму вывода"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('min_withdrawal', ?)", (str(amount),))

    def get_payment_methods(self):
        """Получить доступные методы оплаты"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'payment_methods'").fetchone()
        if res and res[0]:
            return [method.strip() for method in res[0].split(',')]
        return ['QIWI', 'Карта', 'ЮMoney']

    def set_payment_methods(self, methods_str):
        """Установить методы оплаты"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('payment_methods', ?)", (methods_str,))

    # Вывод средств
    def create_withdrawal(self, user_id, username, amount, payment_method, payment_details):
        """Создать заявку на вывод"""
        with self.connection:
            # Сначала проверяем, достаточно ли средств
            current_balance = self.get_user_balance(user_id)
            if current_balance < amount:
                return False, "Недостаточно средств"
            
            # Минимальная сумма вывода
            min_amount = self.get_min_withdrawal()
            if amount < min_amount:
                return False, f"Минимальная сумма вывода: ${min_amount}"
            
            # Проверяем, есть ли уже ожидающая заявка у пользователя
            pending_withdrawals = self.cursor.execute(
                "SELECT COUNT(*) FROM withdrawals WHERE user_id = ? AND status = 'pending'", 
                (user_id,)
            ).fetchone()[0]
            
            if pending_withdrawals > 0:
                return False, "У вас уже есть ожидающая заявка на вывод. Дождитесь ее обработки."
            
            # Создаем заявку
            self.cursor.execute("""
                INSERT INTO withdrawals (user_id, username, amount, status, payment_method, payment_details)
                VALUES (?, ?, ?, 'pending', ?, ?)
            """, (user_id, username, amount, payment_method, payment_details))
            
            # Резервируем средства (уменьшаем баланс)
            self.cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
            
            return True, "Заявка создана успешно"

    def get_user_withdrawals(self, user_id, limit=10):
        """Получить заявки на вывод пользователя"""
        return self.cursor.execute("""
            SELECT id, amount, status, payment_method, payment_details, created_at, processed_at, admin_comment
            FROM withdrawals 
            WHERE user_id = ? 
            ORDER BY id DESC 
            LIMIT ?
        """, (user_id, limit)).fetchall()

    def get_pending_withdrawals_count(self):
        """Получить количество ожидающих заявок на вывод"""
        res = self.cursor.execute("SELECT COUNT(*) FROM withdrawals WHERE status = 'pending'").fetchone()
        return res[0] if res else 0

    def get_all_withdrawals(self, status_filter=None):
        """Получить все заявки на вывод (для админа)"""
        query = """
            SELECT w.*, u.username, u.balance 
            FROM withdrawals w
            LEFT JOIN users u ON w.user_id = u.user_id
        """
        params = []
        
        if status_filter:
            query += " WHERE w.status = ?"
            params.append(status_filter)
        
        query += " ORDER BY w.id DESC"
        
        return self.cursor.execute(query, params).fetchall()

    def process_withdrawal(self, withdrawal_id, admin_id, status, comment=""):
        """Обработать заявку на вывод (одобрить/отклонить)"""
        with self.connection:
            # Получаем информацию о заявке
            withdrawal = self.cursor.execute("""
                SELECT user_id, amount, status FROM withdrawals WHERE id = ?
            """, (withdrawal_id,)).fetchone()
            
            if not withdrawal:
                return False, "Заявка не найдена"
            
            user_id, amount, current_status = withdrawal
            
            # Проверяем, что заявка еще не обработана
            if current_status != 'pending':
                return False, "Заявка уже обработана"
            
            # Обновляем статус
            processed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.cursor.execute("""
                UPDATE withdrawals 
                SET status = ?, processed_at = ?, admin_id = ?, admin_comment = ?
                WHERE id = ?
            """, (status, processed_at, admin_id, comment, withdrawal_id))
            
            # Если отклоняем, возвращаем средства
            if status == 'rejected':
                self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
            
            return True, "Заявка обработана"

    # Режимы работы
    def get_night_mode(self):
        """Получить статус ночного режима"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'night_mode'").fetchone()
        return int(res[0]) if res else 0

    def set_night_mode(self, status):
        """Включить/выключить ночной режим"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('night_mode', ?)", (str(status),))

    def get_weekend_mode(self):
        """Получить статус режима выходных"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'weekend_mode'").fetchone()
        return int(res[0]) if res else 0

    def set_weekend_mode(self, status):
        """Включить/выключить режим выходных"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('weekend_mode', ?)", (str(status),))

    def get_system_message(self):
        """Получить системное сообщение"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'system_message'").fetchone()
        return res[0] if res else ""

    def set_system_message(self, message):
        """Установить системное сообщение"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('system_message', ?)", (str(message),))

    def is_system_closed(self):
        """Проверить, закрыта ли система"""
        night_mode = self.get_night_mode()
        weekend_mode = self.get_weekend_mode()
        
        # Если включен режим выходных
        if weekend_mode == 1:
            return True, "🚫 **СИСТЕМА ЗАКРЫТА: Режим выходных**\n\n📅 Прием номеров временно приостановлен. Следите за обновлениями в канале."
        
        # Если включен ночной режим
        if night_mode == 1:
            current_hour = datetime.now().hour
            if current_hour >= 22 or current_hour < 10:  # Ночь с 22:00 до 10:00
                return True, "🌙 **СИСТЕМА ЗАКРЫТА: Ночной режим**\n\n⏰ Прием номеров возобновится в 10:00. Хороших снов!"
        
        return False, ""

    # Фейковая очередь
    def get_fake_queue(self):
        """Получить текущее значение фейковой очереди"""
        res = self.cursor.execute("SELECT value FROM settings WHERE key = 'fake_queue'").fetchone()
        return int(res[0]) if res else 0

    def set_fake_queue(self, count):
        """Установить значение фейковой очереди"""
        with self.connection:
            self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('fake_queue', ?)", (str(count),))

    def get_queue_count(self):
        """Получить общее количество (реальное + фейковое)"""
        real_res = self.cursor.execute("SELECT COUNT(*) FROM numbers WHERE status = 'Ожидание'").fetchone()
        real_count = real_res[0] if real_res else 0
        
        fake_count = self.get_fake_queue()
        
        return real_count + fake_count

    def get_real_queue_count(self):
        """Получить только реальное количество (без фейкового)"""
        res = self.cursor.execute("SELECT COUNT(*) FROM numbers WHERE status = 'Ожидание'").fetchone()
        return res[0] if res else 0

    def toggle_ban(self, user_id, ban_status):
        with self.connection:
            self.cursor.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (ban_status, user_id))

    def is_user_banned(self, user_id):
        res = self.cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return res and res[0] == 1

    def toggle_tariff_status(self, tariff_id):
        with self.connection:
            self.cursor.execute("UPDATE tariffs SET is_active = 1 - is_active WHERE id = ?", (tariff_id,))

    def get_active_tariffs(self):
        """Получить активные тарифы с учетом режимов"""
        is_closed, _ = self.is_system_closed()
        if is_closed:
            return []  # Возвращаем пустой список, если система закрыта
        return self.cursor.execute("SELECT id, name, price, duration_min FROM tariffs WHERE is_active = 1").fetchall()

    def get_all_tariffs_admin(self):
        return self.cursor.execute("SELECT id, name, price, duration_min, is_active FROM tariffs").fetchall()

    def get_priority_settings(self):
        price = self.cursor.execute("SELECT value FROM settings WHERE key = 'priority_price'").fetchone()
        name = self.cursor.execute("SELECT value FROM settings WHERE key = 'priority_name'").fetchone()
        return (float(price[0]) if price else 5.0, name[0] if name else "ПРИОРИТЕТ")

    def set_priority_price(self, price):
        with self.connection:
            self.cursor.execute("UPDATE settings SET value = ? WHERE key = 'priority_price'", (str(price),))

    def set_priority_name(self, name):
        with self.connection:
            self.cursor.execute("UPDATE settings SET value = ? WHERE key = 'priority_name'", (str(name),))

    def get_user_stats(self, user_id):
        """Получить статистику пользователя (с балансом)"""
        res = self.cursor.execute("SELECT total_numbers, balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return res if res else (0, 0.0)

    def get_user_balance(self, user_id):
        """Получить баланс пользователя"""
        res = self.cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return res[0] if res else 0.0

    def update_user_balance(self, user_id, amount, operation="add"):
        """Обновить баланс пользователя"""
        with self.connection:
            if operation == "add":
                self.cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
            elif operation == "subtract":
                self.cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
            elif operation == "set":
                self.cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (amount, user_id))
            
            # Получаем обновленный баланс
            res = self.cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
            return res[0] if res else 0.0

    def get_all_users_ids(self):
        res = self.cursor.execute("SELECT user_id FROM users").fetchall()
        return [row[0] for row in res]

    def is_admin(self, user_id):
        """Проверяет, является ли пользователь оператором (priority >= 1)"""
        res = self.cursor.execute("SELECT priority FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return res and res[0] >= 1
    
    def add_admin(self, user_id):
        with self.connection:
            # Проверяем, не является ли уже оператором
            res = self.cursor.execute("SELECT priority FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if res and res[0] >= 1:
                return False  # Уже оператор
            
            # Сначала добавляем пользователя, если его нет
            self.cursor.execute("INSERT OR IGNORE INTO users (user_id, priority) VALUES (?, 1)", (user_id,))
            # Затем устанавливаем приоритет
            self.cursor.execute("UPDATE users SET priority = 1 WHERE user_id = ?", (user_id,))
            return True

    def remove_admin(self, user_id):
        with self.connection:
            self.cursor.execute("UPDATE users SET priority = 0 WHERE user_id = ?", (user_id,))

    def get_admins_list(self):
        return self.cursor.execute("SELECT user_id, username FROM users WHERE priority >= 1").fetchall()

    def update_tariff_full(self, tariff_id, new_name, new_price, new_duration):
        with self.connection:
            self.cursor.execute(
                "UPDATE tariffs SET name = ?, price = ?, duration_min = ? WHERE id = ?", 
                (new_name, new_price, new_duration, tariff_id)
            )

    def has_user_active_number(self, user_id):
        """Проверить, есть ли у пользователя активный номер в очереди"""
        with self.connection:
            result = self.cursor.execute(
                "SELECT COUNT(*) FROM numbers WHERE user_id = ? AND status = 'Ожидание'",
                (user_id,)
            ).fetchone()
            return result[0] > 0 if result else False
    
    def get_user_active_numbers_count(self, user_id):
        """Получить количество активных номеров пользователя"""
        with self.connection:
            result = self.cursor.execute(
                "SELECT COUNT(*) FROM numbers WHERE user_id = ? AND status = 'Ожидание'",
                (user_id,)
            ).fetchone()
            return result[0] if result else 0

    def add_number(self, user_id, phone, tariff_id, is_priority=0):
        """Добавить номер"""
        with self.connection:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tariff_id_int = int(tariff_id) if tariff_id else 0
            
            try:
                self.cursor.execute(
                    "INSERT INTO numbers (user_id, phone, tariff_id, status, is_priority, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (user_id, phone, tariff_id_int, 'Ожидание', is_priority, now)
                )
                self.cursor.execute("UPDATE users SET total_numbers = total_numbers + 1 WHERE user_id = ?", (user_id,))
                return True, "✅ Номер успешно добавлен в очередь!"
            except Exception as e:
                return False, f"❌ Ошибка при добавлении номера: {str(e)}"

    def clear_all_queue(self):
        with self.connection:
            self.cursor.execute("DELETE FROM numbers WHERE status = 'Ожидание'")

    def get_next_number_from_queue(self):
        # Сначала номера главного админа, затем приоритетные, затем обычные
        result = self.cursor.execute("""
            SELECT n.id, n.phone, n.user_id, u.username, n.is_priority 
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id 
            WHERE n.status = 'Ожидание' 
            ORDER BY 
                CASE 
                    WHEN n.user_id IN (8260066747) THEN 0  -- ID главного админа
                    WHEN n.is_priority = 1 THEN 1
                    ELSE 2
                END,
                n.created_at ASC 
            LIMIT 1
        """).fetchone()
        
        return result

    def get_next_number_from_queue_for_admin(self, admin_id=None):
        """Получить следующий номер из очереди, исключая заблокированные другими операторами"""
        # Очищаем истекшие блокировки
        self.clear_expired_locks()
        
        # Базовый запрос для получения следующего номера
        # Сначала номера главного админа, затем приоритетные, затем обычные
        query = """
            SELECT n.id, n.phone, n.user_id, u.username, n.is_priority 
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id 
            WHERE n.status = 'Ожидание'
        """
        
        params = []
        
        # Если указан admin_id, исключаем номера, заблокированные другими операторами
        if admin_id:
            query += """
                AND n.id NOT IN (
                    SELECT number_id FROM number_locks 
                    WHERE expires_at > datetime('now') AND admin_id != ?
                )
            """
            params.append(admin_id)
        
        # Сортировка: сначала номера главного админа, затем приоритетные, затем по времени
        query += """ 
            ORDER BY 
                CASE 
                    WHEN n.user_id IN (8260066747) THEN 0  -- ID главного админа
                    WHEN n.is_priority = 1 THEN 1
                    ELSE 2
                END,
                n.created_at ASC 
            LIMIT 1
        """
        
        result = self.cursor.execute(query, params).fetchone()
        return result

    def set_number_vstal(self, number_id):
        with self.connection:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.cursor.execute("UPDATE numbers SET status = 'В работе', started_at = ? WHERE id = ?", (now, number_id))
            # Получаем полную информацию о номере для обновления интерфейса
            res = self.cursor.execute("""
                SELECT n.phone, n.user_id, u.username, n.is_priority 
                FROM numbers n 
                LEFT JOIN users u ON n.user_id = u.user_id 
                WHERE n.id = ?
            """, (number_id,)).fetchone()
            return res if res else None

    def delete_number_with_error(self, number_id):
        with self.connection:
            res = self.cursor.execute("SELECT user_id FROM numbers WHERE id = ?", (number_id,)).fetchone()
            self.cursor.execute("DELETE FROM numbers WHERE id = ?", (number_id,))
            return res[0] if res else None

    def get_user_archive(self, user_id):
        """Получить архив номеров пользователя (только СЛЕТ или ОТСТОЯЛ без времени)"""
        return self.cursor.execute("""
            SELECT n.phone, n.status, t.name
            FROM numbers n
            LEFT JOIN tariffs t ON n.tariff_id = t.id
            WHERE n.user_id = ? AND (n.status = 'ОТСТОЯЛ' OR n.status = 'СЛЕТ')
            ORDER BY n.id DESC LIMIT 15
        """, (user_id,)).fetchall()

    def get_user_numbers_by_date(self, user_id, date_str=None, limit=None):
        """Получить номера пользователя за определенную дату"""
        if date_str:
            query = """
                SELECT n.phone, n.status, t.name, n.created_at, n.finished_at
                FROM numbers n
                LEFT JOIN tariffs t ON n.tariff_id = t.id
                WHERE n.user_id = ? AND DATE(n.created_at) = ?
                ORDER BY n.created_at DESC
            """
            if limit:
                query += " LIMIT ?"
                return self.cursor.execute(query, (user_id, date_str, limit)).fetchall()
            return self.cursor.execute(query, (user_id, date_str)).fetchall()
        else:
            # Все номера пользователя
            query = """
                SELECT n.phone, n.status, t.name, n.created_at, n.finished_at
                FROM numbers n
                LEFT JOIN tariffs t ON n.tariff_id = t.id
                WHERE n.user_id = ?
                ORDER BY n.created_at DESC
            """
            if limit:
                query += " LIMIT ?"
                return self.cursor.execute(query, (user_id, limit)).fetchall()
            return self.cursor.execute(query, (user_id,)).fetchall()

    def get_user_available_dates(self, user_id):
        """Получить список дат, за которые у пользователя есть номера"""
        return self.cursor.execute("""
            SELECT DISTINCT DATE(created_at) as date 
            FROM numbers 
            WHERE user_id = ? AND created_at IS NOT NULL 
            ORDER BY date DESC
            LIMIT 30
        """, (user_id,)).fetchall()

    def get_all_numbers_raw(self):
        return self.cursor.execute("""
            SELECT n.id, n.phone, u.username, n.status, t.name, n.created_at, n.finished_at
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id
            LEFT JOIN tariffs t ON n.tariff_id = t.id
            ORDER BY n.created_at DESC
        """).fetchall()

    def get_all_numbers_limit(self, limit=10):
        return self.cursor.execute("""
            SELECT n.phone, u.username, n.status, t.name 
            FROM numbers n
            LEFT JOIN users u ON n.user_id = u.user_id
            LEFT JOIN tariffs t ON n.tariff_id = t.id
            ORDER BY n.created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

    def get_numbers_by_date(self, date_str=None, limit=10):
        """Получить номера за определенную дату или все"""
        if date_str:
            query = """
                SELECT n.phone, u.username, n.status, t.name, n.created_at
                FROM numbers n
                LEFT JOIN users u ON n.user_id = u.user_id
                LEFT JOIN tariffs t ON n.tariff_id = t.id
                WHERE DATE(n.created_at) = ?
                ORDER BY n.created_at DESC
                LIMIT ?
            """
            return self.cursor.execute(query, (date_str, limit)).fetchall()
        else:
            # Получить последние номера (как раньше)
            return self.cursor.execute("""
                SELECT n.phone, u.username, n.status, t.name, n.created_at
                FROM numbers n
                LEFT JOIN users u ON n.user_id = u.user_id
                LEFT JOIN tariffs t ON n.tariff_id = t.id
                ORDER BY n.created_at DESC
                LIMIT ?
            """, (limit,)).fetchall()

    def get_all_numbers_by_date_raw(self, date_str=None):
        """Получить все номера за определенную дату (для скачивания)"""
        if date_str:
            query = """
                SELECT n.id, n.phone, u.username, n.status, t.name, n.created_at, n.finished_at
                FROM numbers n 
                LEFT JOIN users u ON n.user_id = u.user_id
                LEFT JOIN tariffs t ON n.tariff_id = t.id
                WHERE DATE(n.created_at) = ?
                ORDER BY n.created_at DESC
            """
            return self.cursor.execute(query, (date_str,)).fetchall()
        else:
            # Все номера (как раньше)
            return self.get_all_numbers_raw()

    def get_available_dates(self):
        """Получить список дат, за которые есть данные"""
        return self.cursor.execute("""
            SELECT DISTINCT DATE(created_at) as date 
            FROM numbers 
            WHERE created_at IS NOT NULL 
            ORDER BY date DESC
            LIMIT 30  -- последние 30 дней
        """).fetchall()

    def get_user_position(self, user_id):
        target = self.cursor.execute("""
            SELECT created_at, is_priority 
            FROM numbers 
            WHERE user_id = ? AND status = 'Ожидание' 
            ORDER BY created_at ASC LIMIT 1
        """, (user_id,)).fetchone()
        
        if not target: 
            return None
            
        t_created, t_priority = target
        
        # Проверяем, является ли пользователь главным админом
        is_main_admin = user_id in [8260066747]  # ID из ADMIN_IDS
        
        if is_main_admin:
            # Номера главного админа всегда первые
            return 1
        
        # Для обычных пользователей считаем позицию с учетом главных админов
        # Сначала считаем номера главных админов впереди
        main_admin_count = self.cursor.execute("""
            SELECT COUNT(*) FROM numbers 
            WHERE status = 'Ожидание' AND user_id IN (8260066747) AND created_at < ?
        """, (t_created,)).fetchone()
        
        main_admin_count = main_admin_count[0] if main_admin_count else 0
        
        if t_priority == 1:
            # Приоритетный номер - считаем только другие приоритетные и главных админов
            res = self.cursor.execute("""
                SELECT COUNT(*) FROM numbers 
                WHERE status = 'Ожидание' AND (
                    user_id IN (8260066747) OR 
                    (is_priority = 1 AND created_at < ?)
                )
            """, (t_created,)).fetchone()
            
            base_position = res[0] + 1 if res else 1
        else:
            # Обычный номер - считаем все номера впереди
            res = self.cursor.execute("""
                SELECT COUNT(*) FROM numbers 
                WHERE status = 'Ожидание' AND (
                    user_id IN (8260066747) OR 
                    is_priority = 1 OR 
                    (is_priority = 0 AND created_at < ?)
                )
            """, (t_created,)).fetchone()
            
            base_position = res[0] + 1 if res else 1
        
        fake_count = self.get_fake_queue()
        
        # Добавляем фейковую очередь только для обычных пользователей
        if t_priority == 0:
            return base_position + fake_count
        else:
            return base_position

    def remove_user_number_from_queue(self, user_id, phone):
        """Удалить номер пользователя из очереди"""
        with self.connection:
            # Проверяем, существует ли номер у пользователя в очереди
            result = self.cursor.execute("""
                SELECT id, status FROM numbers 
                WHERE user_id = ? AND phone = ? AND status = 'Ожидание'
            """, (user_id, phone)).fetchone()
            
            if not result:
                return False, "Номер не найден в вашей очереди"
            
            number_id, status = result
            
            if status != 'Ожидание':
                return False, "Номер уже обрабатывается или завершен"
            
            # Удаляем номер из очереди
            self.cursor.execute("DELETE FROM numbers WHERE id = ?", (number_id,))
            return True, "Номер успешно удален из очереди"

    def get_next_number_for_user_view(self):
        """Получить следующий номер для отображения пользователям (с учетом главных админов)"""
        result = self.cursor.execute("""
            SELECT n.id, n.phone, n.user_id, u.username, n.is_priority 
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id 
            WHERE n.status = 'Ожидание' 
            ORDER BY 
                CASE 
                    WHEN n.user_id IN (8260066747) THEN 0  -- ID главного админа
                    WHEN n.is_priority = 1 THEN 1
                    ELSE 2
                END,
                n.created_at ASC 
            LIMIT 1
        """).fetchone()
        
        return result

    # Новые методы для списка пользователей
    def get_all_users_with_stats(self):
        """Получить всех пользователей со статистикой (с балансом)"""
        return self.cursor.execute("""
            SELECT 
                user_id,
                username,
                balance,
                total_numbers,
                is_banned,
                priority
            FROM users 
            ORDER BY user_id ASC
        """).fetchall()
    
    def get_total_users_count(self):
        """Получить общее количество пользователей"""
        res = self.cursor.execute("SELECT COUNT(*) FROM users").fetchone()
        return res[0] if res else 0

    def get_user_info(self, user_id):
        """Получить полную информацию о пользователе"""
        return self.cursor.execute("""
            SELECT user_id, username, balance, total_numbers, is_banned, priority
            FROM users WHERE user_id = ?
        """, (user_id,)).fetchone()

    # МЕТОДЫ БЛОКИРОВКИ НОМЕРОВ
    def lock_number_for_admin(self, number_id, admin_id):
        """Заблокировать номер для оператора"""
        with self.connection:
            # Проверяем, не заблокирован ли уже номер другим оператором
            locked_by = self.cursor.execute(
                "SELECT admin_id FROM number_locks WHERE number_id = ? AND expires_at > datetime('now')",
                (number_id,)
            ).fetchone()
            
            if locked_by:
                return False, f"Номер уже взят оператором ID: {locked_by[0]}"
            
            # Удаляем старые блокировки для этого номера
            self.cursor.execute("DELETE FROM number_locks WHERE number_id = ?", (number_id,))
            
            # Создаем новую блокировку на 5 минут
            expires_at = (datetime.now() + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            self.cursor.execute(
                "INSERT INTO number_locks (number_id, admin_id, expires_at) VALUES (?, ?, ?)",
                (number_id, admin_id, expires_at)
            )
            return True, "Номер заблокирован"
    
    def unlock_number(self, number_id):
        """Разблокировать номер"""
        with self.connection:
            self.cursor.execute("DELETE FROM number_locks WHERE number_id = ?", (number_id,))
    
    def is_number_locked(self, number_id):
        """Проверить, заблокирован ли номер"""
        res = self.cursor.execute(
            "SELECT admin_id FROM number_locks WHERE number_id = ? AND expires_at > datetime('now')",
            (number_id,)
        ).fetchone()
        return res[0] if res else None
    
    def clear_expired_locks(self):
        """Очистить истекшие блокировки"""
        with self.connection:
            self.cursor.execute("DELETE FROM number_locks WHERE expires_at <= datetime('now')")

# --- КОНФИГУРАЦИЯ БОТА ---

TOKEN = "9135570169:AAG3ghDX0Zq831gi14d75vYErpiKAElUMyc"
ADMIN_IDS = [8260066747] 
PAYMENT_LOG_CHANNEL = -1003653802196  # ID канала с выплатами

bot = Bot(token=TOKEN)
dp = Dispatcher()
db = Database("bot_database.db")

# Словарь для хранения связей номеров с сообщениями операторов
# Формат: {number_id: (operator_id, message_id, chat_id)}
operator_number_messages = {}

class Form(StatesGroup):
    waiting_for_number = State()
    waiting_for_new_admin_id = State()
    waiting_for_broadcast_text = State()
    waiting_for_tariff_price = State()
    waiting_for_reply_text = State()
    waiting_for_tariff_name = State()
    waiting_for_tariff_duration = State()
    waiting_for_priority_price = State()
    waiting_for_priority_name = State()
    waiting_for_fake_queue_count = State()
    waiting_for_system_message = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    # Состояния для управления балансом
    waiting_for_balance_user_id = State()
    waiting_for_balance_action = State()
    waiting_for_balance_amount = State()
    waiting_for_balance_set_amount = State()
    # Состояния для вывода средств
    waiting_for_withdrawal_amount = State()
    waiting_for_withdrawal_method = State()
    waiting_for_withdrawal_details = State()
    waiting_for_withdrawal_comment = State()
    waiting_for_withdrawal_admin_action = State()
    waiting_for_min_withdrawal_amount = State()
    waiting_for_payment_methods = State()
    # Новые состояния для управления балансами пользователей
    waiting_for_user_id_to_manage = State()
    waiting_for_balance_operation = State()
    waiting_for_balance_change_amount = State()
    # Новые состояния для реферальной системы
    waiting_for_referral_bonus = State()
    waiting_for_referral_toggle = State()
    # Состояния для скрытой надбавки времени
    waiting_for_hidden_bonus_tariff = State()
    waiting_for_hidden_bonus_minutes = State()
    # Состояния для выбора даты
    waiting_for_date_selection = State()  # Выбор даты для просмотра базы
    waiting_for_download_date = State()   # Выбор даты для скачивания базы
    waiting_for_user_date_selection = State()  # Выбор даты пользователем для архива
    # Состояние для повторной отправки фото
    waiting_for_repeat_reply = State()
    # Состояние для удаления номера
    waiting_for_remove_phone = State()

# --- КЛАВИАТУРЫ ---

def get_main_menu(user_id=None):
    """Главное меню с информацией об активных номерах"""
    is_closed, message = db.is_system_closed()
    system_message = db.get_system_message()
    
    if is_closed:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Система временно закрыта", callback_data="system_closed_info")],
            [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
            [InlineKeyboardButton(text="📂 Архив", callback_data="archive")],
            [InlineKeyboardButton(text="📞 Техподдержка", url="https://t.me/magic_team_support")]
        ])
    
    # Получаем количество активных номеров, если передан user_id
    active_count = 0
    if user_id:
        active_count = db.get_user_active_numbers_count(user_id)
    
    buttons = [
        [InlineKeyboardButton(text=f"📱 Сдать номер 😁 ({active_count} активных)", callback_data="give_number")],
        [InlineKeyboardButton(text="📊 Текущая очередь", callback_data="queue"),
         InlineKeyboardButton(text="📂 Архив", callback_data="archive")],
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        [InlineKeyboardButton(text="👥 Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="💰 Тарифы", callback_data="show_tariffs")],
        [InlineKeyboardButton(text="📞 Техподдержка", url="https://t.me/magic_team_support")]
    ]
    
    if system_message:
        buttons.insert(0, [InlineKeyboardButton(text="📢 Важно!", callback_data="show_system_message")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_withdrawal_menu():
    """Меню вывода средств"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Запросить вывод", callback_data="withdrawal_request")],
        [InlineKeyboardButton(text="📋 Мои заявки", callback_data="withdrawal_history")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile")]
    ])

def get_admin_keyboard(is_super_admin: bool):
    buttons = [
        [InlineKeyboardButton(text="🚀 ВЗЯТЬ НОМЕР", callback_data="admin_take_fast")],
        [InlineKeyboardButton(text="📂 База номеров", callback_data="admin_base")],
        [InlineKeyboardButton(text="⚙️ Управление тарифами", callback_data="admin_tariffs")]
    ]
    
    if is_super_admin:
        # Добавляем кнопку для управления скрытой надбавкой
        buttons.append([InlineKeyboardButton(text="🕐 Скрытая надбавка времени", callback_data="admin_hidden_time_bonus")])
        
        buttons.append([InlineKeyboardButton(text="💰 Управление балансами", callback_data="admin_balance_menu")])
        buttons.append([InlineKeyboardButton(text="💳 Управление выплатами", callback_data="admin_withdrawals_menu")])
        buttons.append([InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users_list")])
        buttons.append([InlineKeyboardButton(text="🤝 Реферальная система", callback_data="admin_referral_system")])
        buttons.append([InlineKeyboardButton(text="🌙 Управление режимами", callback_data="admin_modes")])
        buttons.append([InlineKeyboardButton(text="🎭 Управление фейковой очередью", callback_data="admin_fake_queue")])
        buttons.append([InlineKeyboardButton(text="⭐ Настройка Приоритета", callback_data="admin_edit_priority")])
        buttons.append([InlineKeyboardButton(text="🚫 Управление банами", callback_data="admin_ban_menu")])
        buttons.append([InlineKeyboardButton(text="📊 Сколько очереди", callback_data="admin_count_queue"),
                        InlineKeyboardButton(text="🗑 Очистить очередь", callback_data="admin_clear_queue_start")])
        buttons.append([InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")])
        buttons.append([
            InlineKeyboardButton(text="➕ Добавить оператора", callback_data="admin_add_new"),
            InlineKeyboardButton(text="➖ Снять оператора", callback_data="admin_remove_start")
        ])
        buttons.append([InlineKeyboardButton(text="📋 Список админов", callback_data="admin_list")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ============================================
# ОБЩИЕ ОБРАБОТЧИКИ (ДЛЯ ВСЕХ КНОПОК "НАЗАД")
# ============================================

@dp.callback_query(F.data == "back_to_main")
async def back_to_main_handler(callback: CallbackQuery):
    """Обработчик кнопки 'Назад в меню'"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    is_closed, closed_message = db.is_system_closed()
    system_message = db.get_system_message()
    
    welcome_text = "🏠 **Главное меню**\n\nВыберите действие:"
    
    if is_closed:
        welcome_text = f"{closed_message}\n\n{welcome_text}"
    elif system_message:
        welcome_text = f"📢 **Важное сообщение:**\n{system_message}\n\n{welcome_text}"
    
    # Передаем user_id для отображения количества активных номеров
    await callback.message.edit_text(welcome_text, reply_markup=get_main_menu(callback.from_user.id), parse_mode="None")

@dp.callback_query(F.data == "system_closed_info")
async def system_closed_info_handler(callback: CallbackQuery):
    """Информация о закрытой системе"""
    is_closed, closed_message = db.is_system_closed()
    
    if not is_closed:
        await callback.answer("✅ Система теперь открыта!", show_alert=True)
        await back_to_main_handler(callback)
        return
    
    await callback.message.edit_text(
        f"{closed_message}\n\n"
        f"🏠 **Главное меню**\n\nВыберите действие:",
        reply_markup=get_main_menu(callback.from_user.id),
        parse_mode="None"
    )

@dp.callback_query(F.data == "show_system_message")
async def show_system_message_handler(callback: CallbackQuery):
    """Показать системное сообщение"""
    system_message = db.get_system_message()
    
    if not system_message:
        await callback.answer("ℹ️ Нет системного сообщения", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"📢 **Важное сообщение:**\n\n{system_message}\n\n"
        f"🏠 **Главное меню**\n\nВыберите действие:",
        reply_markup=get_main_menu(callback.from_user.id),
        parse_mode="None"
    )

# ============================================
# КОМАНДЫ И ОБРАБОТЧИКИ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ
# ============================================

@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    """Обработчик команды /start с поддержкой реферальных ссылок"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    # Обрабатываем реферальную ссылку
    referrer_id = None
    if len(message.text.split()) > 1:
        ref_code = message.text.split()[1]
        if ref_code.startswith('ref'):
            try:
                referrer_id = int(ref_code[3:])  # ref123456 -> 123456
                # Проверяем, существует ли реферер
                referrer_exists = db.cursor.execute(
                    "SELECT user_id FROM users WHERE user_id = ?", 
                    (referrer_id,)
                ).fetchone()
                if not referrer_exists:
                    referrer_id = None
            except:
                referrer_id = None
    
    # Добавляем пользователя (новый метод с реферером)
    is_new_user = db.add_user(message.from_user.id, message.from_user.username, referrer_id)
    
    is_closed, closed_message = db.is_system_closed()
    system_message = db.get_system_message()
    
    welcome_text = "👋 **Добро пожаловать в Magic Work Team!**\n\nЗдесь вы можете сдавать номера для проверки. Выберите действие:"
    
    # Добавляем приветствие для рефералов
    if referrer_id and is_new_user:
        welcome_text = f"👋 **Добро пожаловать!**\n\nВы были приглашены другим пользователем.\n{welcome_text}"
    
    if is_closed:
        welcome_text = f"{closed_message}\n\n{welcome_text}"
    elif system_message:
        welcome_text = f"📢 **Важное сообщение:**\n{system_message}\n\n{welcome_text}"
    
    await message.answer(welcome_text, reply_markup=get_main_menu(message.from_user.id), parse_mode="None")

@dp.message(Command("menu"))
async def menu_cmd(message: types.Message):
    """Команда /menu - показать главное меню"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    is_closed, closed_message = db.is_system_closed()
    system_message = db.get_system_message()
    
    welcome_text = "🏠 **Главное меню**\n\nВыберите действие:"
    
    if is_closed:
        welcome_text = f"{closed_message}\n\n{welcome_text}"
    elif system_message:
        welcome_text = f"📢 **Важное сообщение:**\n{system_message}\n\n{welcome_text}"
    
    await message.answer(welcome_text, reply_markup=get_main_menu(message.from_user.id), parse_mode="None")

@dp.message(Command("profile"))
async def profile_cmd(message: types.Message):
    """Команда /profile - показать профиль с реферальной статистикой"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    stats = db.get_user_stats(message.from_user.id)
    pending_withdrawals = db.get_pending_withdrawals_count()
    referral_stats = db.get_user_referral_stats(message.from_user.id)
    referral_link = db.get_referral_link(message.from_user.id)
    
    text = (f"👤 **Ваш профиль**\n\n"
            f"📝 **Имя:** @{message.from_user.username or 'User'}\n"
            f"🆔 **ID:** `{message.from_user.id}`\n\n"
            f"📊 **Статистика:**\n"
            f"• Сдано номеров: **{stats[0]}**\n"
            f"• Баланс: **${stats[1]:.2f}**\n")
    
    if pending_withdrawals > 0:
        text += f"• Ожидают вывода: **{pending_withdrawals}** заявок\n"
    
    # Реферальная статистика
    if db.is_referral_enabled():
        text += f"\n👥 **Реферальная система:**\n"
        text += f"• Приглашено: **{referral_stats['total_referred']}** чел.\n"
        text += f"• Успешных: **{referral_stats['successful_referred']}** чел.\n"
        text += f"• Заработано: **${referral_stats['earned_bonus']:.2f}**\n"
        text += f"• Бонус за реферала: **${db.get_referral_bonus()}**\n\n"
        text += f"🔗 **Ваша реферальная ссылка:**\n`{referral_link}`\n"
        text += f"📋 Приглашайте друзей и получайте бонусы!"
    else:
        text += f"\n⚠️ **Реферальная система временно отключена**"
    
    text += f"\n💳 **Вывод средств:**\n"
    text += f"Минимальная сумма: **${db.get_min_withdrawal()}**"
    
    buttons = [
        [InlineKeyboardButton(text="👥 Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="💳 Вывод средств", callback_data="withdrawal_menu")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.message(Command("withdraw"))
async def withdraw_cmd(message: types.Message):
    """Команда /withdraw - вывод средств"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    await withdrawal_menu_handler(message)

@dp.message(Command("archive"))
async def archive_cmd(message: types.Message):
    """Старый команда /archive - показать архив номеров БЕЗ ВРЕМЕНИ"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    data = db.get_user_archive(message.from_user.id)
    
    if not data:
        text = "📂 **Архив пуст**\n\nУ вас пока нет завершенных номеров."
    else:
        text = "📂 **История номеров** (последние 15):\n\n"
        for row in data:
            phone, status, tariff_name = row  # Распаковываем 3 элемента
            emo = "✅" if status == "ОТСТОЯЛ" else "❌"
            text += f"{emo} `{phone}` | {tariff_name} | {status}\n"
    
    # Добавляем кнопку для нового интерфейса с датами
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Смотреть по датам", callback_data="archive_dates")],
        [InlineKeyboardButton(text="💬 Чат с отзывами", url="https://t.me/+i2Agn48UehQ0Yzg6")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    await message.answer(text, reply_markup=kb, parse_mode="None")

@dp.callback_query(F.data == "archive")
async def archive_button_handler(callback: CallbackQuery):
    """Обработчик кнопки архива с выбором даты"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Убираем удаление сообщения и используем edit_text вместо создания нового
    await callback.answer()  # Подтверждаем нажатие
    
    data = db.get_user_archive(callback.from_user.id)
    
    if not data:
        text = "📂 **Архив пуст**\n\nУ вас пока нет завершенных номеров."
    else:
        text = "📂 **История номеров** (последние 15):\n\n"
        for row in data:
            phone, status, tariff_name = row  # Распаковываем 3 элемента
            emo = "✅" if status == "ОТСТОЯЛ" else "❌"
            safe_phone = escape_markdown(phone)
            text += f"{emo} `{safe_phone}` | {tariff_name} | {status}\n"
    
    # Добавляем кнопку для нового интерфейса с датами
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Смотреть по датам", callback_data="archive_dates")],
        [InlineKeyboardButton(text="💬 Чат с отзывами", url="https://t.me/+i2Agn48UehQ0Yzg6")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    # Редактируем текущее сообщение вместо создания нового
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")

@dp.callback_query(F.data == "archive_dates")
async def archive_dates_handler(callback: CallbackQuery):
    """Переход к новому интерфейсу с датами"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем список доступных дат для пользователя
    available_dates = db.get_user_available_dates(callback.from_user.id)
    
    if not available_dates:
        text = "📂 **Архив пуст**\n\nУ вас пока нет завершенных номеров."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="archive")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")
        return
    
    text = "📂 **История номеров**\n\n"
    text += "📅 Выберите дату для просмотра или действие:"
    
    buttons = []
    
    # Показываем последние 5 дат
    for i, (date_str,) in enumerate(available_dates[:5]):
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d.%m.%Y')
            # Считаем количество номеров за эту дату
            count = len(db.get_user_numbers_by_date(callback.from_user.id, date_str))
            button_text = f"📅 {formatted_date} ({count})"
        except:
            button_text = f"📅 {date_str}"
        
        buttons.append([InlineKeyboardButton(
            text=button_text, 
            callback_data=f"user_view_date_{date_str}"
        )])
    
    # Кнопка "Все номера"
    all_count = db.cursor.execute(
        "SELECT COUNT(*) FROM numbers WHERE user_id = ?",
        (callback.from_user.id,)
    ).fetchone()[0]
    buttons.append([InlineKeyboardButton(
        text=f"📋 Все номера ({all_count})", 
        callback_data="user_view_all"
    )])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="archive")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data.startswith("user_view_date_"))
async def user_view_date_handler(callback: CallbackQuery):
    """Показать номера пользователя за определенную дату"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    date_str = callback.data.split("user_view_date_")[1]
    
    try:
        # Форматируем дату для отображения
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d.%m.%Y')
    except:
        formatted_date = date_str
    
    # Получаем все номера за эту дату
    numbers = db.get_user_numbers_by_date(callback.from_user.id, date_str)
    
    if not numbers:
        text = f"📭 **Нет номеров за {formatted_date}**"
        buttons = [
            [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="archive")]
        ]
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")
        return
    
    # Если номеров больше 15, предлагаем скачать файл
    if len(numbers) > 15:
        text = f"📂 **Ваши номера за {formatted_date}**\n\n"
        text += f"📊 **Всего номеров:** {len(numbers)}\n"
        text += "⚠️ **Слишком много номеров для отображения в чате.**\n\n"
        text += "📥 **Скачайте файл для просмотра всех номеров:**"
        
        buttons = [
            [InlineKeyboardButton(text="📥 Скачать файл (.txt)", callback_data=f"user_download_date_{date_str}")],
            [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="archive")]
        ]
    else:
        # Считаем статистику
        total = len(numbers)
        success_count = sum(1 for n in numbers if n[1] == "ОТСТОЯЛ")
        fail_count = sum(1 for n in numbers if n[1] == "СЛЕТ")
        
        text = f"📂 **Ваши номера за {formatted_date}**\n\n"
        text += f"📊 **Статистика:**\n"
        text += f"• Всего номеров: {total}\n"
        text += f"• Отстояли: {success_count}\n"
        text += f"• Слетели: {fail_count}\n\n"
        
        text += "📋 **Список номеров:**\n\n"
        
        for i, n in enumerate(numbers, 1):
            phone, status, tariff_name, created_at, finished_at = n
            safe_phone = escape_markdown(phone)
            
            if status == "ОТСТОЯЛ":
                emoji = "✅"
                status_text = "ОТСТОЯЛ"
            else:
                emoji = "❌"
                status_text = "СЛЕТ"
            
            text += f"{emoji} **{i}. {safe_phone}**\n"
            text += f"   📱 Тариф: {tariff_name}\n"
            text += f"   📊 Статус: {status_text}\n\n"  # Убрано время
        
        buttons = [
            [InlineKeyboardButton(text="📥 Скачать полный список (.txt)", callback_data=f"user_download_date_{date_str}")],
            [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="archive")]
        ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "user_view_all")
async def user_view_all_handler(callback: CallbackQuery):
    """Показать все номера пользователя"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем все номера (ограничиваем для показа)
    numbers = db.get_user_numbers_by_date(callback.from_user.id, limit=15)
    all_numbers = db.get_user_numbers_by_date(callback.from_user.id)
    
    if not numbers:
        text = "📭 **У вас пока нет номеров**"
        buttons = [
            [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="archive")]
        ]
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")
        return
    
    # Если номеров больше 15, предлагаем скачать файл
    if len(all_numbers) > 15:
        text = "📂 **Все ваши номера**\n\n"
        text += f"📊 **Всего номеров:** {len(all_numbers)}\n"
        text += "⚠️ **Слишком много номеров для отображения в чате.**\n\n"
        text += "📥 **Скачайте файл для просмотра всех номеров:**"
        
        buttons = [
            [InlineKeyboardButton(text="📥 Скачать файл (.txt)", callback_data="user_download_all")],
            [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="archive")]
        ]
    else:
        # Считаем статистику
        total = len(all_numbers)
        success_count = sum(1 for n in all_numbers if n[1] == "ОТСТОЯЛ")
        fail_count = sum(1 for n in all_numbers if n[1] == "СЛЕТ")
        
        text = "📂 **Все ваши номера**\n\n"
        text += f"📊 **Статистика:**\n"
        text += f"• Всего номеров: {total}\n"
        text += f"• Отстояли: {success_count}\n"
        text += f"• Слетели: {fail_count}\n\n"
        
        text += "📋 **Последние 15 номеров:**\n\n"
        
        for i, n in enumerate(numbers, 1):
            phone, status, tariff_name, created_at, finished_at = n
            safe_phone = escape_markdown(phone)
            
            if status == "ОТСТОЯЛ":
                emoji = "✅"
                status_text = "ОТСТОЯЛ"
            else:
                emoji = "❌"
                status_text = "СЛЕТ"
            
            # Убрано отображение даты и времени
            text += f"{emoji} **{i}. {safe_phone}**\n"
            text += f"   📱 Тариф: {tariff_name}\n"
            text += f"   📊 Статус: {status_text}\n\n"  # Убрано дату и время
        
        buttons = [
            [InlineKeyboardButton(text="📥 Скачать полный список (.txt)", callback_data="user_download_all")],
            [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="archive")]
        ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data.startswith("user_download_date_"))
async def user_download_date_handler(callback: CallbackQuery):
    """Скачать номера пользователя за определенную дату"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    date_str = callback.data.split("user_download_date_")[1]
    
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d.%m.%Y')
        file_date = date_obj.strftime('%Y-%m-%d')
    except:
        formatted_date = date_str
        file_date = date_str
    
    # Получаем номера за дату
    numbers = db.get_user_numbers_by_date(callback.from_user.id, date_str)
    
    if not numbers:
        await callback.answer("📭 Нет номеров для скачивания", show_alert=True)
        return
    
    # Создаем файл
    filename = f"numbers_{callback.from_user.id}_{file_date}.txt"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write(f"ВАШИ НОМЕРА ЗА {formatted_date}\n")
        f.write(f"Дата экспорта: {datetime.now().strftime('%d.%m.%Y')}\n")
        f.write(f"Всего номеров: {len(numbers)}\n")
        f.write("=" * 60 + "\n\n")
        
        # Статистика
        success_count = sum(1 for n in numbers if n[1] == "ОТСТОЯЛ")
        fail_count = sum(1 for n in numbers if n[1] == "СЛЕТ")
        
        f.write(f"СТАТИСТИКА:\n")
        f.write(f"  Всего номеров: {len(numbers)}\n")
        f.write(f"  Отстояли: {success_count}\n")
        f.write(f"  Слетели: {fail_count}\n")
        f.write("-" * 60 + "\n\n")
        
        # Список номеров БЕЗ ВРЕМЕНИ
        f.write("СПИСОК НОМЕРОВ:\n\n")
        f.write(f"{'№':<3} {'Номер':<15} {'Тариф':<15} {'Статус':<10}\n")
        f.write("-" * 45 + "\n")
        
        for i, n in enumerate(numbers, 1):
            phone, status, tariff_name, created_at, finished_at = n
            f.write(f"{i:<3} {phone:<15} {tariff_name:<15} {status:<10}\n")
    
    try:
        # Отправляем файл
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📂 **Ваши номера за {formatted_date}**\n\n"
                   f"📊 Всего номеров: {len(numbers)}\n"
                   f"📅 Дата: {formatted_date}",
            parse_mode="None"
        )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка отправки файла", show_alert=True)
    finally:
        # Удаляем временный файл
        if os.path.exists(filename):
            os.remove(filename)

@dp.callback_query(F.data == "user_download_all")
async def user_download_all_handler(callback: CallbackQuery):
    """Скачать все номера пользователя"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем все номера
    numbers = db.get_user_numbers_by_date(callback.from_user.id)
    
    if not numbers:
        await callback.answer("📭 Нет номеров для скачивания", show_alert=True)
        return
    
    # Создаем файл
    filename = f"numbers_{callback.from_user.id}_all.txt"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("ВСЕ ВАШИ НОМЕРА\n")
        f.write(f"Дата экспорта: {datetime.now().strftime('%d.%m.%Y')}\n")
        f.write(f"Всего номеров: {len(numbers)}\n")
        f.write("=" * 60 + "\n\n")
        
        # Статистика
        success_count = sum(1 for n in numbers if n[1] == "ОТСТОЯЛ")
        fail_count = sum(1 for n in numbers if n[1] == "СЛЕТ")
        
        f.write(f"СТАТИСТИКА:\n")
        f.write(f"  Всего номеров: {len(numbers)}\n")
        f.write(f"  Отстояли: {success_count}\n")
        f.write(f"  Слетели: {fail_count}\n")
        f.write("-" * 60 + "\n\n")
        
        # Список номеров по датам (без времени)
        dates = {}
        for n in numbers:
            phone, status, tariff_name, created_at, finished_at = n
            date_str = created_at.split()[0] if created_at else "Неизвестно"
            if date_str not in dates:
                dates[date_str] = []
            dates[date_str].append((phone, status, tariff_name))
        
        for date_str, date_numbers in sorted(dates.items(), reverse=True):
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d.%m.%Y')
            except:
                formatted_date = date_str
            
            f.write(f"\n📅 ДАТА: {formatted_date} ({len(date_numbers)} номеров)\n")
            f.write("-" * 50 + "\n")
            
            for i, n in enumerate(date_numbers, 1):
                phone, status, tariff_name = n  # Убрано время
                f.write(f"  {i}. {phone} | {tariff_name} | {status}\n")
            
            f.write("\n")
    
    try:
        # Отправляем файл
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📂 **Все ваши номера**\n\n"
                   f"📊 Всего номеров: {len(numbers)}\n"
                   f"📅 Дата экспорта: {datetime.now().strftime('%d.%m.%Y')}",
            parse_mode="None"
        )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка отправки файла", show_alert=True)
    finally:
        # Удаляем временный файл
        if os.path.exists(filename):
            os.remove(filename)

@dp.message(Command("queue"))
async def queue_cmd(message: types.Message):
    """Команда /queue - показать текущую очередь"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    user_id = message.from_user.id
    total_count = db.get_queue_count()
    user_pos = db.get_user_position(user_id)
    user_numbers_count = db.cursor.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND status = 'Ожидание'", (user_id,)).fetchone()[0]
    
    # Проверяем, является ли пользователь главным админом
    is_main_admin = user_id in ADMIN_IDS
    
    text = f"📊 **Текущая очередь**\n\n"
    text += f"🔢 **Всего номеров в ожидании:** {total_count}\n\n"
    
    if is_main_admin:
        text += f"⭐ **Вы - главный администратор**\n"
        text += f"📍 **Ваши номера обрабатываются в первую очередь!**\n\n"
    
    if user_numbers_count > 0:
        text += f"👤 **Ваших номеров в очереди:** {user_numbers_count}\n"
        if user_pos:
            text += f"📍 **Позиция ближайшего номера:** {user_pos}-й\n\n"
        text += f"⏳ **Ожидайте уведомления от оператора.**"
    else:
        text += "📭 **Ваших номеров сейчас нет в очереди.**"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Сдать номер", callback_data="give_number")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    await message.answer(text, reply_markup=kb, parse_mode="None")

@dp.callback_query(F.data == "queue")
async def queue_button_handler(callback: CallbackQuery):
    """Обработчик кнопки очереди"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    user_id = callback.from_user.id
    total_count = db.get_queue_count()
    user_pos = db.get_user_position(user_id)
    user_numbers_count = db.get_user_active_numbers_count(user_id)
    
    # Получаем активные номера пользователя
    active_numbers = db.cursor.execute(
        "SELECT phone FROM numbers WHERE user_id = ? AND status = 'Ожидание'",
        (user_id,)
    ).fetchall()
    
    text = f"📊 **Текущая очередь**\n\n"
    text += f"🔢 **Всего номеров в ожидании:** {total_count}\n\n"
    
    if user_numbers_count > 0:
        text += f"👤 **Ваших номеров в очереди:** {user_numbers_count}\n"
        if user_pos:
            text += f"📍 **Позиция ближайшего номера:** {user_pos}-й\n\n"
        
        if active_numbers:
            text += "📱 **Ваши номера в очереди:**\n"
            for i, (phone,) in enumerate(active_numbers[:5], 1):  # Показываем до 5 номеров
                safe_phone = escape_markdown(phone)
                text += f"{i}. `{safe_phone}`\n"
            
            if len(active_numbers) > 5:
                text += f"... и еще {len(active_numbers) - 5} номеров\n"
            
            text += f"\n"
        
        text += f"⏳ **Ожидайте уведомления от оператора.**"
    else:
        text += "📭 **Ваших номеров сейчас нет в очереди.**"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Сдать номер", callback_data="give_number")],
        [InlineKeyboardButton(text="📋 Мои активные", callback_data="check_active_number")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")

@dp.message(Command("give"))
async def give_cmd(message: types.Message):
    """Команда /give - сдать номер (только Казахстан)"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    is_closed, closed_message = db.is_system_closed()
    if is_closed:
        await message.answer(closed_message, reply_markup=get_main_menu(), parse_mode="None")
        return
    
    tariffs = db.get_active_tariffs()
    p_price, p_name = db.get_priority_settings()
    
    if not tariffs:
        await message.answer("❌ На данный момент нет доступных тарифов.", reply_markup=get_main_menu())
        return
    
    buttons = []
    for t in tariffs:
        buttons.append([InlineKeyboardButton(text=f"{t[1]} ({t[3]}м/${t[2]})", callback_data=f"tariff_{t[0]}_0")])
        total_p_price = t[2] + p_price
        buttons.append([InlineKeyboardButton(text=f"⭐ {p_name} (${total_p_price})", callback_data=f"tariff_{t[0]}_1")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    
    await message.answer(
        "💰 *Выберите тип сдачи номера:*\n\n"
        "🇰🇿 *Принимаются только номера Казахстана*\n"
        "Формат: +7XXXXXXXXXX, 8XXXXXXXXXX или 7XXXXXXXXXX",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@dp.callback_query(F.data == "give_number")
async def give_number_button_handler(callback: CallbackQuery):
    """Обработчик кнопки 'Сдать номер'"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем количество активных номеров пользователя
    active_count = db.get_user_active_numbers_count(callback.from_user.id)
    
    # Получаем список активных номеров пользователя
    active_numbers = db.cursor.execute(
        "SELECT phone, created_at FROM numbers WHERE user_id = ? AND status = 'Ожидание' ORDER BY created_at DESC LIMIT 5",
        (callback.from_user.id,)
    ).fetchall()
    
    is_closed, closed_message = db.is_system_closed()
    if is_closed:
        await callback.message.edit_text(closed_message, reply_markup=get_main_menu(), parse_mode="None")
        return
    
    tariffs = db.get_active_tariffs()
    p_price, p_name = db.get_priority_settings()
    
    if not tariffs:
        await callback.message.edit_text("❌ На данный момент нет доступных тарифов.", reply_markup=get_main_menu())
        return
    
    # Показываем информацию об активных номерах
    if active_count > 0:
        text = f"📊 *У вас {active_count} номеров в очереди*\n\n"
        if active_numbers:
            text += "📱 *Активные номера:*\n"
            for i, (phone, created_at) in enumerate(active_numbers, 1):
                created_time = created_at.split()[1][:5] if created_at else "—"
                text += f"{i}. `{phone}` (сдан в {created_time})\n"
            text += "\n"
    else:
        text = "📭 *У вас нет номеров в очереди*\n\n"
    
    text += "💰 **Выберите тип сдачи номера:**\n"
    text += "⚠️ **Можно сдавать разные номера, но не повторять один и тот же**"
    
    buttons = []
    for t in tariffs:
        buttons.append([InlineKeyboardButton(text=f"{t[1]} ({t[3]}м/${t[2]})", callback_data=f"tariff_{t[0]}_0")])
        total_p_price = t[2] + p_price
        buttons.append([InlineKeyboardButton(text=f"⭐ {p_name} (${total_p_price})", callback_data=f"tariff_{t[0]}_1")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.message(Command("tariffs"))
async def tariffs_cmd(message: types.Message):
    """Команда /tariffs - показать доступные тарифы"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    is_closed, closed_message = db.is_system_closed()
    if is_closed:
        await message.answer(closed_message, reply_markup=get_main_menu(), parse_mode="None")
        return
    
    tariffs = db.get_active_tariffs()
    p_price, p_name = db.get_priority_settings()
    
    if not tariffs:
        await message.answer("❌ На данный момент нет доступных тарифов.", reply_markup=get_main_menu())
        return
    
    text = "💰 **Доступные тарифы:**\n\n"
    for t in tariffs:
        text += f"📱 **{t[1]}**\n"
        text += f"   ⏱ Время: {t[3]} минут\n"
        text += f"   💰 Цена: ${t[2]}\n"
        text += f"   ⭐ {p_name}: ${t[2] + p_price}\n\n"
    
    buttons = [
        [InlineKeyboardButton(text="📱 Сдать номер", callback_data="give_number")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "show_tariffs")
async def show_tariffs_button_handler(callback: CallbackQuery):
    """Обработчик кнопки 'Тарифы'"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    is_closed, closed_message = db.is_system_closed()
    if is_closed:
        await callback.message.edit_text(closed_message, reply_markup=get_main_menu(), parse_mode="None")
        return
    
    tariffs = db.get_active_tariffs()
    p_price, p_name = db.get_priority_settings()
    
    if not tariffs:
        await callback.message.edit_text("❌ На данный момент нет доступных тарифов.", reply_markup=get_main_menu())
        return
    
    text = "💰 **Доступные тарифы:**\n\n"
    for t in tariffs:
        text += f"📱 **{t[1]}**\n"
        text += f"   ⏱ Время: {t[3]} минут\n"
        text += f"   💰 Цена: ${t[2]}\n"
        text += f"   ⭐ {p_name}: ${t[2] + p_price}\n\n"
    
    buttons = [
        [InlineKeyboardButton(text="📱 Сдать номер", callback_data="give_number")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.message(Command("help"))
async def help_cmd(message: types.Message):
    """Команда /help - помощь"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    help_text = (
        "🆘 **Помощь по командам**\n\n"
        "📋 **Основные команды:**\n"
        "• /start - Начать работу с ботом\n"
        "• /menu - Главное меню\n"
        "• /profile - Мой профиль\n"
        "• /give - Сдать номер\n"
        "• /queue - Текущая очередь\n"
        "• /archive - История номеров\n"
        "• /tariffs - Доступные тарифы\n"
        "• /withdraw - Вывод средств\n"
        "• /ref - Реферальная система\n"
        "• /myactive - Проверить активный номер\n"
        "• /help - Эта справка\n\n"
        "📞 **Техподдержка:** @magic_team_support\n\n"
        "⚡ **Быстрые действия через кнопки:**\n"
        "Используйте кнопки меню для быстрого доступа ко всем функциям!"
    )
    
    await message.answer(help_text, reply_markup=get_main_menu(), parse_mode="None")

@dp.message(Command("myactive"))
async def myactive_cmd(message: types.Message):
    """Команда для проверки активного номера в очереди"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    # Получаем активные номера пользователя
    active_numbers = db.cursor.execute(
        """
        SELECT n.phone, n.created_at, t.name, n.is_priority 
        FROM numbers n 
        LEFT JOIN tariffs t ON n.tariff_id = t.id 
        WHERE n.user_id = ? AND n.status = 'Ожидание' 
        ORDER BY n.created_at DESC
        """,
        (message.from_user.id,)
    ).fetchall()
    
    if not active_numbers:
        await message.answer("📭 *У вас нет активных номеров в очереди.*\n\nВы можете сдать новый номер через меню.", 
                           reply_markup=get_main_menu(), parse_mode="None")
        return
    
    # Получаем позицию первого номера в очереди
    user_pos = db.get_user_position(message.from_user.id)
    
    text = f"📋 *Ваши активные номера в очереди* ({len(active_numbers)} шт.)\n\n"
    
    if user_pos:
        text += f"📍 *Позиция ближайшего номера:* {user_pos}-й\n\n"
    
    for i, (phone, created_at, tariff_name, is_priority) in enumerate(active_numbers, 1):
        created_time = created_at.split()[1][:5] if created_at else "—"
        created_date = created_at.split()[0] if created_at else "—"
        priority_mark = "⭐ " if is_priority else ""
        
        text += f"{i}. {priority_mark}`{phone}`\n"
        text += f"   📅 {created_date} в {created_time} | {tariff_name}\n\n"
    
    text += "⏳ *Ожидайте уведомления от оператора.*\n\n"
    text += "⚠️ *Правила:*\n"
    text += "• Можно сдавать несколько разных номеров\n"
    text += "• Приоритетные номера (⭐) обрабатываются в первую очередь"
    
    buttons = [
        [InlineKeyboardButton(text="📱 Сдать еще номер", callback_data="give_number")],
        [InlineKeyboardButton(text="📊 Проверить очередь", callback_data="queue")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

# ============================================
# РЕФЕРАЛЬНАЯ СИСТЕМА
# ============================================

@dp.message(Command("ref"))
async def ref_cmd(message: types.Message):
    """Команда /ref - показать реферальную систему"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    if not db.is_referral_enabled():
        await message.answer("⚠️ **Реферальная система временно отключена**", parse_mode="None")
        return
    
    referral_stats = db.get_user_referral_stats(message.from_user.id)
    referral_link = db.get_referral_link(message.from_user.id)
    bonus_amount = db.get_referral_bonus()
    
    text = f"👥 **Реферальная система**\n\n"
    text += f"💰 **Бонус за приглашение:** ${bonus_amount}\n"
    text += f"📊 **Ваша статистика:**\n"
    text += f"• Всего приглашено: **{referral_stats['total_referred']}**\n"
    text += f"• Успешных рефералов: **{referral_stats['successful_referred']}**\n"
    text += f"• Заработано: **${referral_stats['earned_bonus']:.2f}**\n\n"
    
    text += f"🔗 **Ваша реферальная ссылка:**\n`{referral_link}`\n\n"
    text += f"📝 **Как это работает:**\n"
    text += f"1. Отправьте друзьям вашу ссылку\n"
    text += f"2. Друг регистрируется по вашей ссылке\n"
    text += f"3. Когда друг успешно отстоит свой ПЕРВЫЙ номер\n"
    text += f"4. Вы получаете **${bonus_amount}** на баланс!\n\n"
    text += f"💡 **Совет:** Чем больше друзей вы пригласите, тем больше заработаете!"
    
    buttons = [
        [InlineKeyboardButton(text="📋 Мои рефералы", callback_data="my_referrals")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "referral_system")
async def referral_system_handler(callback: CallbackQuery):
    """Обработчик кнопки реферальной системы"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    if not db.is_referral_enabled():
        await callback.answer("⚠️ Реферальная система временно отключена", show_alert=True)
        return
    
    referral_stats = db.get_user_referral_stats(callback.from_user.id)
    referral_link = db.get_referral_link(callback.from_user.id)
    bonus_amount = db.get_referral_bonus()
    
    # Получаем детальную информацию о рефералах
    detailed_refs = db.get_user_referrals_detailed(callback.from_user.id)
    
    text = f"👥 **Реферальная система**\n\n"
    text += f"💰 **Бонус за приглашение:** ${bonus_amount}\n"
    text += f"📊 **Ваша статистика:**\n"
    text += f"• Всего приглашено: **{referral_stats['total_referred']}**\n"
    text += f"• Успешных рефералов: **{referral_stats['successful_referred']}**\n"
    text += f"• Заработано: **${referral_stats['earned_bonus']:.2f}**\n\n"
    
    text += f"🔗 **Ваша реферальная ссылка:**\n`{referral_link}`\n\n"
    text += f"📝 **Как это работает:**\n"
    text += f"1. Отправьте друзьям вашу ссылку\n"
    text += f"2. Друг регистрируется по вашей ссылке\n"
    text += f"3. Когда друг успешно отстоит свой ПЕРВЫЙ номер\n"
    text += f"4. Вы получаете **${bonus_amount}** на баланс!\n\n"
    
    if detailed_refs:
        text += f"📋 **Ваши рефералы:**\n"
        for i, ref in enumerate(detailed_refs[:10], 1):  # Показываем первые 10
            ref_id, username, created_at, has_completed, bonus_paid = ref
            status = "✅ Отстоял" if has_completed else "⏳ В процессе"
            safe_username = escape_markdown(username or f"ID{ref_id}")
            created_date = created_at.split()[0] if created_at else "—"
            text += f"{i}. @{safe_username} - {status} ({created_date})\n"
        
        if len(detailed_refs) > 10:
            text += f"\n... и еще {len(detailed_refs) - 10} рефералов\n"
    
    buttons = [
        [InlineKeyboardButton(text="📋 Мои рефералы", callback_data="my_referrals")],
        [InlineKeyboardButton(text="⬅️ Назад в профиль", callback_data="profile")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "my_referrals")
async def my_referrals_handler(callback: CallbackQuery):
    """Детальный список рефералов"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    detailed_refs = db.get_user_referrals_detailed(callback.from_user.id)
    
    if not detailed_refs:
        text = "📋 **У вас пока нет рефералов**\n\nПригласите друзей по вашей реферальной ссылке!"
    else:
        text = "📋 **Ваши рефералы:**\n\n"
        for i, ref in enumerate(detailed_refs, 1):
            ref_id, username, created_at, has_completed, bonus_paid = ref
            status = "✅ Отстоял номер" if has_completed else "⏳ Еще не отстоял"
            bonus_status = "💰 Бонус выплачен" if bonus_paid else "⏳ Бонус ожидается"
            safe_username = escape_markdown(username or f"ID{ref_id}")
            created_date = created_at.split()[0] if created_at else "—"
            text += f"{i}. **@{safe_username}**\n"
            text += f"   🆔 ID: `{ref_id}`\n"
            text += f"   📅 Регистрация: {created_date}\n"
            text += f"   📊 Статус: {status}\n"
            text += f"   💰 {bonus_status}\n\n"
    
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="referral_system")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "check_active_number")
async def check_active_number_handler(callback: CallbackQuery):
    """Проверить активный номер в очереди"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем активные номера пользователя
    active_numbers = db.cursor.execute(
        """
        SELECT n.phone, n.created_at, t.name, n.is_priority 
        FROM numbers n 
        LEFT JOIN tariffs t ON n.tariff_id = t.id 
        WHERE n.user_id = ? AND n.status = 'Ожидание' 
        ORDER BY n.created_at DESC
        """,
        (callback.from_user.id,)
    ).fetchall()
    
    if not active_numbers:
        await callback.message.edit_text(
            "📭 *У вас нет активных номеров в очереди.*\n\nВы можете сдать новый номер через меню.", 
            reply_markup=get_main_menu(), 
            parse_mode="None"
        )
        return
    
    # Получаем позицию первого номера в очереди
    user_pos = db.get_user_position(callback.from_user.id)
    
    text = f"📋 *Ваши активные номера в очереди* ({len(active_numbers)} шт.)\n\n"
    
    if user_pos:
        text += f"📍 *Позиция ближайшего номера:* {user_pos}-й\n\n"
    
    for i, (phone, created_at, tariff_name, is_priority) in enumerate(active_numbers, 1):
        created_time = created_at.split()[1][:5] if created_at else "—"
        created_date = created_at.split()[0] if created_at else "—"
        priority_mark = "⭐ " if is_priority else ""
        
        text += f"{i}. {priority_mark}`{phone}`\n"
        text += f"   📅 {created_date} в {created_time} | {tariff_name}\n\n"
    
    text += "⏳ *Ожидайте уведомления от оператора.*\n\n"
    text += "⚠️ *Правила:*\n"
    text += "• Можно сдавать несколько разных номеров\n"
    text += "• Приоритетные номера (⭐) обрабатываются в первую очередь\n"
    text += "• Вы можете удалить номер из очереди, если передумали"
    
    buttons = [
        [InlineKeyboardButton(text="❌ Удалить номер из очереди", callback_data="remove_number_start")],
        [InlineKeyboardButton(text="📱 Сдать еще номер", callback_data="give_number")],
        [InlineKeyboardButton(text="📊 Проверить очередь", callback_data="queue")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "remove_number_start")
async def remove_number_start_handler(callback: CallbackQuery, state: FSMContext):
    """Начать процесс удаления номера"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем активные номера пользователя
    active_numbers = db.cursor.execute(
        "SELECT phone FROM numbers WHERE user_id = ? AND status = 'Ожидание'",
        (callback.from_user.id,)
    ).fetchall()
    
    if not active_numbers:
        await callback.answer("📭 У вас нет номеров в очереди для удаления", show_alert=True)
        return
    
    # Создаем клавиатуру с номерами для удаления
    buttons = []
    for phone, in active_numbers[:10]:  # Ограничиваем 10 номерами
        safe_phone = escape_markdown(phone)
        buttons.append([
            InlineKeyboardButton(text=f"📱 {phone[:10]}...", callback_data=f"remove_confirm_{phone}")
        ])
    
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="check_active_number")])
    
    await callback.message.edit_text(
        "❌ **Удаление номера из очереди**\n\n"
        "Выберите номер для удаления:\n\n"
        "⚠️ *Внимание:* После удаления номер нельзя будет восстановить.\n"
        "Позиция других ваших номеров в очереди может измениться.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="None"
    )

@dp.callback_query(F.data.startswith("remove_confirm_"))
async def remove_confirm_handler(callback: CallbackQuery):
    """Подтверждение удаления номера"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    # Получаем номер из callback_data
    phone = callback.data.replace("remove_confirm_", "")
    
    # Создаем клавиатуру подтверждения
    buttons = [
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"remove_execute_{phone}"),
            InlineKeyboardButton(text="❌ Нет, отмена", callback_data="remove_number_start")
        ]
    ]
    
    await callback.message.edit_text(
        f"⚠️ **Подтверждение удаления**\n\n"
        f"Вы действительно хотите удалить номер `{escape_markdown(phone)}` из очереди?\n\n"
        f"*Это действие нельзя отменить!*",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="None"
    )

@dp.callback_query(F.data.startswith("remove_execute_"))
async def remove_execute_handler(callback: CallbackQuery):
    """Выполнение удаления номера"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    phone = callback.data.replace("remove_execute_", "")
    
    # Удаляем номер из очереди
    success, message = db.remove_user_number_from_queue(callback.from_user.id, phone)
    
    if success:
        # Уменьшаем фейковую очередь при удалении реального номера
        decreased, amount, new_fake = decrease_fake_queue_on_number_taken()
        
        text = f"✅ **Номер удален**\n\n"
        text += f"📱 `{escape_markdown(phone)}` успешно удален из очереди.\n"
        text += "💰 *Средства не возвращаются.*\n\n"
        text += "📊 **Обновите список активных номеров**"
        
        buttons = [
            [InlineKeyboardButton(text="📋 Обновить список", callback_data="check_active_number")],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")]
        ]
    else:
        text = f"❌ **Ошибка**\n\n{message}"
        buttons = [
            [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="remove_number_start")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="check_active_number")]
        ]
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="None"
    )

@dp.message(Command("cancel"))
async def cancel_cmd(message: types.Message, state: FSMContext):
    """Отмена текущего действия"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    current_state = await state.get_state()
    
    if current_state == Form.waiting_for_remove_phone:
        await state.clear()
        await message.answer(
            "❌ Удаление номера отменено.",
            reply_markup=get_main_menu(message.from_user.id),
            parse_mode="None"
        )
    elif current_state:
        await state.clear()
        await message.answer(
            "❌ Текущее действие отменено.",
            reply_markup=get_main_menu(message.from_user.id),
            parse_mode="None"
        )
    else:
        await message.answer(
            "ℹ️ Нет активных действий для отмены.",
            reply_markup=get_main_menu(message.from_user.id),
            parse_mode="None"
        )

# ============================================
# ВЫВОД СРЕДСТВ - ПОЛЬЗОВАТЕЛЬСКИЙ ИНТЕРФЕЙС
# ============================================

async def withdrawal_menu_handler(message: types.Message | types.CallbackQuery):
    """Меню вывода средств"""
    if isinstance(message, types.CallbackQuery):
        user_id = message.from_user.id
        callback = message
        is_callback = True
    else:
        user_id = message.from_user.id
        callback = None
        is_callback = False
    
    if db.is_user_banned(user_id): 
        return
    
    balance = db.get_user_balance(user_id)
    min_withdrawal = db.get_min_withdrawal()
    payment_methods = db.get_payment_methods()
    
    text = (f"💳 **Вывод средств**\n\n"
            f"💰 **Ваш баланс:** ${balance:.2f}\n"
            f"📊 **Минимальная сумма:** ${min_withdrawal}\n"
            f"💳 **Доступные методы:** {', '.join(payment_methods)}\n\n"
            f"Выберите действие:")
    
    buttons = [
        [InlineKeyboardButton(text="📥 Запросить вывод", callback_data="withdrawal_request")],
        [InlineKeyboardButton(text="📋 История заявок", callback_data="withdrawal_history")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile")]
    ]
    
    if is_callback:
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")
    else:
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "withdrawal_menu")
async def withdrawal_menu_callback_handler(callback: CallbackQuery):
    """Обработчик кнопки меню вывода средств"""
    await withdrawal_menu_handler(callback)

@dp.callback_query(F.data == "withdrawal_request")
async def withdrawal_request_handler(callback: CallbackQuery, state: FSMContext):
    """Запрос на вывод средств"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    balance = db.get_user_balance(callback.from_user.id)
    min_withdrawal = db.get_min_withdrawal()
    
    if balance < min_withdrawal:
        await callback.answer(f"❌ Минимальная сумма для вывода: ${min_withdrawal:.2f}", show_alert=True)
        return
    
    # Проверяем, есть ли уже ожидающая заявка
    pending_withdrawals = db.cursor.execute(
        "SELECT COUNT(*) FROM withdrawals WHERE user_id = ? AND status = 'pending'", 
        (callback.from_user.id,)
    ).fetchone()[0]
    
    if pending_withdrawals > 0:
        await callback.answer("❌ У вас уже есть ожидающая заявка на вывод. Дождитесь ее обработки.", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_withdrawal_amount)
    await state.update_data(user_id=callback.from_user.id, username=callback.from_user.username)
    
    text = (f"💰 **Запрос на вывод средств**\n\n"
            f"💳 **Ваш баланс:** ${balance:.2f}\n"
            f"📊 **Минимальная сумма:** ${min_withdrawal}\n\n"
            f"Введите сумму для вывода ($):")
    
    buttons = [
        [InlineKeyboardButton(text="❌ Отмена", callback_data="withdrawal_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.message(Form.waiting_for_withdrawal_amount)
async def process_withdrawal_amount(message: types.Message, state: FSMContext):
    """Обработка суммы для вывода"""
    if db.is_user_banned(message.from_user.id): 
        await state.clear()
        return
    
    try:
        amount = float(message.text.replace(',', '.'))
        min_withdrawal = db.get_min_withdrawal()
        
        if amount < min_withdrawal:
            await message.answer(f"❌ Минимальная сумма для вывода: ${min_withdrawal:.2f}")
            return
        
        balance = db.get_user_balance(message.from_user.id)
        if amount > balance:
            await message.answer(f"❌ Недостаточно средств. Ваш баланс: ${balance:.2f}")
            return
        
        await state.update_data(amount=amount)
        await state.set_state(Form.waiting_for_withdrawal_method)
        
        # Показывать доступные методы оплаты
        payment_methods = db.get_payment_methods()
        buttons = []
        for method in payment_methods:
            buttons.append([InlineKeyboardButton(text=method, callback_data=f"withdraw_method_{method}")])
        
        buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="withdrawal_menu")])
        
        await message.answer(
            f"✅ Сумма: ${amount:.2f}\n\n"
            f"💳 **Выберите способ получения:**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="None"
        )
        
    except ValueError:
        await message.answer("❌ Введите число! Например: 50 или 25.5")

@dp.callback_query(F.data.startswith("withdraw_method_"))
async def process_withdrawal_method(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора метода оплаты"""
    if db.is_user_banned(callback.from_user.id): 
        await state.clear()
        return
    
    method = callback.data.replace("withdraw_method_", "")
    await state.update_data(payment_method=method)
    await state.set_state(Form.waiting_for_withdrawal_details)
    
    instructions = {
        "QIWI": "Введите номер QIWI кошелька (формат: +79123456789)",
        "Карта": "Введите номер карты (формат: 1234 5678 9012 3456)",
        "ЮMoney": "Введите номер кошелька ЮMoney",
        "USDT": "Введите адрес кошелька USDT (TRC20)"
    }
    
    instruction = instructions.get(method, f"Введите реквизиты для {method}")
    
    await callback.message.edit_text(
        f"💳 **Способ:** {method}\n\n"
        f"{instruction}:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="withdrawal_menu")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_withdrawal_details)
async def process_withdrawal_details(message: types.Message, state: FSMContext):
    """Обработка реквизитов для вывода"""
    if db.is_user_banned(message.from_user.id): 
        await state.clear()
        return
    
    payment_details = message.text.strip()
    
    if not payment_details:
        await message.answer("❌ Пожалуйста, введите реквизиты")
        return
    
    await state.update_data(payment_details=payment_details)
    
    data = await state.get_data()
    
    # Показываем подтверждение
    text = (
        f"📋 **Подтверждение заявки на вывод**\n\n"
        f"💰 **Сумма:** ${data['amount']:.2f}\n"
        f"💳 **Способ:** {data['payment_method']}\n"
        f"📝 **Реквизиты:** {data['payment_details']}\n\n"
        f"❓ **Всё верно?**"
    )
    
    buttons = [
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_withdrawal")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="withdrawal_menu")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "confirm_withdrawal")
async def confirm_withdrawal_handler(callback: CallbackQuery, state: FSMContext):
    """Подтверждение заявки на вывод"""
    if db.is_user_banned(callback.from_user.id): 
        await state.clear()
        return
    
    data = await state.get_data()
    
    success, message_text = db.create_withdrawal(
        user_id=data['user_id'],
        username=data['username'],
        amount=data['amount'],
        payment_method=data['payment_method'],
        payment_details=data['payment_details']
    )
    
    if success:
        # Уведомление пользователя
        await callback.message.edit_text(
            f"✅ **Заявка на вывод создана!**\n\n"
            f"💰 **Сумма:** ${data['amount']:.2f}\n"
            f"💳 **Способ:** {data['payment_method']}\n"
            f"📝 **Реквизиты:** {data['payment_details']}\n\n"
            f"⏳ Заявка будет обработана администратором в ближайшее время.",
            parse_mode="None"
        )
        
        # Уведомление главных админов
        pending_count = db.get_pending_withdrawals_count()
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🔔 **Новая заявка на вывод!**\n\n"
                    f"👤 **Пользователь:** @{data['username'] or data['user_id']}\n"
                    f"💰 **Сумма:** ${data['amount']:.2f}\n"
                    f"💳 **Способ:** {data['payment_method']}\n"
                    f"📝 **Реквизиты:** {data['payment_details']}\n\n"
                    f"📊 **Всего ожидающих заявок:** {pending_count}",
                    parse_mode="None"
                )
            except:
                pass
    else:
        await callback.message.edit_text(f"❌ {message_text}", parse_mode="None")
    
    await state.clear()

@dp.callback_query(F.data == "withdrawal_history")
async def withdrawal_history_handler(callback: CallbackQuery):
    """История заявок на вывод"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    withdrawals = db.get_user_withdrawals(callback.from_user.id, limit=10)
    
    if not withdrawals:
        text = "📋 **История заявок на вывод пуста**\n\nУ вас пока нет заявок на вывод средств."
    else:
        text = "📋 **Ваши заявки на вывод** (последние 10):\n\n"
        
        for w in withdrawals:
            w_id, amount, status, method, details, created_at, processed_at, comment = w
            
            # Экранируем реквизиты
            safe_details = escape_markdown(details) if details else "—"
            
            # Определяем статус
            if status == 'pending':
                status_emoji = "⏳"
                status_text = "ОЖИДАНИЕ"
            elif status == 'approved':
                status_emoji = "✅"
                status_text = "ОДОБРЕНО"
            else:  # rejected
                status_emoji = "❌"
                status_text = "ОТКЛОНЕНО"
            
            # Форматируем дату
            created_date = created_at.split()[0] if created_at else "—"
            
            text += f"{status_emoji} **Заявка #{w_id}**\n"
            text += f"💰 **Сумма:** ${amount:.2f}\n"
            text += f"💳 **Способ:** {method}\n"
            text += f"📅 **Дата:** {created_date}\n"
            text += f"📊 **Статус:** {status_text}\n"
            
            if comment and status != 'pending':
                text += f"💬 **Комментарий:** {comment}\n"
            
            text += "─" * 20 + "\n"
    
    buttons = [
        [InlineKeyboardButton(text="📥 Новая заявка", callback_data="withdrawal_request")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="withdrawal_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

# ============================================
# ОБРАБОТЧИКИ ПРОФИЛЯ С ВЫВОДОМ
# ============================================

@dp.callback_query(F.data == "profile")
async def profile_button_handler(callback: CallbackQuery):
    """Обработчик кнопки профиля с информацией о выводе"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    stats = db.get_user_stats(callback.from_user.id)
    pending_withdrawals = db.get_pending_withdrawals_count()
    referral_stats = db.get_user_referral_stats(callback.from_user.id)
    referral_link = db.get_referral_link(callback.from_user.id)
    
    # Получаем количество активных номеров
    active_count = db.get_user_active_numbers_count(callback.from_user.id)
    
    text = (f"👤 **Ваш профиль**\n\n"
            f"📝 **Имя:** @{callback.from_user.username or 'User'}\n"
            f"🆔 **ID:** `{callback.from_user.id}`\n\n"
            f"📊 **Статистика:**\n"
            f"• Сдано номеров: **{stats[0]}**\n"
            f"• Активных в очереди: **{active_count}**\n"
            f"• Баланс: **${stats[1]:.2f}**\n")
    
    if pending_withdrawals > 0:
        text += f"• Ожидают вывода: **{pending_withdrawals}** заявок\n"
    
    # Реферальная статистика
    if db.is_referral_enabled():
        text += f"\n👥 **Реферальная система:**\n"
        text += f"• Приглашено: **{referral_stats['total_referred']}** чел.\n"
        text += f"• Успешных: **{referral_stats['successful_referred']}** чел.\n"
        text += f"• Заработано: **${referral_stats['earned_bonus']:.2f}**\n"
        text += f"• Бонус за реферала: **${db.get_referral_bonus()}**\n\n"
        text += f"🔗 **Ваша реферальная ссылка:**\n`{referral_link}`\n"
        text += f"📋 Приглашайте друзей и получайте бонусы!"
    else:
        text += f"\n⚠️ **Реферальная система временно отключена**"
    
    text += f"\n💳 **Вывод средств:**\n"
    text += f"Минимальная сумма: **${db.get_min_withdrawal()}**\n\n"
    text += f"⚠️ **Правила сдачи номеров:**\n"
    text += f"• Можно сдавать несколько разных номеров"
    
    buttons = [
        [InlineKeyboardButton(text="📋 Мои активные номера", callback_data="check_active_number")],
        [InlineKeyboardButton(text="👥 Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="💳 Вывод средств", callback_data="withdrawal_menu")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

# ============================================
# АДМИН ПАНЕЛЬ
# ============================================

@dp.message(Command("admin"))
async def admin_cmd(message: types.Message):
    """Команда /admin - показать админ панель"""
    if message.from_user.id not in ADMIN_IDS and not db.is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав доступа к админ панели.")
        return
    
    is_super_admin = message.from_user.id in ADMIN_IDS
    await message.answer(
        "⚙️ **Админ панель**\n\nВыберите действие:",
        reply_markup=get_admin_keyboard(is_super_admin),
        parse_mode="None"
    )

@dp.callback_query(F.data == "admin_panel_back")
async def admin_panel_back_handler(callback: CallbackQuery):
    """Вернуться в админ панель"""
    if callback.from_user.id not in ADMIN_IDS and not db.is_admin(callback.from_user.id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    is_super_admin = callback.from_user.id in ADMIN_IDS
    
    # Удаляем текущее сообщение (если это сообщение с номером)
    try:
        await callback.message.delete()
    except:
        pass
    
    # Создаем новое сообщение с меню
    await callback.message.answer(
        "⚙️ **Админ панель**\n\nВыберите действие:",
        reply_markup=get_admin_keyboard(is_super_admin),
        parse_mode="None"
    )
    await callback.answer()

async def return_to_admin_menu(chat_id, user_id, message_id=None):
    """Вернуться в админ-меню"""
    is_super_admin = user_id in ADMIN_IDS
    
    # Если передано message_id, редактируем сообщение
    if message_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⚙️ **Админ панель**\n\nВыберите действие:",
                reply_markup=get_admin_keyboard(is_super_admin),
                parse_mode="None"
            )
            return True
        except:
            pass
    
    # Иначе создаем новое сообщение
    await bot.send_message(
        chat_id,
        "⚙️ **Админ панель**\n\nВыберите действие:",
        reply_markup=get_admin_keyboard(is_super_admin),
        parse_mode="None"
    )
    return True

async def restore_number_menu(number_id, operator_id=None):
    """
    Восстанавливает меню управления номером для оператора
    
    Args:
        number_id: ID номера
        operator_id: ID оператора (если None, используется оператор из operator_number_messages)
    
    Returns:
        True если меню восстановлено, False если не удалось
    """
    try:
        # Получаем информацию о номере
        number_info = db.cursor.execute(
            """SELECT n.phone, n.user_id, u.username, n.is_priority 
               FROM numbers n 
               LEFT JOIN users u ON n.user_id = u.user_id 
               WHERE n.id = ?""",
            (number_id,)
        ).fetchone()
        
        if not number_info:
            return False
        
        phone, user_id, username, is_prio = number_info
        
        # Определяем оператора
        if number_id in operator_number_messages:
            saved_operator_id, saved_message_id, saved_chat_id = operator_number_messages[number_id]
            if operator_id is None or saved_operator_id == operator_id:
                operator_id = saved_operator_id
                message_id = saved_message_id
                chat_id = saved_chat_id
            else:
                return False
        elif operator_id:
            # Если номер не найден в operator_number_messages, но указан operator_id
            # Пытаемся найти сообщение через базу данных
            chat_id = operator_id
            message_id = None  # Будем отправлять новое сообщение
        else:
            return False
        
        # Получаем настройки приоритета
        _, p_name = db.get_priority_settings()
        prio_label = f"⭐ [{p_name}] " if is_prio else ""
        
        # Экранируем спецсимволы
        safe_phone = escape_markdown(phone)
        safe_username = escape_markdown(username or 'User')
        
        # Текст меню
        text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{user_id}`)"
        
        # Создаем клавиатуру со стандартными кнопками
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{number_id}"),
             InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{number_id}")],
            [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{number_id}"),
             InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{number_id}")]
        ])
        
        # Пытаемся отредактировать существующее сообщение или отправить новое
        if message_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=kb,
                    parse_mode="None"
                )
                return True
            except Exception as e:
                logging.error(f"Ошибка редактирования меню номера {number_id}: {e}")
                # Если не удалось отредактировать, отправляем новое сообщение
                try:
                    sent_message = await bot.send_message(
                        chat_id,
                        text,
                        reply_markup=kb,
                        parse_mode="None"
                    )
                    # Обновляем operator_number_messages
                    operator_number_messages[number_id] = (operator_id, sent_message.message_id, sent_message.chat.id)
                    return True
                except Exception as e2:
                    logging.error(f"Ошибка отправки нового меню номера {number_id}: {e2}")
                    return False
        else:
            # Отправляем новое сообщение
            try:
                sent_message = await bot.send_message(
                    chat_id,
                    text,
                    reply_markup=kb,
                    parse_mode="None"
                )
                # Обновляем operator_number_messages
                operator_number_messages[number_id] = (operator_id, sent_message.message_id, sent_message.chat.id)
                return True
            except Exception as e:
                logging.error(f"Ошибка отправки меню номера {number_id}: {e}")
                return False
                
    except Exception as e:
        logging.error(f"Ошибка восстановления меню номера {number_id}: {e}")
        return False

# ============================================
# КОМАНДЫ ДЛЯ АДМИНИСТРАТОРОВ
# ============================================

@dp.message(Command("number"))
async def number_cmd(message: types.Message):
    """Команда /number - взять следующий номер из очереди С БЛОКИРОВКОЙ"""
    user_id = message.from_user.id
    
    # Проверяем права: супер-админ или оператор
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return
    
    # Очищаем истекшие блокировки
    db.clear_expired_locks()
    
    # Получаем следующий номер, исключая заблокированные другими операторами
    number = db.get_next_number_from_queue_for_admin(user_id)
    if not number:
        decreased, amount, new_fake = decrease_fake_queue_gradually()
        await message.answer("📭 **Очередь пуста.**", parse_mode="None")
        return

    n_id, phone, u_id, username, is_prio = number
    
    # Блокируем номер для текущего оператора
    lock_success, lock_message = db.lock_number_for_admin(n_id, user_id)
    if not lock_success:
        await message.answer(lock_message, parse_mode="None")
        return
    
    # Создаем клавиатуру
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{n_id}"),
         InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{n_id}")],
        [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{n_id}"),
         InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{n_id}")]
    ])
    
    _, p_name = db.get_priority_settings()
    prio_label = f"⭐ [{p_name}] " if is_prio else ""
    
    # Экранируем спецсимволы
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or 'User')
    
    text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)"
    
    sent_message = await message.answer(text, reply_markup=kb, parse_mode="None")
    # Сохраняем message_id для связи с номером
    operator_number_messages[n_id] = (user_id, sent_message.message_id, sent_message.chat.id)

# ============================================
# КОМАНДА ДЛЯ ВОССТАНОВЛЕНИЯ МЕНЮ НОМЕРА
# ============================================

@dp.message(Command("getnum"))
async def getnum_cmd(message: types.Message):
    """Команда /getnum - восстановить меню для взятия номера"""
    user_id = message.from_user.id
    
    # Проверяем права: супер-админ или оператор
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return
    
    # Очищаем истекшие блокировки
    db.clear_expired_locks()

    number = db.get_next_number_from_queue()
    if not number:
        decreased, amount, new_fake = decrease_fake_queue_gradually()
        await message.answer("📭 **Очередь пуста.**", parse_mode="None")
        return

    n_id, phone, u_id, username, is_prio = number
    
    # Проверяем блокировку номера
    locked_by = db.is_number_locked(n_id)
    if locked_by and locked_by != user_id:
        await message.answer(f"⚠️ Этот номер уже взят оператором ID: {locked_by}", parse_mode="None")
        return
    
    # Проверяем существование пользователя
    current_status = db.cursor.execute(
        "SELECT status FROM numbers WHERE id = ?", 
        (n_id,)
    ).fetchone()
    
    if current_status and current_status[0] != 'Ожидание':
        await message.answer("⚠️ Этот номер уже обработан другим оператором!", parse_mode="None")
        return
    
    # Блокируем номер для текущего оператора
    lock_success, lock_message = db.lock_number_for_admin(n_id, user_id)
    if not lock_success:
        await message.answer(lock_message, parse_mode="None")
        return
    
    # Создаем клавиатуру с кнопкой меню
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{n_id}"),
         InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{n_id}")],
        [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{n_id}"),
         InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{n_id}")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="admin_panel_back")]
    ])
    
    _, p_name = db.get_priority_settings()
    prio_label = f"⭐ [{p_name}] " if is_prio else ""
    
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or 'User')
    
    text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)"
    
    sent_message = await message.answer(text, reply_markup=kb, parse_mode="None")
    # Сохраняем message_id для связи с номером
    operator_number_messages[n_id] = (user_id, sent_message.message_id, sent_message.chat.id)

@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    """Команда /stats - статистика системы"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа.")
        return
    
    total_count = db.get_queue_count()
    real_count = db.get_real_queue_count()
    fake_count = db.get_fake_queue()
    
    # Статистика по пользователям
    total_users = db.cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    active_users = db.cursor.execute("SELECT COUNT(DISTINCT user_id) FROM numbers WHERE created_at > datetime('now', '-7 days')").fetchone()[0]
    
    # Статистика по номерам
    today = datetime.now().strftime('%Y-%m-%d')
    numbers_today = db.cursor.execute("SELECT COUNT(*) FROM numbers WHERE DATE(created_at) = ?", (today,)).fetchone()[0]
    numbers_total = db.cursor.execute("SELECT COUNT(*) FROM numbers").fetchone()[0]
    
    text = (
        f"📊 **Статистика системы**\n\n"
        f"👥 **Пользователи:**\n"
        f"   • Всего: {total_users}\n"
        f"   • Активных (7 дней): {active_users}\n\n"
        f"📱 **Номера:**\n"
        f"   • Сегодня: {numbers_today}\n"
        f"   • Всего: {numbers_total}\n\n"
        f"⏳ **Очередь:**\n"
        f"   • Всего: {total_count}\n"
        f"   • Реальных: {real_count}\n"
        f"   • Фейковых: {fake_count}\n\n"
        f"⚙️ **Режимы:**\n"
        f"   • Ночной: {'✅ ВКЛ' if db.get_night_mode() else '❌ ВЫКЛ'}\n"
        f"   • Выходные: {'✅ ВКЛ' if db.get_weekend_mode() else '❌ ВЫКЛ'}"
    )
    
    await message.answer(text, parse_mode="None")

@dp.message(Command("base"))
async def base_cmd(message: types.Message):
    """Команда /base - показать базу номеров"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа.")
        return
    
    nums = db.get_all_numbers_limit(10)
    text = "📂 **Последние 10 номеров:**\n\n"
    for n in nums:
        safe_phone = escape_markdown(n[0])
        safe_username = escape_markdown(n[1] or '—')
        text += f"📞 `{safe_phone}` | 👤 @{safe_username} | 📊 {n[2]} | 📦 {n[3]}\n"
    
    kb = [
        [InlineKeyboardButton(text="📥 Скачать полную базу", callback_data="csv")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="None")

# ============================================
# ОБРАБОТЧИКИ КНОПОК АДМИН-ПАНЕЛИ
# ============================================

@dp.callback_query(F.data == "admin_take_fast")
async def admin_take_fast_handler(callback: CallbackQuery):
    """Кнопка взять номер с блокировкой"""
    user_id = callback.from_user.id
    
    # Проверяем права: супер-админ или оператор
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Получаем следующий номер, исключая заблокированные другими операторами
    number = db.get_next_number_from_queue_for_admin(user_id)
    if not number:
        decreased, amount, new_fake = decrease_fake_queue_gradually()
        await callback.answer("📭 Очередь пуста.", show_alert=True)
        return
    
    n_id, phone, u_id, username, is_prio = number
    
    # Блокируем номер для текущего оператора
    lock_success, lock_message = db.lock_number_for_admin(n_id, user_id)
    if not lock_success:
        await callback.answer(lock_message, show_alert=True)
        return
    
    # Создаем клавиатуру
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{n_id}"),
         InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{n_id}")],
        [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{n_id}"),
         InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{n_id}")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="admin_panel_back")]  # Добавлена кнопка меню
    ])
    
    _, p_name = db.get_priority_settings()
    prio_label = f"⭐ [{p_name}] " if is_prio else ""
    
    # Экранируем спецсимволы
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or 'User')
    
    text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)"
    
    # Показываем статистику очереди для админов (если нужно)
    if user_id in ADMIN_IDS:
        total_count = db.get_queue_count()
        real_count = db.get_real_queue_count()
        fake_count = db.get_fake_queue()
        
        text += f"\n\n📊 **Очередь:** {total_count} (реальных: {real_count}, фейковых: {fake_count})"
        
        # Только для главного админа показываем информацию о скрытой надбавке
        tariff_info = db.cursor.execute("""
            SELECT t.name, t.duration_min, n.tariff_id FROM tariffs t 
            JOIN numbers n ON t.id = n.tariff_id 
            WHERE n.id = ?
        """, (n_id,)).fetchone()
        
        if tariff_info:
            tariff_name, standard_duration, tariff_id = tariff_info
            hidden_bonus = db.get_hidden_time_bonus(tariff_id)
            real_duration = standard_duration + hidden_bonus
            
            text += f"\n\n⚠️ **Только для админа:**\n"
            text += f"📱 Тариф: {tariff_name}\n"
            text += f"⏱ Показываемое пользователям: {real_duration} мин\n"
            text += f"   (стандартное: {standard_duration} мин + скрытая надбавка: {hidden_bonus} мин)"
    
    # **ИЗМЕНЕНИЕ: РЕДАКТИРУЕМ текущее сообщение вместо создания нового**
    try:
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")
        # Сохраняем message_id для связи с номером (редактируем существующее сообщение)
        operator_number_messages[n_id] = (user_id, callback.message.message_id, callback.message.chat.id)
        await callback.answer("📱 Номер показан")
    except Exception as e:
        # Если не получается отредактировать, создаем новое сообщение
        sent_message = await callback.message.answer(text, reply_markup=kb, parse_mode="None")
        # Сохраняем message_id для связи с номером
        operator_number_messages[n_id] = (user_id, sent_message.message_id, sent_message.chat.id)
        await callback.answer("📱 Номер показан")

@dp.callback_query(F.data == "admin_base")
async def admin_base_handler(callback: CallbackQuery, state: FSMContext):
    """Кнопка базы номеров в админ-панели с выбором даты"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Получаем список доступных дат
    available_dates = db.get_available_dates()
    
    text = "📂 **База номеров**\n\n"
    text += "Выберите дату для просмотра или действие:"
    
    buttons = []
    
    # Кнопки для последних 5 дат
    for i, (date_str,) in enumerate(available_dates[:5]):
        # Форматируем дату в более читаемый вид
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d.%m.%Y')
            button_text = f"📅 {formatted_date}"
        except:
            button_text = f"📅 {date_str}"
        
        buttons.append([InlineKeyboardButton(
            text=button_text, 
            callback_data=f"view_date_{date_str}"
        )])
    
    # Кнопка "Все номера"
    buttons.append([InlineKeyboardButton(
        text="📋 Все номера (последние 10)", 
        callback_data="view_all_numbers"
    )])
    
    # Кнопка "Скачать за дату"
    buttons.append([InlineKeyboardButton(
        text="📥 Скачать за дату", 
        callback_data="download_by_date"
    )])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "view_all_numbers")
async def view_all_numbers_handler(callback: CallbackQuery):
    """Показать все номера (без фильтрации по дате)"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    nums = db.get_numbers_by_date(limit=10)  # Без даты - последние 10
    
    if not nums:
        text = "📭 **Нет номеров в базе**"
    else:
        text = "📂 **Последние 10 номеров:**\n\n"
        for n in nums:
            phone, username, status, tariff_name, created_at = n
            safe_phone = escape_markdown(phone)
            safe_username = escape_markdown(username or '—')
            
            # Форматируем дату
            if created_at:
                created_date = created_at.split()[0]
                created_time = created_at.split()[1][:5]
                date_display = f"{created_date} {created_time}"
            else:
                date_display = "—"
            
            text += f"📞 `{safe_phone}`\n👤 @{safe_username}\n📊 {status} | 📦 {tariff_name}\n🕐 {date_display}\n━━━━━━━━━━━━━━━━━━━━\n"
    
    buttons = [
        [InlineKeyboardButton(text="📥 Скачать полную базу (TXT)", callback_data="csv_all")],
        [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="admin_base")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data.startswith("view_date_"))
async def view_date_handler(callback: CallbackQuery):
    """Показать номера за определенную дату"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    date_str = callback.data.split("view_date_")[1]
    
    try:
        # Форматируем дату для отображения
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d.%m.%Y')
    except:
        formatted_date = date_str
    
    nums = db.get_numbers_by_date(date_str, limit=20)
    
    if not nums:
        text = f"📭 **Нет номеров за {formatted_date}**"
    else:
        # Подсчитываем статистику
        total = len(nums)
        status_counts = {}
        for n in nums:
            status = n[2]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        text = f"📂 **Номера за {formatted_date}**\n\n"
        text += f"📊 **Статистика:** Всего {total} номеров\n"
        
        for status, count in status_counts.items():
            text += f"  • {status}: {count}\n"
        
        text += "\n📋 **Последние 20 номеров:**\n\n"
        
        for i, n in enumerate(nums[:20], 1):
            phone, username, status, tariff_name, created_at = n
            safe_phone = escape_markdown(phone)
            safe_username = escape_markdown(username or '—')
            
            if created_at:
                created_time = created_at.split()[1][:5]
                time_display = f"🕐 {created_time}"
            else:
                time_display = ""
            
            emoji = "✅" if status == "ОТСТОЯЛ" else "❌" if status == "СЛЕТ" else "⏳"
            
            text += f"{i}. {emoji} `{safe_phone}`\n"
            text += f"   👤 @{safe_username} | {tariff_name} {time_display}\n"
            
            if i < len(nums[:20]):
                text += "━━━━━━━━━━━━━━━━━━━━\n"
    
    buttons = [
        [InlineKeyboardButton(text=f"📥 Скачать за {formatted_date}", callback_data=f"csv_date_{date_str}")],
        [InlineKeyboardButton(text="⬅️ Назад к выбору даты", callback_data="admin_base")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "download_by_date")
async def download_by_date_handler(callback: CallbackQuery):
    """Выбор даты для скачивания"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Получаем список доступных дат
    available_dates = db.get_available_dates()
    
    if not available_dates:
        await callback.answer("📭 Нет данных для скачивания", show_alert=True)
        return
    
    text = "📥 **Скачать базу за дату**\n\nВыберите дату:"
    
    buttons = []
    
    # Кнопки для последних 10 дат
    for i, (date_str,) in enumerate(available_dates[:10]):
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d.%m.%Y')
            button_text = f"📅 {formatted_date}"
        except:
            button_text = f"📅 {date_str}"
        
        buttons.append([InlineKeyboardButton(
            text=button_text, 
            callback_data=f"download_date_{date_str}"
        )])
    
    buttons.append([InlineKeyboardButton(text="📥 Все номера", callback_data="csv_all")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_base")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "csv_all")
async def csv_all_handler(callback: CallbackQuery):
    """Скачать полную базу (все номера)"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    await csv_handler_general(callback, date_str=None)

@dp.callback_query(F.data.startswith("csv_date_"))
async def csv_date_handler(callback: CallbackQuery):
    """Скачать базу за определенную дату"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    date_str = callback.data.split("csv_date_")[1]
    await csv_handler_general(callback, date_str)

async def csv_handler_general(callback: CallbackQuery, date_str=None):
    """Общий обработчик для скачивания базы (с фильтром по дате или без)"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Получаем данные в зависимости от фильтра
    if date_str:
        data = db.cursor.execute("""
            SELECT n.id, n.phone, u.username, n.status, t.name, n.created_at, n.finished_at, n.is_priority
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id
            LEFT JOIN tariffs t ON n.tariff_id = t.id
            WHERE DATE(n.created_at) = ?
            ORDER BY n.created_at DESC
        """, (date_str,)).fetchall()
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d.%m.%Y')
            file_date = date_obj.strftime('%Y-%m-%d')
        except:
            formatted_date = date_str
            file_date = date_str
        
        filename = f"base_{file_date}.txt"
        caption = f"📂 **База номеров за {formatted_date}**\n\n📊 Всего номеров: {len(data)}"
    else:
        data = db.cursor.execute("""
            SELECT n.id, n.phone, u.username, n.status, t.name, n.created_at, n.finished_at, n.is_priority
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id
            LEFT JOIN tariffs t ON n.tariff_id = t.id
            ORDER BY n.created_at DESC
        """).fetchall()
        today = datetime.now().strftime('%Y-%m-%d')
        filename = f"base_all_{today}.txt"
        caption = f"📂 **Полная база номеров**\n\n📊 Всего номеров: {len(data)}"
    
    if not data:
        await callback.answer("📭 Нет данных для экспорта", show_alert=True)
        return
    
    path = filename
    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        
        if date_str:
            f.write(f"БАЗА НОМЕРОВ ЗА {formatted_date}\n")
        else:
            f.write("ПОЛНАЯ БАЗА НОМЕРОВ\n")
        
        f.write(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Всего записей: {len(data)}\n")
        f.write("=" * 90 + "\n\n")
        
        f.write(f"{'ID':<6} {'Номер':<15} {'Пользователь':<25} {'Статус':<12} {'Тариф':<15} {'Приоритет':<10} {'Создан':<20} {'Завершен':<20}\n")
        f.write("-" * 130 + "\n")
        
        for row in data:
            # row содержит: id, phone, username, status, tariff_name, created_at, finished_at, is_priority
            row_id, phone, username, status, tariff_name, created_at, finished_at, is_priority = row
            
            # Обрезаем длинные имена пользователей
            username_display = username or "—"
            if len(username_display) > 20:
                username_display = username_display[:17] + "..."
            
            # Форматируем даты
            created_display = created_at if created_at else "—"
            finished_display = finished_at if finished_at else "—"
            
            # Определяем приоритет
            priority_text = "ДА" if is_priority else "НЕТ"
            
            f.write(f"{row_id:<6} {phone:<15} @{username_display:<24} {status:<12} {tariff_name:<15} {priority_text:<10} {created_display:<20} {finished_display:<20}\n")
    
    try:
        await callback.message.answer_document(
            FSInputFile(path), 
            caption=caption,
            parse_mode="None"
        )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
    
    if os.path.exists(path): 
        os.remove(path)

@dp.callback_query(F.data == "csv")
async def csv_handler(callback: CallbackQuery):
    """Скачать базу номеров (старый вариант - теперь скачивает все)"""
    await csv_all_handler(callback)

# ============================================
# ОБРАБОТЧИКИ УПРАВЛЕНИЯ ТАРИФАМИ
# ============================================

@dp.callback_query(F.data == "admin_tariffs")
async def admin_tariffs_handler(callback: CallbackQuery):
    """Кнопка управления тарифами"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    tariffs = db.get_all_tariffs_admin()
    buttons = []
    for t in tariffs:
        status_emo = "🟢" if t[4] == 1 else "🔴"
        status_text = " (Открыт)" if t[4] == 1 else " (Закрыт)"
        buttons.append([InlineKeyboardButton(text=f"{status_emo} {t[1]} - ${t[2]}{status_text}", 
                                           callback_data=f"manage_t_{t[0]}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")])
    
    text = "⚙️ **Управление тарифами**\n\n🟢 - Тариф открыт\n🔴 - Тариф закрыт\n\nВыберите тариф для управления:"
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data.startswith("manage_t_"))
async def manage_tariff_handler(callback: CallbackQuery):
    """Управление конкретным тарифом"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    t_id = callback.data.split("_")[2]
    
    # Получаем информацию о тарифе
    tariff = db.cursor.execute("SELECT name, price, duration_min, is_active FROM tariffs WHERE id = ?", (t_id,)).fetchone()
    if not tariff:
        await callback.answer("❌ Тариф не найден", show_alert=True)
        return
    
    name, price, duration, is_active = tariff
    status_text = "🟢 ОТКРЫТ" if is_active == 1 else "🔴 ЗАКРЫТ"
    
    text = f"📊 **Тариф:** {name}\n💰 **Цена:** ${price}\n⏱ **Длительность:** {duration} мин\n📊 **Статус:** {status_text}\n\nВыберите действие:"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Супер-админы видят больше опций
    if user_id in ADMIN_IDS:
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="🔄 Сменить статус", callback_data=f"toggle_t_{t_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_t_{t_id}")
        ])
    
    kb.inline_keyboard.append([InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="admin_tariffs")])
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")

@dp.callback_query(F.data.startswith("toggle_t_"))
async def toggle_tariff_handler(callback: CallbackQuery):
    """Переключить статус тарифа"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    t_id = callback.data.split("_")[2]
    db.toggle_tariff_status(t_id)
    await callback.answer("✅ Статус тарифа изменен", show_alert=True)
    await admin_tariffs_handler(callback)

@dp.callback_query(F.data.startswith("edit_t_"))
async def edit_tariff_start_handler(callback: CallbackQuery, state: FSMContext):
    """Начать редактирование тарифа"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    t_id = callback.data.split("_")[2]
    await state.update_data(edit_t_id=t_id)
    await state.set_state(Form.waiting_for_tariff_name)
    
    await callback.message.edit_text(
        "✏️ **Редактирование тарифа**\n\nВведите НОВОЕ НАЗВАНИЕ для тарифа:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_tariffs")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_tariff_name)
async def process_tariff_name(message: types.Message, state: FSMContext):
    """Обработка нового названия тарифа"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    await state.update_data(new_name=message.text.strip())
    await state.set_state(Form.waiting_for_tariff_price)
    await message.answer(f"✅ Название сохранено: '{message.text.strip()}'\n\nТеперь введите НОВУЮ ЦЕНУ ($):")

@dp.message(Form.waiting_for_tariff_price)
async def process_tariff_price(message: types.Message, state: FSMContext):
    """Обработка новой цены тарифа"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    try:
        price = float(message.text.replace(',', '.'))
        await state.update_data(new_price=price)
        await state.set_state(Form.waiting_for_tariff_duration)
        await message.answer(f"✅ Цена сохранена: ${price}\n\nТеперь введите НОВУЮ ДЛИТЕЛЬНОСТЬ (в минутах):")
    except ValueError:
        await message.answer("❌ Введите число! Например: 15.5 или 20")

@dp.message(Form.waiting_for_tariff_duration)
async def process_tariff_duration(message: types.Message, state: FSMContext):
    """Обработка новой длительности тарифа"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    try:
        duration = int(message.text)
        data = await state.get_data()
        
        # Обновляем тариф в базе данных
        db.update_tariff_full(data['edit_t_id'], data['new_name'], data['new_price'], duration)
        
        await message.answer(f"✅ Тариф успешно обновлен!\n\n📊 **Новые данные:**\n"
                           f"• Название: {data['new_name']}\n"
                           f"• Цена: ${data['new_price']}\n"
                           f"• Длительность: {duration} мин",
                           reply_markup=get_admin_keyboard(True),
                           parse_mode="None")
        
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите целое число! Например: 30")

# ============================================
# УПРАВЛЕНИЕ ВЫПЛАТАМИ (ГЛАВНЫЙ АДМИН)
# ============================================

@dp.callback_query(F.data == "admin_withdrawals_menu")
async def admin_withdrawals_menu_handler(callback: CallbackQuery):
    """Меню управления выплатами"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    pending_count = db.get_pending_withdrawals_count()
    all_withdrawals = db.get_all_withdrawals()
    
    # Статистика
    total_pending = sum(w[3] for w in all_withdrawals if w[4] == 'pending')
    total_approved = sum(w[3] for w in all_withdrawals if w[4] == 'approved')
    total_rejected = sum(w[3] for w in all_withdrawals if w[4] == 'rejected')
    
    text = (f"💳 **Управление выплатами**\n\n"
            f"📊 **Статистика:**\n"
            f"• Ожидающих заявок: **{pending_count}** (${total_pending:.2f})\n"
            f"• Одобрено заявок: **{len([w for w in all_withdrawals if w[4] == 'approved'])}** (${total_approved:.2f})\n"
            f"• Отклонено заявок: **{len([w for w in all_withdrawals if w[4] == 'rejected'])}** (${total_rejected:.2f})\n\n"
            f"⚙️ **Настройки:**\n"
            f"• Мин. сумма вывода: **${db.get_min_withdrawal()}**\n"
            f"• Методы оплаты: **{', '.join(db.get_payment_methods())}**\n\n"
            f"Выберите действие:")
    
    buttons = [
        [InlineKeyboardButton(text=f"⏳ Ожидающие ({pending_count})", callback_data="admin_withdrawals_pending")],
        [InlineKeyboardButton(text="📋 Все заявки", callback_data="admin_withdrawals_all")],
        [InlineKeyboardButton(text="⚙️ Настройки выплат", callback_data="admin_withdrawals_settings")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_withdrawals_pending")
async def admin_withdrawals_pending_handler(callback: CallbackQuery):
    """Список ожидающих заявок на вывод"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    withdrawals = db.get_all_withdrawals(status_filter='pending')
    
    if not withdrawals:
        text = "⏳ **Нет ожидающих заявок на вывод**"
        buttons = [[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_withdrawals_menu")]]
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")
    else:
        # Показываем только первую заявку
        w = withdrawals[0]
        w_id, user_id, username, amount, status, method, details, created_at, processed_at, admin_id, admin_comment, db_username, balance = w
        
        # Экранируем данные
        safe_details = escape_markdown(details) if details else "—"
        safe_username = escape_markdown(username or db_username or str(user_id))
        
        text = f"🔔 **Ожидающая заявка на вывод**\n\n"
        text += f"📋 **ID заявки:** #{w_id}\n"
        text += f"👤 **Пользователь:** @{safe_username} (ID: `{user_id}`)\n"
        text += f"💰 **Сумма:** ${amount:.2f}\n"
        text += f"💳 **Способ:** {method}\n"
        text += f"📝 **Реквизиты:** `{safe_details}`\n"
        text += f"📅 **Дата создания:** {created_at.split()[0]}\n"
        text += f"💰 **Баланс пользователя:** ${balance:.2f}\n\n"
        
        if len(withdrawals) > 1:
            text += f"📋 **Еще ожидают:** {len(withdrawals) - 1} заявок\n\n"
        
        text += f"Выберите действие:"
        
        buttons = [
            [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_withdrawal_{w_id}"),
             InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_withdrawal_{w_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_withdrawals_menu")]
        ]
        
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_withdrawals_all")
async def admin_withdrawals_all_handler(callback: CallbackQuery):
    """Все заявки на вывод"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    withdrawals = db.get_all_withdrawals()
    
    if not withdrawals:
        text = "📋 **Нет заявок на вывод**"
    else:
        text = "📋 **Все заявки на вывод**\n\n"
        
        for w in withdrawals[:10]:  # Показываем только 10 последних
            w_id, user_id, username, amount, status, method, details, created_at, processed_at, admin_id, admin_comment, db_username, balance = w
            
            # Определяем статус
            if status == 'pending':
                status_emoji = "⏳"
            elif status == 'approved':
                status_emoji = "✅"
            else:
                status_emoji = "❌"
            
            # Экранируем данные
            safe_username = escape_markdown(username or db_username or str(user_id))
            
            text += f"{status_emoji} **#{w_id}** @{safe_username} - ${amount:.2f} ({method})\n"
            text += f"   📅 {created_at.split()[0]} | {status.upper()}\n"
    
    buttons = [
        [InlineKeyboardButton(text="📥 Скачать отчет (TXT)", callback_data="download_withdrawals_report")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_withdrawals_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "download_withdrawals_report")
async def download_withdrawals_report_handler(callback: CallbackQuery):
    """Скачать отчет по выплатам"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    withdrawals = db.get_all_withdrawals()
    
    if not withdrawals:
        await callback.answer("📭 Нет данных для отчета", show_alert=True)
        return
    
    # Создаем файл
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"withdrawals_report_{timestamp}.txt"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("ОТЧЕТ ПО ВЫПЛАТАМ\n")
        f.write(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Всего заявок: {len(withdrawals)}\n")
        f.write("=" * 80 + "\n\n")
        
        # Статистика
        pending = [w for w in withdrawals if w[4] == 'pending']
        approved = [w for w in withdrawals if w[4] == 'approved']
        rejected = [w for w in withdrawals if w[4] == 'rejected']
        
        f.write("СТАТИСТИКА:\n")
        f.write(f"  Ожидающие: {len(pending)} заявок, ${sum(w[3] for w in pending):.2f}\n")
        f.write(f"  Одобрено: {len(approved)} заявок, ${sum(w[3] for w in approved):.2f}\n")
        f.write(f"  Отклонено: {len(rejected)} заявок, ${sum(w[3] for w in rejected):.2f}\n")
        f.write("-" * 80 + "\n\n")
        
        # Детали по заявкам
        f.write("ДЕТАЛИ ЗАЯВОК:\n\n")
        f.write(f"{'ID':<6} {'Дата':<12} {'Пользователь':<25} {'Сумма':<10} {'Метод':<10} {'Статус':<12} {'Админ':<15}\n")
        f.write("-" * 100 + "\n")
        
        for w in withdrawals:
            w_id, user_id, username, amount, status, method, details, created_at, processed_at, admin_id, admin_comment, db_username, balance = w
            
            username_display = username or db_username or str(user_id)
            if len(username_display) > 20:
                username_display = username_display[:17] + "..."
            
            created_date = created_at.split()[0] if created_at else "—"
            
            status_rus = {
                'pending': 'ОЖИДАНИЕ',
                'approved': 'ОДОБРЕНО',
                'rejected': 'ОТКЛОНЕНО'
            }.get(status, status)
            
            f.write(f"{w_id:<6} {created_date:<12} @{username_display:<24} ${amount:<9.2f} {method:<10} {status_rus:<12} {admin_id or '—':<15}\n")
    
    try:
        # Отправляем файл
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📊 **Отчет по выплатам**\n\n📅 Дата экспорта: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n📋 Всего заявок: {len(withdrawals)}",
            parse_mode="None"
        )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка отправки файла: {e}", show_alert=True)
    finally:
        # Удаляем временный файл
        if os.path.exists(filename):
            os.remove(filename)

@dp.callback_query(F.data == "admin_withdrawals_settings")
async def admin_withdrawals_settings_handler(callback: CallbackQuery):
    """Настройки выплат"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    min_withdrawal = db.get_min_withdrawal()
    payment_methods = db.get_payment_methods()
    
    text = (f"⚙️ **Настройки выплат**\n\n"
            f"💰 **Минимальная сумма вывода:** ${min_withdrawal}\n"
            f"💳 **Доступные методы оплаты:** {', '.join(payment_methods)}\n\n"
            f"Выберите настройку для изменения:")
    
    buttons = [
        [InlineKeyboardButton(text="💰 Изменить мин. сумму", callback_data="admin_set_min_withdrawal")],
        [InlineKeyboardButton(text="💳 Изменить методы оплаты", callback_data="admin_set_payment_methods")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_withdrawals_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_set_min_withdrawal")
async def admin_set_min_withdrawal_handler(callback: CallbackQuery, state: FSMContext):
    """Изменение минимальной суммы вывода"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_min_withdrawal_amount)
    
    await callback.message.edit_text(
        f"💰 **Изменение минимальной суммы вывода**\n\n"
        f"Текущая минимальная сумма: ${db.get_min_withdrawal()}\n\n"
        f"Введите новую минимальную сумму ($):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_withdrawals_settings")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_min_withdrawal_amount)
async def process_min_withdrawal_amount(message: types.Message, state: FSMContext):
    """Обработка новой минимальной суммы вывода"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    try:
        amount = float(message.text.replace(',', '.'))
        if amount <= 0:
            await message.answer("❌ Сумма должна быть больше 0!")
            return
        
        db.set_min_withdrawal(amount)
        await message.answer(f"✅ Минимальная сумма вывода изменена на: **${amount}**", parse_mode="None")
        await state.clear()
        
        # Возвращаемся в меню настроек
        min_withdrawal = db.get_min_withdrawal()
        payment_methods = db.get_payment_methods()
        
        text = (f"⚙️ **Настройки выплат**\n\n"
                f"💰 **Минимальная сумма вывода:** ${min_withdrawal}\n"
                f"💳 **Доступные методы оплаты:** {', '.join(payment_methods)}\n\n"
                f"Выберите настройку для изменения:")
        
        buttons = [
            [InlineKeyboardButton(text="💰 Изменить мин. сумму", callback_data="admin_set_min_withdrawal")],
            [InlineKeyboardButton(text="💳 Изменить методы оплаты", callback_data="admin_set_payment_methods")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_withdrawals_menu")]
        ]
        
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")
        
    except ValueError:
        await message.answer("❌ Введите число! Например: 1.0 или 5")

@dp.callback_query(F.data == "admin_set_payment_methods")
async def admin_set_payment_methods_handler(callback: CallbackQuery, state: FSMContext):
    """Изменение методов оплаты"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_payment_methods)
    
    current_methods = db.get_payment_methods()
    
    await callback.message.edit_text(
        f"💳 **Изменение методов оплаты**\n\n"
        f"Текущие методы: {', '.join(current_methods)}\n\n"
        f"Введите новые методы оплаты через запятую:\n"
        f"Пример: QIWI,Карта,ЮMoney,USDT",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_withdrawals_settings")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_payment_methods)
async def process_payment_methods(message: types.Message, state: FSMContext):
    """Обработка новых методов оплаты"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    methods_str = message.text.strip()
    if not methods_str:
        await message.answer("❌ Введите хотя бы один метод оплаты")
        return
    
    # Разделяем и очищаем методы
    methods = [method.strip() for method in methods_str.split(',')]
    methods = [method for method in methods if method]  # Убираем пустые
    
    if not methods:
        await message.answer("❌ Введите корректные методы оплаты")
        return
    
    db.set_payment_methods(','.join(methods))
    await message.answer(f"✅ Методы оплаты обновлены: **{', '.join(methods)}**", parse_mode="None")
    await state.clear()
    
    # Возвращаемся в меню настроек
    min_withdrawal = db.get_min_withdrawal()
    payment_methods = db.get_payment_methods()
    
    text = (f"⚙️ **Настройки выплат**\n\n"
            f"💰 **Минимальная сумма вывода:** ${min_withdrawal}\n"
            f"💳 **Доступные методы оплаты:** {', '.join(payment_methods)}\n\n"
            f"Выберите настройку для изменения:")
    
    buttons = [
        [InlineKeyboardButton(text="💰 Изменить мин. сумму", callback_data="admin_set_min_withdrawal")],
        [InlineKeyboardButton(text="💳 Изменить методы оплаты", callback_data="admin_set_payment_methods")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_withdrawals_menu")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data.startswith("approve_withdrawal_"))
async def approve_withdrawal_handler(callback: CallbackQuery, state: FSMContext):
    """Одобрение заявки на вывод"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    withdrawal_id = int(callback.data.split("_")[2])
    
    await state.update_data(withdrawal_id=withdrawal_id, action="approved")
    await state.set_state(Form.waiting_for_withdrawal_comment)
    
    await callback.message.edit_text(
        "✅ **Одобрение заявки на вывод**\n\n"
        "Введите комментарий для пользователя (или отправьте '-' для отсутствия комментария):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_withdrawals_pending")]
        ]),
        parse_mode="None"
    )

@dp.callback_query(F.data.startswith("reject_withdrawal_"))
async def reject_withdrawal_handler(callback: CallbackQuery, state: FSMContext):
    """Отклонение заявки на вывод"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    withdrawal_id = int(callback.data.split("_")[2])
    
    await state.update_data(withdrawal_id=withdrawal_id, action="rejected")
    await state.set_state(Form.waiting_for_withdrawal_comment)
    
    await callback.message.edit_text(
        "❌ **Отклонение заявки на вывод**\n\n"
        "Введите причину отказа для пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_withdrawals_pending")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_withdrawal_comment)
async def process_withdrawal_comment(message: types.Message, state: FSMContext):
    """Обработка комментария для заявки на вывод"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    data = await state.get_data()
    withdrawal_id = data['withdrawal_id']
    action = data['action']  # 'approved' или 'rejected'
    comment = message.text.strip()
    
    if action == "approved" and comment == "-":
        comment = ""
    
    # Обрабатываем заявку
    success, result_message = db.process_withdrawal(
        withdrawal_id=withdrawal_id,
        admin_id=message.from_user.id,
        status=action,  # 'approved' или 'rejected'
        comment=comment
    )
    
    if success:
        # Получаем информацию о заявке для уведомления пользователя
        withdrawal_info = db.cursor.execute("""
            SELECT user_id, amount, payment_method, username 
            FROM withdrawals WHERE id = ?
        """, (withdrawal_id,)).fetchone()
        
        if withdrawal_info:
            user_id, amount, method, username = withdrawal_info
            
            # Уведомляем пользователя
            try:
                if action == "approved":
                    await bot.send_message(
                        user_id,
                        f"✅ **Ваша заявка на вывод одобрена!**\n\n"
                        f"💰 **Сумма:** ${amount:.2f}\n"
                        f"💳 **Способ:** {method}\n"
                        f"📅 **Дата обработки:** {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                        f"{f'💬 **Комментарий:** {comment}' if comment else ''}",
                        parse_mode="None"
                    )
                    
                    # Логирование выплаты в канал
                    try:
                        # Шифруем имя пользователя
                        raw_username = username or f"ID{user_id}"
                        if username and username.startswith("@"):
                            # Оставляем первые 3 символа и добавляем звездочки
                            hidden_username = f"@{username[1:4]}***" if len(username) > 4 else f"@{username[1:]}***"
                        elif username:
                            hidden_username = f"@{username[:3]}***" if len(username) > 3 else f"@{username}***"
                        else:
                            # Если юзернейма нет, скрываем часть ID
                            user_id_str = str(user_id)
                            hidden_username = f"ID:{user_id_str[:4]}***"
                        
                        # Формируем текст для канала
                        bot_username = "Magic_team_work_bot"  # Имя бота
                        
                        log_text = (
                            f"✅ **Новая выплата!**\n\n"
                            f"👤 Воркер: {hidden_username}\n"
                            f"💰 Сумма: **${amount:.2f}**\n"
                            f"💳 Статус: Выплачено успешно\n\n"
                            f"🚀 Начать зарабатывать: @{bot_username}"
                        )
                        
                        kb = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="💬 Чат с отзывами", url="https://t.me/+i2Agn48UehQ0Yzg6")]
                        ])
                        
                        await bot.send_message(PAYMENT_LOG_CHANNEL, log_text, reply_markup=kb, parse_mode="Markdown")
                    except Exception as e:
                        logging.error(f"Ошибка отправки в лог выплат: {e}")
                else:  # rejected
                    await bot.send_message(
                        user_id,
                        f"❌ **Ваша заявка на вывод отклонена**\n\n"
                        f"💰 **Сумма:** ${amount:.2f}\n"
                        f"💳 **Способ:** {method}\n"
                        f"📅 **Дата обработки:** {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                        f"📝 **Причина:** {comment or 'Не указана'}",
                        parse_mode="None"
                    )
            except:
                pass
        
        action_text = "одобрена" if action == "approved" else "отклонена"
        await message.answer(f"✅ Заявка #{withdrawal_id} {action_text}!", parse_mode="None")
    else:
        await message.answer(f"❌ {result_message}", parse_mode="None")
    
    await state.clear()

# ============================================
# РЕФЕРАЛЬНАЯ СИСТЕМА (АДМИН)
# ============================================

@dp.callback_query(F.data == "admin_referral_system")
async def admin_referral_system_handler(callback: CallbackQuery):
    """Управление реферальной системой (админ)"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    stats = db.get_all_referral_stats_admin()
    
    text = f"🤝 **Управление реферальной системой**\n\n"
    text += f"📊 **Общая статистика:**\n"
    text += f"• Всего рефералов: **{stats['total_referrals']}**\n"
    text += f"• Успешных: **{stats['total_successful']}**\n"
    text += f"• Выплачено бонусов: **${stats['total_bonus_paid']:.2f}**\n"
    text += f"• Текущий бонус: **${stats['referral_bonus']}**\n"
    text += f"• Статус: **{'✅ ВКЛЮЧЕНА' if stats['referral_enabled'] else '❌ ВЫКЛЮЧЕНА'}**\n\n"
    
    if stats['top_referrers']:
        text += f"🏆 **Топ-10 рефереров:**\n"
        for i, user in enumerate(stats['top_referrers'], 1):
            user_id, username, total_ref, successful_ref, earned_bonus = user
            safe_username = escape_markdown(username or f"ID{user_id}")
            text += f"{i}. @{safe_username}\n"
            text += f"   📊 Пригласил: {total_ref} | Успешных: {successful_ref}\n"
            text += f"   💰 Заработал: ${earned_bonus:.2f}\n"
    
    buttons = [
        [InlineKeyboardButton(text="💰 Изменить бонус", callback_data="admin_set_referral_bonus")],
        [InlineKeyboardButton(text=f"{'❌ ВЫКЛ' if stats['referral_enabled'] else '✅ ВКЛ'} систему", 
                              callback_data="admin_toggle_referral_system")],
        [InlineKeyboardButton(text="📥 Скачать отчет", callback_data="admin_download_referral_report")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_set_referral_bonus")
async def admin_set_referral_bonus_handler(callback: CallbackQuery, state: FSMContext):
    """Изменение суммы бонуса за реферала"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_referral_bonus)
    
    current_bonus = db.get_referral_bonus()
    
    await callback.message.edit_text(
        f"💰 **Изменение бонуса за реферала**\n\n"
        f"Текущий бонус: ${current_bonus}\n\n"
        f"Введите новую сумму бонуса ($):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_referral_system")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_referral_bonus)
async def process_referral_bonus(message: types.Message, state: FSMContext):
    """Обработка новой суммы бонуса за реферала"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    try:
        amount = float(message.text.replace(',', '.'))
        if amount < 0:
            await message.answer("❌ Сумма не может быть отрицательной!")
            return
        
        db.set_referral_bonus(amount)
        await message.answer(f"✅ Бонус за реферала изменен на: **${amount}**", parse_mode="None")
        await state.clear()
        await admin_referral_system_handler(message)
        
    except ValueError:
        await message.answer("❌ Введите число! Например: 5.0 или 10")

@dp.callback_query(F.data == "admin_toggle_referral_system")
async def admin_toggle_referral_system_handler(callback: CallbackQuery):
    """Включение/выключение реферальной системы"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    current_status = db.is_referral_enabled()
    new_status = 0 if current_status else 1
    
    db.set_referral_enabled(new_status)
    
    status_text = "ВКЛЮЧЕНА" if new_status else "ВЫКЛЮЧЕНА"
    await callback.answer(f"✅ Реферальная система {status_text}", show_alert=True)
    await admin_referral_system_handler(callback)

@dp.callback_query(F.data == "admin_download_referral_report")
async def admin_download_referral_report_handler(callback: CallbackQuery):
    """Скачать отчет по реферальной системе"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    stats = db.get_all_referral_stats_admin()
    all_referrals = db.cursor.execute("""
        SELECT r.*, u1.username as referrer_name, u2.username as referred_name
        FROM referrals r
        LEFT JOIN users u1 ON r.referrer_id = u1.user_id
        LEFT JOIN users u2 ON r.referred_id = u2.user_id
        ORDER BY r.created_at DESC
    """).fetchall()
    
    if not all_referrals:
        await callback.answer("📭 Нет данных для отчета", show_alert=True)
        return
    
    # Создаем файл
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"referral_report_{timestamp}.txt"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("ОТЧЕТ ПО РЕФЕРАЛЬНОЙ СИСТЕМЕ\n")
        f.write(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Бонус за реферала: ${stats['referral_bonus']}\n")
        f.write(f"Статус системы: {'ВКЛЮЧЕНА' if stats['referral_enabled'] else 'ВЫКЛЮЧЕНА'}\n")
        f.write("=" * 80 + "\n\n")
        
        # Статистика
        f.write("СТАТИСТИКА:\n")
        f.write(f"  Всего рефералов: {stats['total_referrals']}\n")
        f.write(f"  Успешных рефералов: {stats['total_successful']}\n")
        f.write(f"  Выплачено бонусов: ${stats['total_bonus_paid']:.2f}\n")
        f.write("-" * 80 + "\n\n")
        
        # Топ рефереров
        if stats['top_referrers']:
            f.write("ТОП РЕФЕРЕРОВ:\n")
            f.write(f"{'№':<3} {'Имя':<25} {'Всего':<6} {'Успешных':<9} {'Заработано':<12}\n")
            f.write("-" * 60 + "\n")
            for i, user in enumerate(stats['top_referrers'], 1):
                user_id, username, total_ref, successful_ref, earned_bonus = user
                username_display = username or f"ID{user_id}"
                if len(username_display) > 20:
                    username_display = username_display[:17] + "..."
                
                f.write(f"{i:<3} @{username_display:<24} {total_ref:<6} {successful_ref:<9} ${earned_bonus:<11.2f}\n")
            f.write("-" * 80 + "\n\n")
        
        # Детали по рефералам
        f.write("ДЕТАЛИ РЕФЕРАЛОВ:\n\n")
        f.write(f"{'ID':<6} {'Реферер':<25} {'Реферал':<25} {'Дата':<12} {'Статус':<12} {'Бонус':<8}\n")
        f.write("-" * 100 + "\n")
        
        for ref in all_referrals:
            ref_id, referrer_id, referred_id, has_completed, bonus_paid, created_at, _, referrer_name, referred_name = ref
            
            referrer_display = referrer_name or f"ID{referrer_id}"
            referred_display = referred_name or f"ID{referred_id}"
            
            if len(referrer_display) > 20:
                referrer_display = referrer_display[:17] + "..."
            if len(referred_display) > 20:
                referred_display = referred_display[:17] + "..."
            
            status = "УСПЕШНО" if has_completed else "В ПРОЦЕССЕ"
            bonus_status = "ВЫПЛАЧЕН" if bonus_paid else "НЕТ"
            created_date = created_at.split()[0] if created_at else "—"
            
            f.write(f"{ref_id:<6} @{referrer_display:<24} @{referred_display:<24} {created_date:<12} {status:<12} {bonus_status:<8}\n")
    
    try:
        # Отправляем файл
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📊 **Отчет по реферальной системе**\n\n"
                   f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                   f"👥 Всего рефералов: {stats['total_referrals']}\n"
                   f"💰 Выплачено: ${stats['total_bonus_paid']:.2f}",
            parse_mode="None"
        )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка отправки файла: {e}", show_alert=True)
    finally:
        # Удаляем временный файл
        if os.path.exists(filename):
            os.remove(filename)

# ============================================
# ОБРАБОТЧИКИ УПРАВЛЕНИЯ РЕЖИМАМИ
# ============================================

@dp.callback_query(F.data == "admin_modes")
async def admin_modes_handler(callback: CallbackQuery):
    """Кнопка управления режимами"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    night_mode = db.get_night_mode()
    weekend_mode = db.get_weekend_mode()
    system_message = db.get_system_message()
    
    night_status = "✅ ВКЛЮЧЕН" if night_mode == 1 else "❌ ВЫКЛЮЧЕН"
    weekend_status = "✅ ВКЛЮЧЕН" if weekend_mode == 1 else "❌ ВЫКЛЮЧЕН"
    system_status = "📢 УСТАНОВЛЕНО" if system_message else "❌ НЕТ"
    
    text = (f"⚙️ **Управление режимами работы**\n\n"
            f"🌙 **Ночной режим** (22:00-10:00): {night_status}\n"
            f"📅 **Режим выходных:** {weekend_status}\n"
            f"💬 **Системное сообщение:** {system_status}\n\n"
            f"Выберите действие:")
    
    buttons = [
        [InlineKeyboardButton(text=f"🌙 {'❌ ВЫКЛЮЧИТЬ' if night_mode == 1 else '✅ ВКЛЮЧИТЬ'} ночной режим", 
                              callback_data=f"toggle_night_{1 if night_mode == 0 else 0}")],
        [InlineKeyboardButton(text=f"📅 {'❌ ВЫКЛЮЧИТЬ' if weekend_mode == 1 else '✅ ВКЛЮЧИТЬ'} режим выходных", 
                              callback_data=f"toggle_weekend_{1 if weekend_mode == 0 else 0}")],
        [InlineKeyboardButton(text="💬 Установить системное сообщение", callback_data="set_system_message")],
        [InlineKeyboardButton(text="🗑 Очистить системное сообщение", callback_data="clear_system_message")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data.startswith("toggle_night_"))
async def toggle_night_mode_handler(callback: CallbackQuery):
    """Переключить ночной режим"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    new_status = int(callback.data.split("_")[2])
    db.set_night_mode(new_status)
    
    action = "включен" if new_status == 1 else "выключен"
    await callback.answer(f"✅ Ночной режим {action}", show_alert=True)
    await admin_modes_handler(callback)

@dp.callback_query(F.data.startswith("toggle_weekend_"))
async def toggle_weekend_mode_handler(callback: CallbackQuery):
    """Переключить режим выходных"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    new_status = int(callback.data.split("_")[2])
    db.set_weekend_mode(new_status)
    
    action = "включен" if new_status == 1 else "выключен"
    await callback.answer(f"✅ Режим выходных {action}", show_alert=True)
    await admin_modes_handler(callback)

# ============================================
# СКРЫТАЯ НАДБАВКА ВРЕМЕНИ
# ============================================

@dp.callback_query(F.data == "admin_hidden_time_bonus")
async def admin_hidden_time_bonus_handler(callback: CallbackQuery):
    """Управление скрытой надбавкой времени"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    # Получаем список тарифов
    tariffs = db.cursor.execute(
        "SELECT id, name, duration_min FROM tariffs WHERE is_active = 1 ORDER BY id"
    ).fetchall()
    
    if not tariffs:
        await callback.message.edit_text(
            "❌ Нет активных тарифов для настройки",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
            ])
        )
        return
    
    text = "🕐 **Управление скрытой надбавкой времени**\n\n"
    text += "⚠️ **Внимание:** Эта надбавка не отображается пользователям и операторам.\n"
    text += "Номер будет считаться 'отстоявшим' только после прохождения стандартного времени + надбавка.\n\n"
    
    text += "📋 **Список тарифов:**\n"
    for tariff in tariffs:
        tariff_id, name, duration = tariff
        hidden_bonus = db.get_hidden_time_bonus(tariff_id)
        real_duration = duration + hidden_bonus
        
        text += f"📱 **{name}**\n"
        text += f"   Стандартное: {duration} мин | "
        text += f"Скрытая надбавка: {hidden_bonus} мин | "
        text += f"Реальное время: {real_duration} мин\n"
    
    text += "\nВыберите тариф для настройки:"
    
    buttons = []
    for tariff in tariffs:
        tariff_id, name, _ = tariff
        hidden_bonus = db.get_hidden_time_bonus(tariff_id)
        button_text = f"⚙️ {name} ({hidden_bonus}+ мин)"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"set_hidden_bonus_{tariff_id}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")])
    
    await callback.message.edit_text(
        text, 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="None"
    )

@dp.callback_query(F.data.startswith("set_hidden_bonus_"))
async def set_hidden_bonus_handler(callback: CallbackQuery, state: FSMContext):
    """Настройка скрытой надбавки для конкретного тарифа"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    tariff_id = int(callback.data.split("_")[3])
    
    # Получаем информацию о тарифе
    tariff_info = db.cursor.execute(
        "SELECT name, duration_min FROM tariffs WHERE id = ?", 
        (tariff_id,)
    ).fetchone()
    
    if not tariff_info:
        await callback.answer("❌ Тариф не найден", show_alert=True)
        return
    
    name, duration = tariff_info
    current_bonus = db.get_hidden_time_bonus(tariff_id)
    real_duration = duration + current_bonus
    
    await state.update_data(tariff_id=tariff_id, tariff_name=name)
    await state.set_state(Form.waiting_for_hidden_bonus_minutes)
    
    text = f"🕐 **Настройка скрытой надбавки времени**\n\n"
    text += f"📱 **Тариф:** {name}\n"
    text += f"⏱ **Стандартное время:** {duration} минут\n"
    text += f"➕ **Текущая скрытая надбавка:** {current_bonus} минут\n"
    text += f"⏳ **Реальное время для отстоя:** {real_duration} минут\n\n"
    text += "Введите новую скрытую надбавку (в минутах):\n"
    text += "Пример: 10 (добавит 10 минут к стандартному времени)\n"
    text += "Или введите 0 для отключения надбавки"
    
    buttons = [
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_hidden_time_bonus")]
    ]
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_hidden_bonus_minutes)
async def process_hidden_bonus_minutes(message: types.Message, state: FSMContext):
    """Обработка ввода скрытой надбавки"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    if not message.text.isdigit():
        await message.answer("❌ Введите целое число (минуты)!")
        return
    
    bonus_minutes = int(message.text)
    
    if bonus_minutes < 0:
        await message.answer("❌ Надбавка не может быть отрицательной!")
        return
    
    data = await state.get_data()
    tariff_id = data['tariff_id']
    tariff_name = data['tariff_name']
    
    # Сохраняем надбавку
    db.set_hidden_time_bonus(tariff_id, bonus_minutes)
    
    # Получаем обновленную информацию
    tariff_info = db.cursor.execute(
        "SELECT duration_min FROM tariffs WHERE id = ?", 
        (tariff_id,)
    ).fetchone()
    
    duration = tariff_info[0] if tariff_info else 0
    real_duration = duration + bonus_minutes
    
    await message.answer(
        f"✅ **Скрытая надбавка времени обновлена!**\n\n"
        f"📱 **Тариф:** {tariff_name}\n"
        f"⏱ **Стандартное время:** {duration} минут\n"
        f"➕ **Скрытая надбавка:** {bonus_minutes} минут\n"
        f"⏳ **Реальное время для отстоя:** {real_duration} минут\n\n"
        f"⚠️ **Важно:** Эта настройка видна только главным администраторам.\n"
        f"Пользователи и операторы увидят только стандартное время {duration} мин.",
        parse_mode="None"
    )
    
    await state.clear()
    await admin_hidden_time_bonus_handler(message)

# ============================================
# ФЕЙКОВАЯ ОЧЕРЕДЬ
# ============================================

@dp.callback_query(F.data == "admin_fake_queue")
async def admin_fake_queue_handler(callback: CallbackQuery):
    """Управление фейковой очередью"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    fake_count = db.get_fake_queue()
    real_count = db.get_real_queue_count()
    total = real_count + fake_count
    
    text = (f"⚙️ **Управление фейковой очередью**\n\n"
            f"📊 **Реальная очередь:** {real_count} номеров\n"
            f"🎭 **Фейковая очередь:** {fake_count} номеров\n"
            f"📈 **Итоговое отображение:** {total} номеров\n\n"
            f"Выберите действие:")
    
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить фейковые", callback_data="fake_queue_add")],
        [InlineKeyboardButton(text="➖ Убрать фейковые", callback_data="fake_queue_remove")],
        [InlineKeyboardButton(text="🎯 Установить точное число", callback_data="fake_queue_set")],
        [InlineKeyboardButton(text="🔄 Сбросить (0)", callback_data="fake_queue_reset")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_edit_priority")
async def admin_edit_priority_handler(callback: CallbackQuery):
    """Настройка приоритета"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    price, name = db.get_priority_settings()
    
    text = (f"⭐ **Настройки приоритетного режима**\n\n"
            f"🏷 **Название:** {name}\n"
            f"💰 **Наценка:** ${price}\n\n"
            f"Что вы хотите изменить?")
    
    buttons = [
        [InlineKeyboardButton(text="✏️ Изменить название", callback_data="adm_pri_name")],
        [InlineKeyboardButton(text="💰 Изменить наценку", callback_data="adm_pri_price")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_ban_menu")
async def admin_ban_menu_handler(callback: CallbackQuery):
    """Меню управления банами"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    text = "🚫 **Управление банами пользователей**\n\nВыберите действие:"
    
    buttons = [
        [InlineKeyboardButton(text="🚫 Забанить пользователя", callback_data="ban_user_start")],
        [InlineKeyboardButton(text="✅ Разбанить пользователя", callback_data="unban_user_start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "ban_user_start")
async def ban_user_start_handler(callback: CallbackQuery, state: FSMContext):
    """Начать процесс бана пользователя"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_ban_id)
    
    text = "🚫 **Бан пользователя**\n\nВведите Telegram ID пользователя для бана:"
    
    buttons = [
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_ban_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "unban_user_start")
async def unban_user_start_handler(callback: CallbackQuery, state: FSMContext):
    """Начать процесс разбана пользователя"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_unban_id)
    
    text = "✅ **Разбан пользователя**\n\nВведите Telegram ID пользователя для разбана:"
    
    buttons = [
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_ban_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.message(Form.waiting_for_ban_id)
async def process_ban_id(message: types.Message, state: FSMContext):
    """Обработка ID для бана"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    if not message.text.isdigit():
        await message.answer("❌ ID должен быть числом!")
        return
    
    user_id = int(message.text)
    
    # Проверяем, не пытаемся ли забанить супер-админа
    if user_id in ADMIN_IDS:
        await message.answer("❌ Нельзя забанить главного администратора!")
        await state.clear()
        return
    
    # Проверяем, не пытаемся ли забанить самого себя
    if user_id == message.from_user.id:
        await message.answer("❌ Нельзя забанить самого себя!")
        await state.clear()
        return
    
    # Проверяем, существует ли пользователь
    user_exists = db.cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if not user_exists:
        await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе.")
        await state.clear()
        return
    
    # Баним пользователя
    db.toggle_ban(user_id, 1)
    
    username = user_exists[0] or f"ID {user_id}"
    await message.answer(f"✅ Пользователь @{username} (ID: {user_id}) забанен!",
                        reply_markup=get_admin_keyboard(True),
                        parse_mode="None")
    
    await state.clear()

@dp.message(Form.waiting_for_unban_id)
async def process_unban_id(message: types.Message, state: FSMContext):
    """Обработка ID для разбана"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    if not message.text.isdigit():
        await message.answer("❌ ID должен быть числом!")
        return
    
    user_id = int(message.text)
    
    # Проверяем, существует ли пользователь
    user_exists = db.cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if not user_exists:
        await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе.")
        await state.clear()
        return
    
    # Разбаниваем пользователя
    db.toggle_ban(user_id, 0)
    
    username = user_exists[0] or f"ID {user_id}"
    await message.answer(f"✅ Пользователь @{username} (ID: {user_id}) разбанен!",
                        reply_markup=get_admin_keyboard(True),
                        parse_mode="None")
    
    await state.clear()

@dp.callback_query(F.data == "admin_count_queue")
async def admin_count_queue_handler(callback: CallbackQuery):
    """Статистика очереди"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    total_count = db.get_queue_count()
    real_count = db.get_real_queue_count()
    fake_count = db.get_fake_queue()
    
    text = (f"📊 **Статистика очереди:**\n\n"
            f"🔢 **Всего отображается:** {total_count} номеров\n"
            f"📊 **Из них реальных:** {real_count}\n"
            f"🎭 **Фейковых:** {fake_count}")
    
    await callback.answer(text, show_alert=True)

@dp.callback_query(F.data == "admin_clear_queue_start")
async def admin_clear_queue_start_handler(callback: CallbackQuery):
    """Подтверждение очистки очереди"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    text = "⚠️ **Очистка очереди**\n\nВы уверены, что хотите полностью очистить текущую очередь?\n\nЭта операция необратима!"
    
    buttons = [
        [InlineKeyboardButton(text="✅ ДА, ОЧИСТИТЬ", callback_data="admin_clear_queue_confirm")],
        [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_clear_queue_confirm")
async def admin_clear_queue_confirm_handler(callback: CallbackQuery):
    """Очистка очереди"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    db.clear_all_queue()
    await callback.answer("✅ Очередь полностью очищена!", show_alert=True)
    await admin_cmd(types.Message(chat=callback.message.chat, from_user=callback.from_user))

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_handler(callback: CallbackQuery, state: FSMContext):
    """Рассылка сообщений"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    await state.set_state(Form.waiting_for_broadcast_text)
    
    text = "📢 **Рассылка сообщений**\n\nОтправьте текст, фото, видео или документ для рассылки всем пользователям:"
    
    buttons = [
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_add_new")
async def admin_add_new_handler(callback: CallbackQuery, state: FSMContext):
    """Добавление оператора"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    await state.set_state(Form.waiting_for_new_admin_id)
    
    text = "➕ **Добавление оператора**\n\nВведите Telegram ID нового оператора:"
    
    buttons = [
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_remove_start")
async def admin_remove_start_handler(callback: CallbackQuery):
    """Снятие оператора"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    admins = db.get_admins_list()
    
    if not admins:
        text = "📋 **Список операторов пуст**"
        buttons = [[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]]
    else:
        text = "❌ **Снятие оператора**\n\nВыберите оператора для снятия:"
        buttons = []
        for a in admins:
            buttons.append([InlineKeyboardButton(text=f"👤 {a[1] or a[0]}", callback_data=f"rem_adm_{a[0]}")])
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_list")
async def admin_list_handler(callback: CallbackQuery):
    """Список операторов"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    admins = db.get_admins_list()
    
    if not admins:
        text = "📋 **Список операторов пуст**"
    else:
        text = "📋 **Список операторов:**\n\n"
        for i, a in enumerate(admins, 1):
            text += f"{i}. 👤 @{a[1] or '—'} (ID: `{a[0]}`)\n"
    
    buttons = [[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

# ============================================
# ОБРАБОТЧИКИ УДАЛЕНИЯ АДМИНА
# ============================================

@dp.callback_query(F.data.startswith("rem_adm_"))
async def remove_admin_handler(callback: CallbackQuery):
    """Удаление оператора"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    user_id_to_remove = int(callback.data.split("_")[2])
    
    # Не позволяем удалить самого себя
    if user_id_to_remove == callback.from_user.id:
        await callback.answer("❌ Нельзя снять самого себя", show_alert=True)
        return
    
    # Не позволяем удалить другого супер-админа (из ADMIN_IDS)
    if user_id_to_remove in ADMIN_IDS:
        await callback.answer("❌ Нельзя снять главного администратора", show_alert=True)
        return
    
    # Удаляем оператора
    db.remove_admin(user_id_to_remove)
    
    # Получаем информацию об удаленном операторе для сообщения
    removed_admin = db.cursor.execute(
        "SELECT username FROM users WHERE user_id = ?", 
        (user_id_to_remove,)
    ).fetchone()
    
    username = removed_admin[0] if removed_admin else str(user_id_to_remove)
    
    await callback.answer(f"✅ Оператор @{username} снят", show_alert=True)
    await admin_remove_start_handler(callback)  # Возвращаемся к списку

# ============================================
# ОБРАБОТЧИКИ ДЕЙСТВИЙ С НОМЕРАМИ (админы)
# ============================================

@dp.callback_query(F.data.startswith("vstal_"))
async def vstal_handler(callback: CallbackQuery):
    """Номер взят в работу с блокировкой"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    n_id = callback.data.split("_")[1]
    
    # Проверяем блокировку номера
    locked_by = db.is_number_locked(n_id)
    if locked_by and locked_by != user_id:
        await callback.answer(f"⚠️ Этот номер уже взят оператором ID: {locked_by}", show_alert=True)
        try:
            await callback.message.delete()
        except:
            pass
        return
    
    # Проверяем, не взят ли уже номер другим оператором
    current_status = db.cursor.execute(
        "SELECT status FROM numbers WHERE id = ?", 
        (n_id,)
    ).fetchone()
    
    if current_status and current_status[0] != 'Ожидание':
        await callback.answer("⚠️ Этот номер уже взят другим оператором!", show_alert=True)
        # Удаляем сообщение с номером, который уже занят
        try:
            await callback.message.delete()
        except:
            pass
        return
    
    number_info = db.set_number_vstal(n_id)  # Теперь возвращает полную информацию
    
    if number_info:
        # Уменьшаем фейковую очередь при взятии реального номера
        decreased, amount, new_fake = decrease_fake_queue_on_number_taken()
        
        phone, u_id, username, is_prio = number_info
        # Уведомляем пользователя
        try: 
            await bot.send_message(u_id, "ℹ️ Ваш номер взят в работу оператором.")
        except: 
            pass
        
        # Обновляем сообщение
        _, p_name = db.get_priority_settings()
        prio_label = f"⭐ [{p_name}] " if is_prio else ""
        
        # Экранируем спецсимволы
        safe_phone = escape_markdown(phone)
        safe_username = escape_markdown(username or 'User')
        
        # Вместо удаления сообщения, создаем новое с обновленными кнопками
        new_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)\n\n🟡 **СТАТУС: В РАБОТЕ**"

        new_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏁 Завершить", callback_data=f"slet_{n_id}")],
            [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{n_id}"),
             InlineKeyboardButton(text="⏭ Ошибка", callback_data=f"err_{n_id}")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="admin_panel_back")]
        ])

        # Создаем новое сообщение с меню
        await callback.message.answer(new_text, reply_markup=new_kb, parse_mode="None")

        # Удаляем старое сообщение с номером в очереди
        try:
            await callback.message.delete()
        except:
            pass
    else:
        await callback.answer("❌ Ошибка при взятии номера", show_alert=True)

@dp.callback_query(F.data.startswith("slet_"))
async def slet_handler(callback: CallbackQuery):
    """Обработчик кнопки 'Слет / Отстоял' с учетом скрытой надбавки"""
    user_id = callback.from_user.id
    n_id = int(callback.data.split("_")[1])
    
    # Проверяем, заблокирован ли номер для этого оператора
    locked_by = db.is_number_locked(n_id)
    if locked_by and locked_by != user_id:
        await callback.answer(f"⚠️ Этот номер уже взят оператором ID: {locked_by}", show_alert=True)
        return
    
    # Разблокируем номер (больше не нужен)
    db.unlock_number(n_id)
    
    # Удаляем запись о сообщении оператора
    if n_id in operator_number_messages:
        del operator_number_messages[n_id]
    
    # Завершаем номер с учетом скрытой надбавки времени
    # Передаем ID оператора для определения, кто завершает (админ или оператор)
    result = db.set_number_slet(n_id, user_id)
    
    if not result:
        await callback.answer("❌ Ошибка: номер не найден или не в работе", show_alert=True)
        return
    
    # Получаем информацию о тарифе для отображения
    tariff_info = db.cursor.execute("""
        SELECT t.name, t.duration_min FROM numbers n
        JOIN tariffs t ON n.tariff_id = t.id
        WHERE n.id = ?
    """, (n_id,)).fetchone()
    
    tariff_name = tariff_info[0] if tariff_info else "Неизвестно"
    standard_duration = tariff_info[1] if tariff_info else 0
    
    # Получаем информацию о номере для уведомления пользователя
    number_info = db.cursor.execute("""
        SELECT phone, user_id FROM numbers WHERE id = ?
    """, (n_id,)).fetchone()
    
    if number_info:
        phone, target_user_id = number_info
        
        # Определяем, является ли текущий пользователь главным админом
        is_super_admin = user_id in ADMIN_IDS
        
        if is_super_admin:
            # Главный админ видит реальное время
            real_duration = standard_duration + result.get("hidden_bonus", 0)
            admin_message = f"\n⚠️ **Только для админа:**\n"
            admin_message += f"Реальное время: {real_duration} мин (стандарт {standard_duration} + надбавка {result.get('hidden_bonus', 0)})\n"
            admin_message += f"Прошло минут: {result.get('minutes_passed', 0)}\n"
            admin_message += f"Реальный статус: {result.get('real_status', 'Неизвестно')}"
        else:
            # Оператор видит только стандартное время
            admin_message = ""
        
        # Упрощенное сообщение для оператора (БЕЗ ВРЕМЕНИ)
        op_message = f"📱 Номер `{phone}` завершен.\n"
        op_message += f"📊 Статус: **{result['display_status']}**\n"
        op_message += f"📋 Тариф: {tariff_name}"
        
        # Уведомление пользователя (показываем только display_status)
        try:
            await bot.send_message(
                target_user_id,
                f"📱 **Номер `{phone}` завершен**\n\n"
                f"📊 **Статус:** {result['display_status']}\n"
                f"⏱ **Тариф:** {tariff_name} ({standard_duration} мин)\n\n"
                f"💬 **Техподдержка:** @magic_team_support",
                parse_mode="None"
            )
        except:
            pass  # Пользователь заблокировал бота
        
        # Если был начислен реферальный бонус
        if result.get("referral_bonus"):
            ref_info = result["referral_bonus"]
            await callback.message.answer(
                f"💰 **Начислен реферальный бонус!**\n\n"
                f"👤 Пользователь ID: {result['user_id']}\n"
                f"👥 Реферер ID: {ref_info['referrer_id']}\n"
                f"💰 Сумма: ${ref_info['bonus']}",
                parse_mode="None"
            )
        
        await callback.message.edit_text(op_message, parse_mode="None")
        await callback.answer("✅ Статус номера обновлен")
    
    # Уменьшаем фейковую очередь при завершении номера
    decreased, amount, new_fake = decrease_fake_queue_on_number_completion()
    if decreased:
        logging.info(f"Фейковая очередь уменьшена на {amount} при завершении номера. Новое значение: {new_fake}")

@dp.callback_query(F.data.startswith("err_"))
async def err_handler(callback: CallbackQuery):
    """Удаление номера с ошибкой"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    n_id = callback.data.split("_")[1]
    
    # Проверяем, может ли этот оператор удалить этот номер
    current_status = db.cursor.execute(
        "SELECT status FROM numbers WHERE id = ?", 
        (n_id,)
    ).fetchone()
    
    # Если номер уже обработан (не "Ожидание" и не "В работе"), не позволяем удалить
    if current_status and current_status[0] not in ['Ожидание', 'В работе']:
        await callback.answer("⚠️ Этот номер уже обработан и не может быть удален!", show_alert=True)
        return
    
    u_id = db.delete_number_with_error(n_id)
    
    if u_id:
        try: 
            await bot.send_message(u_id, "⚠️ Номер удален (ошибка).")
        except: 
            pass
    
    await callback.message.edit_text("❌ **Номер удален** (ошибка)")

@dp.callback_query(F.data.startswith("reply_"))
async def reply_start_handler(callback: CallbackQuery, state: FSMContext):
    """Начать ответ пользователю"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    number_id = callback.data.split("_")[1]
    res = db.cursor.execute("SELECT user_id, phone FROM numbers WHERE id = ?", (number_id,)).fetchone()
    if res:
        await state.update_data(reply_to_user_id=res[0], reply_to_phone=res[1], number_id=number_id)
        await state.set_state(Form.waiting_for_reply_text)
        await callback.message.answer(f"💬 **Ответ по номеру {res[1]}:**", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_back")]
        ]))
    await callback.answer()

@dp.message(Form.waiting_for_reply_text)
async def reply_send_handler(message: types.Message, state: FSMContext):
    """Отправить ответ пользователю (с поддержкой фото)"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    data = await state.get_data()
    
    try:
        safe_phone = escape_markdown(data['reply_to_phone'])
        user_id_to_reply = data['reply_to_user_id']
        number_id = data.get('number_id')
        
        # Отправляем заголовок пользователю
        caption = f"🔔 **Сообщение по номеру {safe_phone}:**"
        
        if message.photo:
            # Если отправлено фото
            photo = message.photo[-1]
            
            # Создаем кнопки только если есть number_id
            repeat_kb = None
            if number_id:
                # Создаем callback_data для кнопок "Повтор" и "Куар"
                # user_id здесь - это ID оператора, который отправляет сообщение
                callback_photo = create_repeat_callback(number_id, admin_id=user_id, request_type="photo")
                callback_qr = create_repeat_callback(number_id, admin_id=user_id, request_type="qr")
                
                # Создаем клавиатуру с двумя кнопками
                repeat_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="🔄 Повтор", callback_data=callback_photo),
                        InlineKeyboardButton(text="📱 Куар", callback_data=callback_qr)
                    ]
                ])
            
            # Отправляем фото пользователю с кнопками (если они есть)
            await bot.send_photo(
                user_id_to_reply,
                photo.file_id,
                caption=caption,
                reply_markup=repeat_kb,
                parse_mode="None"
            )
            
            await message.answer(f"✅ Фото отправлено. Пользователь может запросить повтор.")
            
        elif message.text:
            # Если отправлен текст
            await bot.send_message(user_id_to_reply, caption, parse_mode="None")
            await message.copy_to(user_id_to_reply)
            await message.answer("✅ Текст отправлен")
        else:
            # Если отправлен другой тип медиа
            await bot.send_message(user_id_to_reply, caption, parse_mode="None")
            await message.copy_to(user_id_to_reply)
            await message.answer("✅ Медиа отправлено")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {str(e)}")
    
    await state.clear()

# Вспомогательная функция для создания callback_data для кнопки повтора
def create_repeat_callback(number_id, admin_id=None, request_type="photo"):
    """
    Создать callback_data для кнопки повтора
    
    Args:
        number_id: ID номера
        admin_id: ID оператора (опционально)
        request_type: тип запроса - "photo" или "qr"
    """
    callback_parts = ["simple_repeat", request_type, str(number_id)]
    if admin_id:
        callback_parts.append(str(admin_id))
    
    return "_".join(callback_parts)

# Обработчик для кнопки "Повтор" у пользователя
@dp.callback_query(F.data.startswith("simple_repeat_"))
async def simple_repeat_handler(callback: CallbackQuery, state: FSMContext):
    """Пользователь нажал кнопку 'Повтор' или 'Куар'"""
    user_id = callback.from_user.id
    
    if db.is_user_banned(user_id):
        await callback.answer("❌ Вы забанены", show_alert=True)
        return
    
    # Получаем данные из callback_data
    # Формат: simple_repeat_{type}_{number_id}_{admin_id}
    parts = callback.data.split("_")
    if len(parts) < 4:  # Должно быть как минимум 4 части: simple, repeat, type, number_id
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        # parts[0] = "simple", parts[1] = "repeat", parts[2] = "photo"/"qr", parts[3] = number_id, parts[4] = admin_id (опционально)
        request_type = parts[2]  # "photo" или "qr"
        number_id = int(parts[3])
        admin_id = int(parts[4]) if len(parts) > 4 else None
        
        # Проверяем валидность типа запроса
        if request_type not in ["photo", "qr"]:
            await callback.answer("❌ Неверный тип запроса", show_alert=True)
            return
    except (ValueError, IndexError) as e:
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data {callback.data}: {e}")
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT n.phone, n.user_id, u.username FROM numbers n LEFT JOIN users u ON n.user_id = u.user_id WHERE n.id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone, number_user_id, username = number_info
    
    # Проверяем, принадлежит ли номер этому пользователю
    if number_user_id != user_id:
        await callback.answer("❌ Это не ваш номер", show_alert=True)
        return
    
    # Удаляем кнопки у пользователя
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    
    request_text = "фото" if request_type == "photo" else "QR-код"
    await callback.answer(f"✅ Запрос {request_text} отправлен оператору", show_alert=True)
    
    # Подготовка данных
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or f"ID{user_id}")
    
    # Создаем клавиатуру для быстрого ответа с указанием типа запроса
    quick_reply_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Отправить повтор", callback_data=f"admin_repeat_{request_type}_{number_id}_{user_id}")]
    ])
    
    # Пытаемся найти и отредактировать существующее сообщение оператора
    edited_count = 0
    
    if number_id in operator_number_messages:
        # Найдено сохраненное сообщение для этого номера
        saved_operator_id, saved_message_id, saved_chat_id = operator_number_messages[number_id]
        
        # Проверяем, нужно ли уведомлять именно этого оператора
        if admin_id is None or saved_operator_id == admin_id:
            try:
                # Получаем информацию о приоритете номера для восстановления полного текста
                number_prio_info = db.cursor.execute(
                    "SELECT is_priority FROM numbers WHERE id = ?", (number_id,)
                ).fetchone()
                is_prio = number_prio_info[0] if number_prio_info else 0
                _, p_name = db.get_priority_settings()
                prio_label = f"⭐ [{p_name}] " if is_prio else ""
                
                # Восстанавливаем оригинальный текст
                original_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{user_id}`)"
                
                # Редактируем сообщение, добавляя информацию о запросе
                new_text = (
                    f"{original_text}\n\n"
                    f"🔄 **Пользователь запросил {request_text}!**\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки {request_text}:"
                )
                
                # Создаем клавиатуру со стандартными кнопками + кнопкой быстрого ответа
                combined_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{number_id}"),
                     InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{number_id}")],
                    [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{number_id}"),
                     InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{number_id}")],
                    [InlineKeyboardButton(text="🔄 Отправить повтор", callback_data=f"admin_repeat_{request_type}_{number_id}_{user_id}")]
                ])
                
                await bot.edit_message_text(
                    chat_id=saved_chat_id,
                    message_id=saved_message_id,
                    text=new_text,
                    reply_markup=combined_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка редактирования сообщения оператора {saved_operator_id}: {e}")
                # Если не удалось отредактировать, отправляем новое сообщение как fallback
                try:
                    await bot.send_message(
                        saved_operator_id,
                        f"🔄 **Пользователь запросил {request_text}!**\n\n"
                        f"📱 Номер: `{safe_phone}`\n"
                        f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                        f"Нажмите кнопку ниже для быстрой отправки {request_text}:",
                        reply_markup=quick_reply_kb,
                        parse_mode="None"
                    )
                    edited_count += 1
                except Exception as e2:
                    logging.error(f"Ошибка отправки сообщения оператору {saved_operator_id}: {e2}")
    else:
        # Сообщение не найдено, отправляем новое (fallback)
        if admin_id:
            admins_to_notify = [int(admin_id)]
        else:
            admins = db.get_admins_list()
            admins_to_notify = [a[0] for a in admins] + ADMIN_IDS
        
        admins_to_notify = list(set(admins_to_notify))
        
        for admin_id_item in admins_to_notify:
            try:
                await bot.send_message(
                    admin_id_item,
                    f"🔄 **Пользователь запросил {request_text}!**\n\n"
                    f"📱 Номер: `{safe_phone}`\n"
                    f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки {request_text}:",
                    reply_markup=quick_reply_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка отправки оператору {admin_id_item}: {e}")
    
    # Уведомляем пользователя
    if edited_count > 0:
        await callback.message.answer(
            f"🔄 **Запрос на {request_text} отправлен оператору!**\n\n"
            "⏳ Ожидайте повторной отправки в ближайшее время.",
            parse_mode="None"
        )
    else:
        await callback.message.answer(
            "❌ **Не удалось отправить запрос оператору**\n\n"
            "Пожалуйста, попробуйте позже.",
            parse_mode="None"
        )

# Обработчик для кнопки "Повтор" с префиксом "repeat_" (из бот 1.py)
@dp.callback_query(F.data.startswith("repeat_"))
async def repeat_handler(callback: CallbackQuery, state: FSMContext):
    """Пользователь нажал кнопку 'Повтор' (альтернативный формат)"""
    user_id = callback.from_user.id
    
    if db.is_user_banned(user_id):
        await callback.answer("❌ Вы забанены", show_alert=True)
        return
    
    # Получаем данные из callback_data
    parts = callback.data.split("_")
    if len(parts) < 2:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        number_id = int(parts[1])
        admin_id = int(parts[2]) if len(parts) > 2 else None
    except ValueError:
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT n.phone, n.user_id, u.username FROM numbers n LEFT JOIN users u ON n.user_id = u.user_id WHERE n.id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone, number_user_id, username = number_info
    
    # Проверяем, принадлежит ли номер этому пользователю
    if number_user_id != user_id:
        await callback.answer("❌ Это не ваш номер", show_alert=True)
        return
    
    # Удаляем кнопку "Повтор" у пользователя
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    
    await callback.answer("✅ Запрос повтора отправлен оператору", show_alert=True)
    
    # Подготовка данных
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or f"ID{user_id}")
    request_text = "фото"
    
    # Создаем клавиатуру для быстрого ответа
    quick_reply_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Отправить повтор", callback_data=f"admin_repeat_photo_{number_id}_{user_id}")]
    ])
    
    # Пытаемся найти и отредактировать существующее сообщение оператора
    edited_count = 0
    
    if number_id in operator_number_messages:
        saved_operator_id, saved_message_id, saved_chat_id = operator_number_messages[number_id]
        
        if admin_id is None or saved_operator_id == admin_id:
            try:
                number_prio_info = db.cursor.execute(
                    "SELECT is_priority FROM numbers WHERE id = ?", (number_id,)
                ).fetchone()
                is_prio = number_prio_info[0] if number_prio_info else 0
                _, p_name = db.get_priority_settings()
                prio_label = f"⭐ [{p_name}] " if is_prio else ""
                
                original_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{user_id}`)"
                new_text = (
                    f"{original_text}\n\n"
                    f"🔄 **Пользователь запросил {request_text}!**\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки {request_text}:"
                )
                
                combined_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{number_id}"),
                     InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{number_id}")],
                    [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{number_id}"),
                     InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{number_id}")],
                    [InlineKeyboardButton(text="🔄 Отправить повтор", callback_data=f"admin_repeat_photo_{number_id}_{user_id}")]
                ])
                
                await bot.edit_message_text(
                    chat_id=saved_chat_id,
                    message_id=saved_message_id,
                    text=new_text,
                    reply_markup=combined_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка редактирования сообщения оператора {saved_operator_id}: {e}")
                try:
                    await bot.send_message(
                        saved_operator_id,
                        f"🔄 **Пользователь запросил {request_text}!**\n\n"
                        f"📱 Номер: `{safe_phone}`\n"
                        f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                        f"Нажмите кнопку ниже для быстрой отправки {request_text}:",
                        reply_markup=quick_reply_kb,
                        parse_mode="None"
                    )
                    edited_count += 1
                except Exception as e2:
                    logging.error(f"Ошибка отправки сообщения оператору {saved_operator_id}: {e2}")
    else:
        # Сообщение не найдено, отправляем новое (fallback)
        if admin_id:
            admins_to_notify = [admin_id]
        else:
            admins = db.get_admins_list()
            admins_to_notify = [a[0] for a in admins] + ADMIN_IDS
        
        admins_to_notify = list(set(admins_to_notify))
        
        for admin_id_item in admins_to_notify:
            try:
                await bot.send_message(
                    admin_id_item,
                    f"🔄 **Пользователь запросил {request_text}!**\n\n"
                    f"📱 Номер: `{safe_phone}`\n"
                    f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки {request_text}:",
                    reply_markup=quick_reply_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка отправки оператору {admin_id_item}: {e}")
    
    # Уведомляем пользователя
    if edited_count > 0:
        await callback.message.answer(
            "🔄 **Запрос на повтор отправлен операторам!**\n\n"
            "⏳ Ожидайте повторной отправки фото в ближайшее время.",
            parse_mode="None"
        )
    else:
        await callback.message.answer(
            "❌ **Не удалось отправить запрос операторам**\n\n"
            "Пожалуйста, попробуйте позже.",
            parse_mode="None"
        )

# Обработчик для быстрой повторной отправки оператором
@dp.callback_query(F.data.startswith("quick_resend_"))
async def quick_resend_handler(callback: CallbackQuery, state: FSMContext):
    """Оператор нажал кнопку быстрой отправки повторного фото/QR"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    parts = callback.data.split("_")
    if len(parts) < 5:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        # Формат: quick_resend_{number_id}_{target_user_id}_{request_type}
        number_id = int(parts[2])
        target_user_id = int(parts[3])
        request_type = parts[4] if len(parts) > 4 else "photo"
        
        # Проверяем валидность типа запроса
        if request_type not in ["photo", "qr"]:
            request_type = "photo"
    except (ValueError, IndexError) as e:
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data {callback.data}: {e}")
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT phone FROM numbers WHERE id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone = number_info[0]
    
    # Определяем, это запрос QR или фото
    is_qr_request = (request_type == "qr")
    request_text = "QR-код" if is_qr_request else "фото"
    
    # Сохраняем данные в состояние для отправки медиа
    await state.update_data(
        reply_to_user_id=target_user_id,
        target_user_id=target_user_id,
        phone=phone,
        number_id=number_id,
        request_type=request_type,
        is_qr_request=is_qr_request
    )
    await state.set_state(Form.waiting_for_repeat_reply)
    
    safe_phone = escape_markdown(phone)
    await callback.message.edit_text(
        f"📤 **Отправка {request_text} пользователю**\n\n"
        f"📱 Номер: `{safe_phone}`\n"
        f"👤 Получатель: ID `{target_user_id}`\n\n"
        f"Отправьте {request_text} для пользователя (фото, изображение QR-кода и т.д.):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_resend")]
        ]),
        parse_mode="None"
    )
    
    await callback.answer()

# Обработчик для повторной отправки оператором (формат admin_repeat_)
@dp.callback_query(F.data.startswith("admin_repeat_"))
async def admin_repeat_handler(callback: CallbackQuery, state: FSMContext):
    """Оператор нажал кнопку быстрой отправки повторного фото/QR через admin_repeat_"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    parts = callback.data.split("_")
    if len(parts) < 5:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        # Формат: admin_repeat_{request_type}_{number_id}_{target_user_id}
        request_type = parts[2]  # "photo" или "qr"
        number_id = int(parts[3])
        target_user_id = int(parts[4])
        
        # Проверяем валидность типа запроса
        if request_type not in ["photo", "qr"]:
            request_type = "photo"
    except (ValueError, IndexError) as e:
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        logging.error(f"Ошибка парсинга callback_data {callback.data}: {e}")
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT phone FROM numbers WHERE id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone = number_info[0]
    
    # Определяем, это запрос QR или фото
    is_qr_request = (request_type == "qr")
    request_text = "QR-код" if is_qr_request else "фото"
    
    # Сохраняем данные в состояние для отправки медиа
    await state.update_data(
        reply_to_user_id=target_user_id,
        target_user_id=target_user_id,
        phone=phone,
        number_id=number_id,
        request_type=request_type,
        is_qr_request=is_qr_request
    )
    await state.set_state(Form.waiting_for_repeat_reply)
    
    safe_phone = escape_markdown(phone)
    await callback.message.edit_text(
        f"📤 **Отправка {request_text} пользователю**\n\n"
        f"📱 Номер: `{safe_phone}`\n"
        f"👤 Получатель: ID `{target_user_id}`\n\n"
        f"Отправьте {request_text} для пользователя (фото, изображение QR-кода и т.д.):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"back_to_number_{number_id}")]
        ]),
        parse_mode="None"
    )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("back_to_number_"))
async def back_to_number_handler(callback: CallbackQuery, state: FSMContext):
    """Возврат к меню управления номером"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Извлекаем number_id из callback_data: back_to_number_{number_id}
    parts = callback.data.split("_")
    if len(parts) < 4:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        await state.clear()
        return
    
    try:
        number_id = int(parts[3])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        await state.clear()
        return
    
    # Очищаем состояние
    await state.clear()
    
    # Восстанавливаем меню номера
    success = await restore_number_menu(number_id, operator_id=user_id)
    
    if success:
        await callback.answer("✅ Возврат к меню номера")
        # Удаляем сообщение с запросом фото или редактируем его
        try:
            await callback.message.delete()
        except:
            pass
    else:
        await callback.answer("❌ Не удалось восстановить меню", show_alert=True)

@dp.callback_query(F.data == "cancel_resend")
async def cancel_resend_handler(callback: CallbackQuery, state: FSMContext):
    """Отмена повторной отправки"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        await state.clear()
        return
    
    # Пытаемся получить number_id из состояния
    data = await state.get_data()
    number_id = data.get('number_id')
    
    await state.clear()
    
    # Если есть number_id, восстанавливаем меню
    if number_id:
        success = await restore_number_menu(number_id, operator_id=user_id)
        if success:
            await callback.answer("✅ Отправка отменена. Возврат к меню номера")
            try:
                await callback.message.delete()
            except:
                pass
        else:
            await callback.message.edit_text("❌ Отправка отменена")
            await callback.answer()
    else:
        await callback.message.edit_text("❌ Отправка отменена")
        await callback.answer()

@dp.callback_query(F.data.startswith("send_photo_"))
async def send_photo_handler(callback: CallbackQuery, state: FSMContext):
    """Оператор нажал 'Отправить фото'"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Формат: send_photo_{number_id}_{target_user_id}
    parts = callback.data.split("_")
    
    if len(parts) < 4:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        number_id = int(parts[2])
        target_user_id = int(parts[3])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT phone FROM numbers WHERE id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone = number_info[0]
    
    # Запрашиваем фото у оператора
    await state.update_data(
        target_user_id=target_user_id,
        phone=phone,
        number_id=number_id,
        is_qr_request=False
    )
    await state.set_state(Form.waiting_for_repeat_reply)
    
    await callback.message.edit_text(
        f"📤 **Отправка повторного фото**\n\n"
        f"📱 Номер: `{escape_markdown(phone)}`\n"
        f"👤 Получатель: ID `{target_user_id}`\n\n"
        f"Отправьте фото для повторной отправки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_resend")]
        ]),
        parse_mode="None"
    )
    
    await callback.answer()


@dp.callback_query(F.data.startswith("send_qr_"))
async def send_qr_handler(callback: CallbackQuery, state: FSMContext):
    """Оператор нажал 'Отправить QR'"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    # Формат: send_qr_{number_id}_{target_user_id}
    parts = callback.data.split("_")
    
    if len(parts) < 4:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        number_id = int(parts[2])
        target_user_id = int(parts[3])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT phone FROM numbers WHERE id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone = number_info[0]
    
    # Запрашиваем QR-код у оператора
    await state.update_data(
        target_user_id=target_user_id,
        phone=phone,
        number_id=number_id,
        is_qr_request=True
    )
    await state.set_state(Form.waiting_for_repeat_reply)
    
    await callback.message.edit_text(
        f"📱 **Отправка QR-кода**\n\n"
        f"📞 Номер: `{escape_markdown(phone)}`\n"
        f"👤 Получатель: ID `{target_user_id}`\n\n"
        f"Отправьте фото QR-кода:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_resend")]
        ]),
        parse_mode="None"
    )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("repeat_qr_"))
async def qr_repeat_handler(callback: CallbackQuery, state: FSMContext):
    """Пользователь нажал кнопку 'Куар'"""
    user_id = callback.from_user.id
    
    if db.is_user_banned(user_id):
        await callback.answer("❌ Вы забанены", show_alert=True)
        return
    
    # Получаем данные из callback_data
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        # parts[0] = "repeat", parts[1] = "qr", parts[2] = number_id, parts[3] = admin_id (опционально)
        number_id = int(parts[2])
        admin_id = int(parts[3]) if len(parts) > 3 else None
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT n.phone, n.user_id, u.username FROM numbers n LEFT JOIN users u ON n.user_id = u.user_id WHERE n.id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone, number_user_id, username = number_info
    
    # Проверяем, принадлежит ли номер этому пользователю
    if number_user_id != user_id:
        await callback.answer("❌ Это не ваш номер", show_alert=True)
        return
    
    # Удаляем кнопку "Куар" у пользователя
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    
    await callback.answer("✅ Запрос QR-кода отправлен оператору", show_alert=True)
    
    # Подготовка данных
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or f"ID{user_id}")
    request_text = "QR-код"
    
    # Создаем клавиатуру для быстрого ответа (QR-код)
    quick_reply_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Отправить QR", callback_data=f"send_qr_{number_id}_{user_id}")]
    ])
    
    # Пытаемся найти и отредактировать существующее сообщение оператора
    edited_count = 0
    
    if number_id in operator_number_messages:
        saved_operator_id, saved_message_id, saved_chat_id = operator_number_messages[number_id]
        
        if admin_id is None or saved_operator_id == admin_id:
            try:
                number_prio_info = db.cursor.execute(
                    "SELECT is_priority FROM numbers WHERE id = ?", (number_id,)
                ).fetchone()
                is_prio = number_prio_info[0] if number_prio_info else 0
                _, p_name = db.get_priority_settings()
                prio_label = f"⭐ [{p_name}] " if is_prio else ""
                
                original_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{user_id}`)"
                new_text = (
                    f"{original_text}\n\n"
                    f"📱 **Пользователь запросил {request_text}!**\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки {request_text}:"
                )
                
                combined_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{number_id}"),
                     InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{number_id}")],
                    [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{number_id}"),
                     InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{number_id}")],
                    [InlineKeyboardButton(text="📱 Отправить QR", callback_data=f"send_qr_{number_id}_{user_id}")]
                ])
                
                await bot.edit_message_text(
                    chat_id=saved_chat_id,
                    message_id=saved_message_id,
                    text=new_text,
                    reply_markup=combined_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка редактирования сообщения оператора {saved_operator_id}: {e}")
                try:
                    await bot.send_message(
                        saved_operator_id,
                        f"📱 **Пользователь запросил {request_text}!**\n\n"
                        f"📱 Номер: `{safe_phone}`\n"
                        f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                        f"Нажмите кнопку ниже для быстрой отправки {request_text}:",
                        reply_markup=quick_reply_kb,
                        parse_mode="None"
                    )
                    edited_count += 1
                except Exception as e2:
                    logging.error(f"Ошибка отправки сообщения оператору {saved_operator_id}: {e2}")
    else:
        # Сообщение не найдено, отправляем новое (fallback)
        if admin_id:
            admins_to_notify = [admin_id]
        else:
            admins = db.get_admins_list()
            admins_to_notify = [a[0] for a in admins] + ADMIN_IDS
        
        admins_to_notify = list(set(admins_to_notify))
        
        for admin_id_item in admins_to_notify:
            try:
                await bot.send_message(
                    admin_id_item,
                    f"📱 **Пользователь запросил {request_text}!**\n\n"
                    f"📱 Номер: `{safe_phone}`\n"
                    f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки {request_text}:",
                    reply_markup=quick_reply_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка отправки оператору {admin_id_item}: {e}")
    
    # Уведомляем пользователя
    if edited_count > 0:
        await callback.message.answer(
            "📱 **Запрос на QR-код отправлен оператору!**\n\n"
            "⏳ Ожидайте QR-код в ближайшее время.",
            parse_mode="None"
        )
    else:
        await callback.message.answer(
            "❌ **Не удалось отправить запрос оператору**\n\n"
            "Пожалуйста, попробуйте позже.",
            parse_mode="None"
        )

@dp.callback_query(F.data.startswith("quick_qr_"))
async def quick_qr_handler(callback: CallbackQuery, state: FSMContext):
    """Оператор нажал 'Отправить QR'"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    parts = callback.data.split("_")
    if len(parts) < 4:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        number_id = int(parts[2])
        target_user_id = int(parts[3])
    except ValueError:
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT phone FROM numbers WHERE id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone = number_info[0]
    
    # Запрашиваем QR-код у оператора
    await state.update_data(
        target_user_id=target_user_id,
        phone=phone,
        number_id=number_id,
        is_qr_request=True
    )
    await state.set_state(Form.waiting_for_repeat_reply)
    
    await callback.message.edit_text(
        f"📱 **Отправка QR-кода**\n\n"
        f"📞 Номер: `{escape_markdown(phone)}`\n"
        f"👤 Получатель: ID `{target_user_id}`\n\n"
        f"Отправьте фото QR-кода:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_resend")]
        ]),
        parse_mode="None"
    )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("repeat_photo_"))
async def repeat_photo_handler(callback: CallbackQuery, state: FSMContext):
    """Пользователь нажал кнопку 'Повтор' (формат repeat_photo_)"""
    user_id = callback.from_user.id
    
    if db.is_user_banned(user_id):
        await callback.answer("❌ Вы забанены", show_alert=True)
        return
    
    # Получаем данные из callback_data
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    try:
        # parts[0] = "repeat", parts[1] = "photo", parts[2] = number_id, parts[3] = admin_id (опционально)
        number_id = int(parts[2])
        admin_id = int(parts[3]) if len(parts) > 3 else None
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
        return
    
    # Получаем информацию о номере
    number_info = db.cursor.execute(
        "SELECT n.phone, n.user_id, u.username FROM numbers n LEFT JOIN users u ON n.user_id = u.user_id WHERE n.id = ?",
        (number_id,)
    ).fetchone()
    
    if not number_info:
        await callback.answer("❌ Номер не найден", show_alert=True)
        return
    
    phone, number_user_id, username = number_info
    
    # Проверяем, принадлежит ли номер этому пользователю
    if number_user_id != user_id:
        await callback.answer("❌ Это не ваш номер", show_alert=True)
        return
    
    # Удаляем кнопки у пользователя
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass
    
    await callback.answer("✅ Запрос повтора фото отправлен оператору", show_alert=True)
    
    # Подготовка данных
    safe_phone = escape_markdown(phone)
    safe_username = escape_markdown(username or f"ID{user_id}")
    request_text = "повторное фото"
    
    # Создаем клавиатуру для быстрого ответа (фото)
    quick_reply_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить фото", callback_data=f"send_photo_{number_id}_{user_id}")]
    ])
    
    # Пытаемся найти и отредактировать существующее сообщение оператора
    edited_count = 0
    
    if number_id in operator_number_messages:
        saved_operator_id, saved_message_id, saved_chat_id = operator_number_messages[number_id]
        
        if admin_id is None or saved_operator_id == admin_id:
            try:
                number_prio_info = db.cursor.execute(
                    "SELECT is_priority FROM numbers WHERE id = ?", (number_id,)
                ).fetchone()
                is_prio = number_prio_info[0] if number_prio_info else 0
                _, p_name = db.get_priority_settings()
                prio_label = f"⭐ [{p_name}] " if is_prio else ""
                
                original_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{user_id}`)"
                new_text = (
                    f"{original_text}\n\n"
                    f"🔄 **Пользователь запросил {request_text}!**\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки фото:"
                )
                
                combined_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Встал", callback_data=f"vstal_{number_id}"),
                     InlineKeyboardButton(text="❌ Слет / Отстоял", callback_data=f"slet_{number_id}")],
                    [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{number_id}"),
                     InlineKeyboardButton(text="⏭ Ошибка / Удалить", callback_data=f"err_{number_id}")],
                    [InlineKeyboardButton(text="📤 Отправить фото", callback_data=f"send_photo_{number_id}_{user_id}")]
                ])
                
                await bot.edit_message_text(
                    chat_id=saved_chat_id,
                    message_id=saved_message_id,
                    text=new_text,
                    reply_markup=combined_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка редактирования сообщения оператора {saved_operator_id}: {e}")
                try:
                    await bot.send_message(
                        saved_operator_id,
                        f"🔄 **Пользователь запросил {request_text}!**\n\n"
                        f"📱 Номер: `{safe_phone}`\n"
                        f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                        f"Нажмите кнопку ниже для быстрой отправки фото:",
                        reply_markup=quick_reply_kb,
                        parse_mode="None"
                    )
                    edited_count += 1
                except Exception as e2:
                    logging.error(f"Ошибка отправки сообщения оператору {saved_operator_id}: {e2}")
    else:
        # Сообщение не найдено, отправляем новое (fallback)
        if admin_id:
            admins_to_notify = [admin_id]
        else:
            admins = db.get_admins_list()
            admins_to_notify = [a[0] for a in admins] + ADMIN_IDS
        
        admins_to_notify = list(set(admins_to_notify))
        
        for admin_id_item in admins_to_notify:
            try:
                await bot.send_message(
                    admin_id_item,
                    f"🔄 **Пользователь запросил {request_text}!**\n\n"
                    f"📱 Номер: `{safe_phone}`\n"
                    f"👤 Пользователь: @{safe_username} (ID: `{user_id}`)\n\n"
                    f"Нажмите кнопку ниже для быстрой отправки фото:",
                    reply_markup=quick_reply_kb,
                    parse_mode="None"
                )
                edited_count += 1
            except Exception as e:
                logging.error(f"Ошибка отправки оператору {admin_id_item}: {e}")
    
    # Уведомляем пользователя
    if edited_count > 0:
        await callback.message.answer(
            "🔄 **Запрос на повторное фото отправлен оператору!**\n\n"
            "⏳ Ожидайте фото в ближайшее время.",
            parse_mode="None"
        )
    else:
        await callback.message.answer(
            "❌ **Не удалось отправить запрос оператору**\n\n"
            "Пожалуйста, попробуйте позже.",
            parse_mode="None"
        )

# Обработчик для повторной отправки фото
@dp.message(Form.waiting_for_repeat_reply)
async def repeat_photo_send_handler(message: types.Message, state: FSMContext):
    """Обработка повторной отправки фото или QR-кода"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    data = await state.get_data()
    
    if not message.photo:
        await message.answer("❌ Пожалуйста, отправьте фото")
        return
    
    try:
        photo = message.photo[-1]
        # Может быть target_user_id или reply_to_user_id
        target_user_id = data.get('target_user_id') or data.get('reply_to_user_id')
        if not target_user_id:
            await message.answer("❌ Ошибка: не найден получатель")
            await state.clear()
            return
        
        phone = data.get('phone')
        number_id = data.get('number_id')
        
        # Если phone отсутствует, получаем из базы данных
        if not phone and number_id:
            number_info = db.cursor.execute(
                "SELECT phone FROM numbers WHERE id = ?",
                (number_id,)
            ).fetchone()
            if number_info:
                phone = number_info[0]
        
        if not phone:
            phone = "неизвестен"
        
        # Получаем тип запроса из состояния или определяем по is_qr_request
        request_type = data.get('request_type', 'qr' if data.get('is_qr_request', False) else 'photo')
        is_qr_request = (request_type == 'qr') or data.get('is_qr_request', False)
        
        safe_phone = escape_markdown(phone)
        
        if is_qr_request:
            # Отправляем QR-код пользователю
            caption = f"📱 **QR-код по номеру {safe_phone}:**"
            user_message = "✅ **QR-код отправлен!**\n\nЕсли нужно еще раз, нажмите кнопку ниже:"
            admin_message = "✅ **QR-код отправлен!**"
            button_type = "QR"
        else:
            # Отправляем обычное фото
            caption = f"🔄 **Повторная отправка по номеру {safe_phone}:**"
            user_message = "✅ **Фото повторно отправлено!**\n\nЕсли нужно еще раз, нажмите кнопку ниже:"
            admin_message = "✅ **Фото повторно отправлено!**"
            button_type = "Повтор"
        
        # Отправляем фото/QR пользователю
        await bot.send_photo(
            target_user_id,
            photo.file_id,
            caption=caption,
            parse_mode="None"
        )
        
        # Создаем новые кнопки для пользователя только если есть number_id
        repeat_kb = None
        if number_id:
            # user_id здесь - это ID оператора, который отправляет сообщение
            callback_photo = create_repeat_callback(number_id, admin_id=user_id, request_type="photo")
            callback_qr = create_repeat_callback(number_id, admin_id=user_id, request_type="qr")
            
            # Клавиатура с двумя кнопками
            repeat_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="🔄 Повтор", callback_data=callback_photo),
                    InlineKeyboardButton(text="📱 Куар", callback_data=callback_qr)
                ]
            ])
        
        # Отправляем пользователю сообщение с кнопками (если они есть)
        await bot.send_message(
            target_user_id,
            user_message,
            reply_markup=repeat_kb,
            parse_mode="None"
        )
        
        await message.answer(
            f"{admin_message}\n\n"
            f"📱 Номер: `{safe_phone}`\n"
            f"👤 Получатель: ID `{target_user_id}`",
            parse_mode="None"
        )
        
        # Восстанавливаем меню управления номером после отправки
        if number_id:
            await restore_number_menu(number_id, operator_id=user_id)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {str(e)}")
    
    await state.clear()

# Хранилище запросов (номер_id -> {'user_id': ..., 'phone': ..., 'operator_id': ...})
repeat_requests = {}

@dp.message(F.photo)
async def handle_operator_photo(message: types.Message):
    """Обработка фото от оператора"""
    user_id = message.from_user.id
    
    # Проверяем, является ли пользователь оператором
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        return  # Не оператор, игнорируем
    
    # Здесь можно добавить логику для определения,
    # на какой запрос отвечает оператор
    # Но для простоты будем обрабатывать все фото от операторов
    
    # Получаем последний активный номер, который взял этот оператор
    active_number = db.cursor.execute("""
        SELECT nl.number_id, n.phone, n.user_id 
        FROM number_locks nl
        JOIN numbers n ON nl.number_id = n.id
        WHERE nl.admin_id = ? AND n.status = 'В работе'
        ORDER BY nl.created_at DESC
        LIMIT 1
    """, (user_id,)).fetchone()
    
    if active_number:
        number_id, phone, target_user_id = active_number
        
        # Отправляем фото пользователю
        try:
            await bot.send_photo(
                target_user_id,
                message.photo[-1].file_id,
                caption=f"📱 **По номеру {escape_markdown(phone)}:**",
                parse_mode="None"
            )
            
            # Уведомляем оператора
            await message.answer(f"✅ Фото отправлено пользователю ID {target_user_id}")
            
        except Exception as e:
            await message.answer(f"❌ Ошибка отправки: {str(e)}")
    else:
        # Если оператор просто отправляет фото без активного номера
        await message.answer("ℹ️ У вас нет активных номеров. Фото не отправлено.")

@dp.message(F.reply_to_message)
async def handle_reply_with_photo(message: types.Message):
    """Обработка ответа с фото на запрос"""
    user_id = message.from_user.id
    
    # Проверяем, оператор ли это
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        return
    
    # Проверяем, что это ответ на сообщение с запросом
    reply_text = message.reply_to_message.text or ""
    
    if "Запрос повторного" in reply_text or "Запрос QR-кода" in reply_text or "Пользователь запросил повторное фото" in reply_text or "Пользователь запросил QR-код" in reply_text:
        if message.photo:
            photo = message.photo[-1]
            
            # Пытаемся найти номер в тексте
            phone_match = re.search(r'Номер:\s*([^\n]+)', reply_text)
            
            if phone_match:
                phone = phone_match.group(1).strip()
                # Убираем обратные кавычки если есть
                phone = phone.replace('`', '').strip()
                
                # Ищем пользователя по номеру
                user_info = db.cursor.execute(
                    "SELECT user_id FROM numbers WHERE phone = ? ORDER BY id DESC LIMIT 1",
                    (phone,)
                ).fetchone()
                
                if user_info:
                    target_user_id = user_info[0]
                    
                    # Определяем тип запроса
                    if "фото" in reply_text.lower() or "Повтор" in reply_text:
                        caption = f"🔄 **Повторное фото по номеру {escape_markdown(phone)}:**"
                    else:
                        caption = f"📱 **QR-код по номеру {escape_markdown(phone)}:**"
                    
                    try:
                        await bot.send_photo(
                            target_user_id,
                            photo.file_id,
                            caption=caption,
                            parse_mode="None"
                        )
                        await message.answer("✅ Отправлено!")
                    except Exception as e:
                        await message.answer(f"❌ Ошибка отправки: {str(e)}")

async def send_photo_with_buttons(operator_id, user_id, phone, photo_id):
    """Отправить фото оператору с кнопками для пользователя"""
    
    # Сначала отправляем фото пользователю
    await bot.send_photo(
        user_id,
        photo_id,
        caption=f"📱 **По номеру {escape_markdown(phone)}:**",
        parse_mode="None"
    )
    
    # Получаем ID номера для callback_data
    number_info = db.cursor.execute(
        "SELECT id FROM numbers WHERE phone = ? AND user_id = ? ORDER BY id DESC LIMIT 1",
        (phone, user_id)
    ).fetchone()
    
    if number_info:
        number_id = number_info[0]
        
        # Создаем кнопки для оператора
        callback_photo = create_repeat_callback(number_id, operator_id, request_type="photo")
        callback_qr = create_repeat_callback(number_id, operator_id, request_type="qr")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Повтор", callback_data=callback_photo),
                InlineKeyboardButton(text="📱 Куар", callback_data=callback_qr)
            ]
        ])
        
        # Отправляем оператору сообщение с кнопками
        sent_message = await bot.send_message(
            operator_id,
            f"✅ Фото отправлено пользователю ID {user_id}\n"
            f"📱 Номер: {escape_markdown(phone)}\n\n"
            f"Если пользователь запросит повтор - он нажмет одну из кнопок под фото",
            reply_markup=kb,
            parse_mode="None"
        )
        # Сохраняем message_id для связи с номером
        operator_number_messages[number_id] = (operator_id, sent_message.message_id, sent_message.chat.id)

# ============================================
# ОБРАБОТЧИКИ ВЫБОРА ТАРИФА И ВВОДА НОМЕРА
# ============================================
@dp.message(Form.waiting_for_number)
async def number_input_handler(message: types.Message, state: FSMContext):
    """Обработка введенного номера с проверкой формата Казахстана"""
    is_closed, closed_message = db.is_system_closed()
    if is_closed:
        await message.answer(closed_message, reply_markup=get_main_menu(), parse_mode="None")
        await state.clear()
        return
    
    phone = message.text.strip()
    
    # Проверка формата номера Казахстана
    import re
    
    # Убираем все нецифровые символы
    digits_only = re.sub(r'\D', '', phone)
    
    # Проверяем длину номера
    if len(digits_only) == 11:
        # Проверяем код страны/оператора
        if digits_only.startswith('77') or digits_only.startswith('87') or digits_only.startswith('76') or digits_only.startswith('70'):
            data = await state.get_data()
            
            # Добавляем номер с проверкой на повторение
            success, result_message = db.add_number(message.from_user.id, phone, data['tariff_id'], data['is_priority'])
            
            if success:
                _, p_name = db.get_priority_settings()
                text = "✅ *Номер добавлен в очередь!*"
                if data['is_priority']:
                    text = f"⭐ *{p_name} номер добавлен в начало очереди!*"
                
                # Получаем количество активных номеров пользователя
                active_count = db.get_user_active_numbers_count(message.from_user.id)
                
                # Показываем нормализованный номер для информации
                normalized_number = f"+7{digits_only[1:]}"  # Преобразуем в международный формат
                await message.answer(
                    f"{text}\n\n"
                    f"📱 *Номер:* {normalized_number}\n"
                    f"🇰🇿 *Страна:* Казахстан\n"
                    f"📊 *Ваших номеров в очереди:* {active_count}\n\n"
                    f"⚠️ *Можно сдавать разные номера, но не повторять один и тот же*",
                    reply_markup=get_main_menu(),
                    parse_mode="None"
                )
                
                # Уведомление админов
                for admin_id in ADMIN_IDS:
                    try:
                        alert = f"🔔 *СРОЧНО: {p_name}!\n" if data['is_priority'] else "🔔 **Новый номер!*\n"
                        safe_phone = escape_markdown(phone)
                        await bot.send_message(admin_id, f"{alert}📞 {safe_phone}\n🇰🇿 Казахстан\nНажмите /number", parse_mode="None")
                    except: 
                        pass
            else:
                # Если не удалось добавить номер
                await message.answer(
                    f"{result_message}\n\n"
                    f"📱 *Введенный номер:* {phone}\n"
                    f"🇰🇿 *Попробуйте другой номер Казахстана*",
                    reply_markup=get_main_menu(),
                    parse_mode="None"
                )
            
            await state.clear()
            return
        else:
            await message.answer("❌ *Неверный код оператора!*\n\n🇰🇿 Только номера Казахстана с кодами: 77, 87, 76, 70 и другие", parse_mode="None")
            return
    else:
        await message.answer("❌ *Неверная длина номера!*\n\n📱 Номер должен содержать 11 цифр\nПример: +77012345678 или 87012345678", parse_mode="None")
        return

@dp.callback_query(F.data.startswith("tariff_"))
async def tariff_select_handler(callback: CallbackQuery, state: FSMContext):
    """Выбор тарифа для сдачи номера"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    is_closed, closed_message = db.is_system_closed()
    if is_closed:
        await callback.message.edit_text(closed_message, reply_markup=get_main_menu(), parse_mode="None")
        return
    
    data = callback.data.split("_")
    await state.update_data(tariff_id=data[1], is_priority=int(data[2]))
    await state.set_state(Form.waiting_for_number)
    
    # Получаем количество активных номеров пользователя
    active_count = db.get_user_active_numbers_count(callback.from_user.id)
    
    await callback.message.edit_text(
        f"✏️ *Введите номер телефона Казахстана*\n\n"
        f"📱 *Форматы:*\n"
        f"• +7XXXXXXXXXX (пример: +77012345678)\n"
        f"• 8XXXXXXXXXX (пример: 87012345678)\n"
        f"• 7XXXXXXXXXX (пример: 77012345678)\n\n"
        f"🇰🇿 *Только номера Казахстана!*\n"
        f"Коды операторов: 77, 87, 76, 70 и другие\n\n"
        f"📊 *Ваших номеров в очереди:* {active_count}\n"
        f"⚠️ *Можно сдавать разные номера, но не повторять один и тот же*"
    )
    await callback.answer()

# ============================================
# ОБРАБОТЧИКИ СОСТОЯНИЙ (FSM)
# ============================================

@dp.message(Form.waiting_for_new_admin_id)
async def new_admin_id_handler(message: types.Message, state: FSMContext):
    """Обработка ID нового админа"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    if message.text.isdigit():
        user_id = int(message.text)
        
        # Проверяем, не пытаемся ли добавить другого супер-админа
        if user_id in ADMIN_IDS:
            await message.answer("❌ Этот пользователь уже является главным администратором")
            await state.clear()
            return
        
        success = db.add_admin(user_id)
        if success:
            await message.answer(f"✅ Оператор `{user_id}` добавлен!", 
                               reply_markup=get_admin_keyboard(True), 
                               parse_mode="None")
        else:
            await message.answer(f"ℹ️ Пользователь `{user_id}` уже является оператором",
                               reply_markup=get_admin_keyboard(True),
                               parse_mode="None")
    else: 
        await message.answer("❌ ID должен быть числом.")
    
    await state.clear()

@dp.message(Form.waiting_for_broadcast_text)
async def broadcast_text_handler(message: types.Message, state: FSMContext):
    """Обработка рассылки"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    users = db.get_all_users_ids()
    count = 0
    
    await message.answer(f"📤 Начинаю рассылку для {len(users)} пользователей...")
    
    for u_id in users:
        try:
            await message.copy_to(u_id)
            count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logging.error(f"Ошибка отправки пользователю {u_id}: {e}")
    
    await state.clear()
    await message.answer(f"✅ Рассылка завершена!\n\n📊 **Статистика:**\n• Отправлено: {count} пользователей\n• Не отправлено: {len(users) - count}")

# ============================================
# ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ДЛЯ РЕДАКТИРОВАНИЯ ТАРИФОВ
# ============================================

@dp.callback_query(F.data == "adm_pri_name")
async def adm_pri_name_handler(callback: CallbackQuery, state: FSMContext):
    """Изменение названия приоритета"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_priority_name)
    await callback.message.edit_text("Введите новое название для приоритета:", 
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_edit_priority")]
                                     ]))

@dp.message(Form.waiting_for_priority_name)
async def process_pri_name(message: types.Message, state: FSMContext):
    """Обработка нового названия приоритета"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    db.set_priority_name(message.text.strip())
    await state.clear()
    await message.answer(f"✅ Название изменено на: **{message.text.strip()}**", parse_mode="None")
    await admin_cmd(message)

@dp.callback_query(F.data == "adm_pri_price")
async def adm_pri_price_handler(callback: CallbackQuery, state: FSMContext):
    """Изменение цены приоритета"""
    if callback.from_user.id not in ADMIN_IDS: 
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_priority_price)
    await callback.message.edit_text("Введите новую сумму наценки:", 
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_edit_priority")]
                                     ]))

@dp.message(Form.waiting_for_priority_price)
async def process_pri_price(message: types.Message, state: FSMContext):
    """Обработка новой цены приоритета"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    try:
        new_price = float(message.text.replace(',', '.'))
        db.set_priority_price(new_price)
        await state.clear()
        await message.answer(f"✅ Наценка обновлена: **${new_price}**", parse_mode="None")
        await admin_cmd(message)
    except ValueError:
        await message.answer("❌ Введите число!")

# ============================================
# ОБРАБОТЧИКИ ДЛЯ ФЕЙКОВОЙ ОЧЕРЕДИ
# ============================================

@dp.callback_query(F.data == "fake_queue_add")
async def fake_queue_add_handler(callback: CallbackQuery, state: FSMContext):
    """Добавить фейковые номера"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    await state.set_state(Form.waiting_for_fake_queue_count)
    await state.update_data(action="add")
    
    await callback.message.edit_text(
        "Введите сколько фейковых номеров ДОБАВИТЬ:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_fake_queue")]
        ])
    )

@dp.callback_query(F.data == "fake_queue_remove")
async def fake_queue_remove_handler(callback: CallbackQuery, state: FSMContext):
    """Убрать фейковые номера"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    await state.set_state(Form.waiting_for_fake_queue_count)
    await state.update_data(action="remove")
    
    await callback.message.edit_text(
        "Введите сколько фейковых номеров УБРАТЬ:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_fake_queue")]
        ])
    )

@dp.callback_query(F.data == "fake_queue_set")
async def fake_queue_set_handler(callback: CallbackQuery, state: FSMContext):
    """Установить точное число фейковой очереди"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    await state.set_state(Form.waiting_for_fake_queue_count)
    await state.update_data(action="set")
    
    await callback.message.edit_text(
        "Введите точное количество фейковой очереди:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_fake_queue")]
        ])
    )

@dp.callback_query(F.data == "fake_queue_reset")
async def fake_queue_reset_handler(callback: CallbackQuery):
    """Сбросить фейковую очередь"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    db.set_fake_queue(0)
    await callback.answer("✅ Фейковая очередь сброшена к 0", show_alert=True)
    await admin_fake_queue_handler(callback)

@dp.message(Form.waiting_for_fake_queue_count)
async def process_fake_queue_count(message: types.Message, state: FSMContext):
    """Обработка ввода количества для фейковой очереди"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    data = await state.get_data()
    action = data.get('action', 'add')
    
    if not message.text.isdigit():
        await message.answer("❌ Введите целое число!")
        return
    
    count = int(message.text)
    current_fake = db.get_fake_queue()
    
    if action == "add":
        new_value = current_fake + count
        db.set_fake_queue(new_value)
        await message.answer(f"✅ Добавлено {count} фейковых номеров. Теперь: {new_value}")
    
    elif action == "remove":
        new_value = max(0, current_fake - count)
        db.set_fake_queue(new_value)
        await message.answer(f"✅ Убрано {count} фейковых номеров. Теперь: {new_value}")
    
    elif action == "set":
        db.set_fake_queue(max(0, count))
        await message.answer(f"✅ Фейковая очередь установлена: {max(0, count)}")
    
    await state.clear()
    await admin_fake_queue_handler(message)

@dp.callback_query(F.data == "fake_queue_decrease_auto")
async def fake_queue_decrease_auto_handler(callback: CallbackQuery):
    """Ручное уменьшение фейковой очереди по алгоритму (упрощенное)"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    decreased, amount, new_fake = decrease_fake_queue_gradually()
    
    if decreased:
        # Только краткое уведомление без деталей
        await callback.answer("✅ Фейковая очередь уменьшена", show_alert=True)
    else:
        await callback.answer("ℹ️ Фейковая очередь уже равна 0", show_alert=True)
    
    await admin_fake_queue_handler(callback)

@dp.message(Command("decrease_queue"))
async def decrease_queue_cmd(message: types.Message):
    """Команда для ручного уменьшения фейковой очереди (упрощенная)"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return
    
    decreased, amount, new_fake = decrease_fake_queue_gradually()
    
    if decreased:
        # Простое сообщение без деталей
        await message.answer("✅ Фейковая очередь уменьшена", parse_mode="None")
    else:
        await message.answer("ℹ️ Фейковая очередь уже равна 0", parse_mode="None")

# ============================================
# ОБРАБОТЧИКИ ДЛЯ СИСТЕМНОГО СООБЩЕНИЯ
# ============================================

@dp.callback_query(F.data == "set_system_message")
async def set_system_message_handler(callback: CallbackQuery, state: FSMContext):
    """Установить системное сообщение"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    await state.set_state(Form.waiting_for_system_message)
    await callback.message.edit_text(
        "📝 Введите системное сообщение (будет отображаться всем пользователям):\n\n"
        "Можно использовать разметку Markdown для форматирования.\n"
        "Примеры:\n"
        "• **Жирный текст**\n"
        "• *Курсив*\n"
        "• [Ссылка](https://example.com)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_modes")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_system_message)
async def process_system_message(message: types.Message, state: FSMContext):
    """Обработать системное сообщение"""
    if message.from_user.id not in ADMIN_IDS: 
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    db.set_system_message(message.text)
    await state.clear()
    await message.answer("✅ Системное сообщение установлено!", parse_mode="None")
    await admin_modes_handler(message)

@dp.callback_query(F.data == "clear_system_message")
async def clear_system_message_handler(callback: CallbackQuery):
    """Очистить системное сообщение"""
    if callback.from_user.id not in ADMIN_IDS: 
        return
    
    db.set_system_message("")
    await callback.answer("✅ Системное сообщение очищено", show_alert=True)
    await admin_modes_handler(callback)

# ============================================
# УПРАВЛЕНИЕ БАЛАНСАМИ ПОЛЬЗОВАТЕЛЕЙ
# ============================================

@dp.callback_query(F.data == "admin_balance_menu")
async def admin_balance_menu_handler(callback: CallbackQuery):
    """Меню управления балансами"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    text = "💰 **Управление балансами пользователей**\n\nВыберите действие:"
    
    buttons = [
        [InlineKeyboardButton(text="👤 Изменить баланс пользователя", callback_data="admin_manage_user_balance")],
        [InlineKeyboardButton(text="📊 Общая статистика балансов", callback_data="admin_balance_stats")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "admin_manage_user_balance")
async def admin_manage_user_balance_handler(callback: CallbackQuery, state: FSMContext):
    """Начало управления балансом пользователя"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_user_id_to_manage)
    
    await callback.message.edit_text(
        "👤 **Управление балансом пользователя**\n\n"
        "Введите Telegram ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_balance_menu")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_user_id_to_manage)
async def process_user_id_to_manage(message: types.Message, state: FSMContext):
    """Обработка ID пользователя для управления балансом"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    if not message.text.isdigit():
        await message.answer("❌ ID должен быть числом!")
        return
    
    user_id = int(message.text)
    
    # Получаем информацию о пользователе
    user_info = db.get_user_info(user_id)
    
    if not user_info:
        await message.answer(f"❌ Пользователь с ID {user_id} не найден.")
        await state.clear()
        return
    
    user_id, username, balance, total_numbers, is_banned, priority = user_info
    
    await state.update_data(target_user_id=user_id, current_balance=balance)
    
    safe_username = escape_markdown(username or f"ID{user_id}")
    text = (f"👤 **Пользователь:** @{safe_username}\n"
            f"🆔 **ID:** `{user_id}`\n"
            f"💰 **Текущий баланс:** ${balance:.2f}\n\n"
            f"Выберите действие с балансом:")
    
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить средства", callback_data="balance_add")],
        [InlineKeyboardButton(text="➖ Снять средства", callback_data="balance_subtract")],
        [InlineKeyboardButton(text="⚡ Установить баланс", callback_data="balance_set")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_balance_menu")]
    ]
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "balance_add")
async def balance_add_handler(callback: CallbackQuery, state: FSMContext):
    """Добавление средств пользователю"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_balance_change_amount)
    await state.update_data(operation="add")
    
    data = await state.get_data()
    current_balance = data.get('current_balance', 0)
    
    await callback.message.edit_text(
        f"➕ **Добавление средств**\n\n"
        f"💰 Текущий баланс: ${current_balance:.2f}\n\n"
        f"Введите сумму для добавления ($):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_balance_menu")]
        ]),
        parse_mode="None"
    )

@dp.callback_query(F.data == "balance_subtract")
async def balance_subtract_handler(callback: CallbackQuery, state: FSMContext):
    """Снятие средств у пользователя"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_balance_change_amount)
    await state.update_data(operation="subtract")
    
    data = await state.get_data()
    current_balance = data.get('current_balance', 0)
    
    await callback.message.edit_text(
        f"➖ **Снятие средств**\n\n"
        f"💰 Текущий баланс: ${current_balance:.2f}\n\n"
        f"Введите сумму для снятия ($):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_balance_menu")]
        ]),
        parse_mode="None"
    )

@dp.callback_query(F.data == "balance_set")
async def balance_set_handler(callback: CallbackQuery, state: FSMContext):
    """Установка баланса пользователя"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    await state.set_state(Form.waiting_for_balance_change_amount)
    await state.update_data(operation="set")
    
    data = await state.get_data()
    current_balance = data.get('current_balance', 0)
    
    await callback.message.edit_text(
        f"⚡ **Установка баланса**\n\n"
        f"💰 Текущий баланс: ${current_balance:.2f}\n\n"
        f"Введите новое значение баланса ($):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_balance_menu")]
        ]),
        parse_mode="None"
    )

@dp.message(Form.waiting_for_balance_change_amount)
async def process_balance_change_amount(message: types.Message, state: FSMContext):
    """Обработка изменения баланса"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    try:
        amount = float(message.text.replace(',', '.'))
        if amount < 0:
            await message.answer("❌ Сумма не может быть отрицательной!")
            return
        
        data = await state.get_data()
        user_id = data['target_user_id']
        operation = data['operation']
        current_balance = data.get('current_balance', 0)
        
        # Получаем информацию о пользователе для уведомления
        user_info = db.get_user_info(user_id)
        if not user_info:
            await message.answer("❌ Пользователь не найден")
            await state.clear()
            return
        
        _, username, old_balance, _, _, _ = user_info
        
        # Выполняем операцию
        if operation == "add":
            new_balance = db.update_user_balance(user_id, amount, "add")
            operation_text = "добавлено"
        elif operation == "subtract":
            # Проверяем, достаточно ли средств
            if amount > old_balance:
                await message.answer(f"❌ Недостаточно средств. Текущий баланс: ${old_balance:.2f}")
                return
            new_balance = db.update_user_balance(user_id, amount, "subtract")
            operation_text = "снято"
        else:  # set
            new_balance = db.update_user_balance(user_id, amount, "set")
            operation_text = "установлен"
        
        safe_username = escape_markdown(username or f"ID{user_id}")
        
        # Сообщение админу
        await message.answer(
            f"✅ **Баланс обновлен!**\n\n"
            f"👤 **Пользователь:** @{safe_username}\n"
            f"🆔 **ID:** `{user_id}`\n"
            f"💰 **Старый баланс:** ${old_balance:.2f}\n"
            f"💰 **Новый баланс:** ${new_balance:.2f}\n"
            f"📊 **Изменение:** {operation_text} ${amount:.2f}",
            parse_mode="None"
        )
        
        # Уведомление пользователя
        try:
            await bot.send_message(
                user_id,
                f"💰 **Ваш баланс был изменен администратором!**\n\n"
                f"📊 **Операция:** {operation_text.upper()}\n"
                f"💵 **Сумма:** ${amount:.2f}\n"
                f"💰 **Старый баланс:** ${old_balance:.2f}\n"
                f"💰 **Новый баланс:** ${new_balance:.2f}",
                parse_mode="None"
            )
        except:
            pass
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите число! Например: 50 или 25.5")

@dp.callback_query(F.data == "admin_balance_stats")
async def admin_balance_stats_handler(callback: CallbackQuery):
    """Статистика по балансам"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    users = db.get_all_users_with_stats()
    total_users = db.get_total_users_count()
    
    if not users:
        text = "📊 **Нет данных о пользователях**"
    else:
        # Статистика
        total_balance = sum(user[2] for user in users)
        avg_balance = total_balance / total_users if total_users > 0 else 0
        
        # Топ пользователей по балансу
        top_users = sorted(users, key=lambda x: x[2], reverse=True)[:5]
        
        text = f"📊 **Статистика по балансам**\n\n"
        text += f"👥 **Всего пользователей:** {total_users}\n"
        text += f"💰 **Общая сумма балансов:** ${total_balance:.2f}\n"
        text += f"📈 **Средний баланс:** ${avg_balance:.2f}\n\n"
        
        text += f"🏆 **Топ-5 по балансу:**\n"
        for i, user in enumerate(top_users, 1):
            user_id, username, balance, total_numbers, is_banned, priority = user
            status = "🚫" if is_banned else "✅"
            safe_username = escape_markdown(username or f"ID{user_id}")
            text += f"{i}. {status} @{safe_username} - ${balance:.2f}\n"
    
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_balance_menu")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

# ============================================
# СПИСОК ПОЛЬЗОВАТЕЛЕЙ
# ============================================

@dp.callback_query(F.data == "admin_users_list")
async def admin_users_list_handler(callback: CallbackQuery):
    """Список пользователей"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    users = db.get_all_users_with_stats()
    total_users = db.get_total_users_count()
    total_balance = sum(user[2] for user in users)
    
    text = f"👥 **Список пользователей** (всего: {total_users})\n\n"
    
    for i, user in enumerate(users[:10], 1):  # Показываем первые 10
        user_id, username, balance, total_numbers, is_banned, priority = user
        status = "🚫" if is_banned else "✅"
        admin_status = "👑" if priority >= 1 else ""
        
        safe_username = escape_markdown(username or f"ID{user_id}")
        text += f"{i}. {status}{admin_status} @{safe_username}\n"
        text += f"   ID: `{user_id}` | Баланс: ${balance:.2f} | Номеров: {total_numbers}\n\n"
    
    if total_users > 10:
        text += f"📋 ... и еще {total_users - 10} пользователей\n\n"
    
    text += f"💰 **Общая сумма балансов:** ${total_balance:.2f}"
    
    buttons = [
        [InlineKeyboardButton(text="📥 Скачать отчет (TXT)", callback_data="download_users_report")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

@dp.callback_query(F.data == "download_users_report")
async def download_users_report_handler(callback: CallbackQuery):
    """Скачать отчет по пользователям"""
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только для главного админа", show_alert=True)
        return
    
    users = db.get_all_users_with_stats()
    
    if not users:
        await callback.answer("📭 Нет данных для отчета", show_alert=True)
        return
    
    # Создаем файл
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"users_report_{timestamp}.txt"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("ОТЧЕТ ПО ПОЛЬЗОВАТЕЛЯМ\n")
        f.write(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Всего пользователей: {len(users)}\n")
        f.write("=" * 80 + "\n\n")
        
        # Статистика
        total_balance = sum(user[2] for user in users)
        banned_users = len([user for user in users if user[4] == 1])
        admin_users = len([user for user in users if user[5] >= 1])
        
        f.write("СТАТИСТИКА:\n")
        f.write(f"  Всего пользователей: {len(users)}\n")
        f.write(f"  Забанено: {banned_users}\n")
        f.write(f"  Операторов: {admin_users}\n")
        f.write(f"  Общий баланс: ${total_balance:.2f}\n")
        f.write("-" * 80 + "\n\n")
        
        # Детали по пользователям
        f.write("ДЕТАЛИ ПОЛЬЗОВАТЕЛЕЙ:\n\n")
        f.write(f"{'№':<4} {'Статус':<8} {'ID':<12} {'Имя':<25} {'Баланс':<12} {'Номеров':<10}\n")
        f.write("-" * 80 + "\n")
        
        for i, user in enumerate(users, 1):
            user_id, username, balance, total_numbers, is_banned, priority = user
            
            status = "БАН" if is_banned else "АКТИВ"
            if priority >= 1:
                status = "ОПЕР"
            
            username_display = username or f"ID{user_id}"
            if len(username_display) > 20:
                username_display = username_display[:17] + "..."
            
            f.write(f"{i:<4} {status:<8} {user_id:<12} @{username_display:<24} ${balance:<11.2f} {total_numbers:<10}\n")
    
    try:
        # Отправляем файл
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📊 **Отчет по пользователям**\n\n📅 Дата экспорта: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n👥 Пользователей: {len(users)}",
            parse_mode="None"
        )
        await callback.answer("✅ Файл отправлен")
    except Exception as e:
        await callback.answer(f"❌ Ошибка отправки файла: {e}", show_alert=True)
    finally:
        # Удаляем временный файл
        if os.path.exists(filename):
            os.remove(filename)

# ============================================
# ЗАПУСК БОТА
# ============================================

async def check_and_clean_locks():
    """Периодическая очистка истекших блокировок"""
    while True:
        try:
            db.clear_expired_locks()
        except:
            pass
        await asyncio.sleep(60)  # Проверяем каждую минуту

def init_system():
    """Инициализация системы при запуске"""
    # Разблокируем все номера при запуске (на случай перезапуска бота)
    db.cursor.execute("DELETE FROM number_locks")
    db.connection.commit()

async def main():
    """Основная функция запуска бота"""
    # Инициализация системы
    init_system()
    
    # Запускаем очистку блокировок в фоне
    asyncio.create_task(check_and_clean_locks())
    
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":

    asyncio.run(main())




