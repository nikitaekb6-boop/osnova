import asyncio
import logging
import csv
import os
import re
import sqlite3
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
            
            # Проверяем, есть ли успешный номер у пользователя (ОТСТОЯЛ)
            successful_number = self.cursor.execute("""
                SELECT COUNT(*) FROM numbers 
                WHERE user_id = ? AND status = 'ОТСТОЯЛ'
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
        bot_username = "Magic_team_work_bot"  # Нужно будет заменить
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
        
        if for_admin:
            # Для админа показываем реальное время с надбавкой
            bonus = self.get_hidden_time_bonus(tariff_id)
            return standard_duration + bonus, bonus
        else:
            # Для пользователей показываем только стандартное время
            return standard_duration, 0

    def set_number_slet(self, number_id, is_admin=False):
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
            
            # РЕАЛЬНОЕ время для проверки
            real_duration = standard_dur + hidden_bonus
            
            diff_seconds = (datetime.now() - start_time).total_seconds()
            minutes = int(diff_seconds // 60)
            seconds = int(diff_seconds % 60)
            
            time_str = f"{minutes}м {seconds}с"
            
            # Определяем статус
            if is_admin:
                # Админ видит реальное время
                if minutes >= real_duration:
                    final_status = "ОТСТОЯЛ"
                else:
                    final_status = "СЛЕТ"
            else:
                # Пользователю показываем по стандартному времени
                if minutes >= standard_dur:
                    final_status = "ОТСТОЯЛ"
                else:
                    final_status = "СЛЕТ"
            
            # РЕАЛЬНЫЙ статус для внутренней логики
            real_status = "ОТСТОЯЛ" if minutes >= real_duration else "СЛЕТ"
            
            finish_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Сохраняем статус для пользователя (который он видит)
            self.cursor.execute(
                "UPDATE numbers SET status = ?, finished_at = ? WHERE id = ?", 
                (final_status, finish_now, number_id)
            )
            
            # ПРОВЕРКА НА РЕФЕРАЛЬНЫЙ БОНУС - используем РЕАЛЬНЫЙ статус
            if real_status == "ОТСТОЯЛ":
                user_id = res[1]
                referral_result = self.check_and_award_referral_bonus(user_id)
                # Возвращаем также информацию о реферальном бонусе
                return {
                    "user_id": res[1], 
                    "status": final_status,  # Статус для отображения
                    "real_status": real_status,  # Реальный статус
                    "referral_bonus": referral_result,
                    "hidden_bonus": hidden_bonus,
                    "minutes_passed": minutes
                }
            
            return {
                "user_id": res[1], 
                "status": final_status,
                "real_status": real_status,
                "referral_bonus": None,
                "hidden_bonus": hidden_bonus,
                "minutes_passed": minutes
            }

    # В методе set_number_slet без флага админа (для совместимости)
    def set_number_slet_old(self, number_id):
        """Старый метод set_number_slet без учета скрытой надбавки"""
        with self.connection:
            res = self.cursor.execute("""
                SELECT n.started_at, n.user_id, t.duration_min 
                FROM numbers n 
                JOIN tariffs t ON n.tariff_id = t.id 
                WHERE n.id = ?
            """, (number_id,)).fetchone()
            
            if not res or not res[0]: return None
            
            start_time = datetime.strptime(str(res[0]).split('.')[0], '%Y-%m-%d %H:%M:%S')
            target_dur = res[2]
            diff_seconds = (datetime.now() - start_time).total_seconds()
            minutes = int(diff_seconds // 60)
            seconds = int(diff_seconds % 60)
            
            time_str = f"{minutes}м {seconds}с"
            # Убираем время из статуса
            final_status = "ОТСТОЯЛ" if minutes >= target_dur else "СЛЕТ"
            finish_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute("UPDATE numbers SET status = ?, finished_at = ? WHERE id = ?", (final_status, finish_now, number_id))
            
            # ПРОВЕРКА НА РЕФЕРАЛЬНЫЙ БОНУС
            if final_status == "ОТСТОЯЛ":
                user_id = res[1]
                referral_result = self.check_and_award_referral_bonus(user_id)
                # Возвращаем также информацию о реферальном бонусе
                return {"user_id": res[1], "status": final_status, "referral_bonus": referral_result}
            
            return {"user_id": res[1], "status": final_status, "referral_bonus": None}

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
    
    def has_user_repeated_number(self, user_id, phone):
        """Проверить, сдавал ли пользователь этот номер ранее"""
        with self.connection:
            result = self.cursor.execute(
                "SELECT COUNT(*) FROM numbers WHERE user_id = ? AND phone = ?",
                (user_id, phone)
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
        """Добавить номер с проверкой на повторение номера"""
        with self.connection:
            # Проверяем, не сдавал ли пользователь этот номер ранее (в ЛЮБОМ статусе)
            if self.has_user_repeated_number(user_id, phone):
                return False, "❌ Вы уже сдавали этот номер ранее. Пожалуйста, используйте другой номер."
            
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
        result = self.cursor.execute("""
            SELECT n.id, n.phone, n.user_id, u.username, n.is_priority 
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id 
            WHERE n.status = 'Ожидание' 
            ORDER BY n.is_priority DESC, n.created_at ASC LIMIT 1
        """).fetchone()
        
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
            SELECT n.phone, n.status, t.name  # Убрали n.finished_at из SELECT
            FROM numbers n
            LEFT JOIN tariffs t ON n.tariff_id = t.id
            WHERE n.user_id = ? AND (n.status = 'ОТСТОЯЛ' OR n.status = 'СЛЕТ')
            ORDER BY n.id DESC LIMIT 15
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

    def get_user_position(self, user_id):
        target = self.cursor.execute("""
            SELECT created_at, is_priority 
            FROM numbers 
            WHERE user_id = ? AND status = 'Ожидание' 
            ORDER BY is_priority DESC, created_at ASC LIMIT 1
        """, (user_id,)).fetchone()
        
        if not target: return None
            
        t_created, t_priority = target

        if t_priority == 1:
            res = self.cursor.execute("""
                SELECT COUNT(*) FROM numbers 
                WHERE status = 'Ожидание' AND is_priority = 1 AND created_at < ?
            """, (t_created,)).fetchone()
        else:
            res = self.cursor.execute("""
                SELECT COUNT(*) FROM numbers 
                WHERE status = 'Ожидание' AND (is_priority = 1 OR (is_priority = 0 AND created_at < ?))
            """, (t_created,)).fetchone()
        
        fake_count = self.get_fake_queue()
        
        if t_priority == 0:
            return res[0] + 1 + fake_count if res else 1 + fake_count
        else:
            return res[0] + 1 if res else 1

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

# --- КОНФИГУРАЦИЯ БОТА ---

TOKEN = "8168150477:AAGX0s9L3KTIBB0X-wuFke7AIVUPcXaBigU"
ADMIN_IDS = [8260066747] 

bot = Bot(token=TOKEN)
dp = Dispatcher()
db = Database("bot_database.db")

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
            [InlineKeyboardButton(text="📞 Техподдержка", url="https://t.me/magic_work_official")]
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
        [InlineKeyboardButton(text="📞 Техподдержка", url="https://t.me/magic_work_official")]
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
    """Команда /archive - показать архив номеров БЕЗ ВРЕМЕНИ"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    data = db.get_user_archive(message.from_user.id)
    
    if not data:
        text = "📂 **Архив пуст**\n\nУ вас пока нет завершенных номеров."
    else:
        text = "📂 **История номеров** (последние 15):\n\n"
        for i in data:
            emo = "✅" if i[1] == "ОТСТОЯЛ" else "❌"
            # Теперь используем только i[0] (телефон), i[1] (статус), i[2] (название тарифа)
            text += f"{emo} `{i[0]}` | {i[2]} | {i[1]}\n"  # Убрано время
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    await message.answer(text, reply_markup=kb, parse_mode="None")

@dp.callback_query(F.data == "archive")
async def archive_button_handler(callback: CallbackQuery):
    """Обработчик кнопки архива БЕЗ ВРЕМЕНИ"""
    if db.is_user_banned(callback.from_user.id): 
        return
    
    data = db.get_user_archive(callback.from_user.id)
    
    if not data:
        text = "📂 **Архив пуст**\n\nУ вас пока нет завершенных номеров."
    else:
        text = "📂 **История номеров** (последние 15):\n\n"
        for i in data:
            emo = "✅" if i[1] == "ОТСТОЯЛ" else "❌"
            # Теперь используем только i[0] (телефон), i[1] (статус), i[2] (название тарифа)
            text += f"{emo} `{i[0]}` | {i[2]} | {i[1]}\n"  # Убрано время
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")

@dp.message(Command("queue"))
async def queue_cmd(message: types.Message):
    """Команда /queue - показать текущую очередь"""
    if db.is_user_banned(message.from_user.id): 
        return
    
    user_id = message.from_user.id
    total_count = db.get_queue_count()
    user_pos = db.get_user_position(user_id)
    user_numbers_count = db.cursor.execute("SELECT COUNT(*) FROM numbers WHERE user_id = ? AND status = 'Ожидание'", (user_id,)).fetchone()[0]
    
    text = f"📊 **Текущая очередь**\n\n"
    text += f"🔢 **Всего номеров в ожидании:** {total_count}\n\n"
    
    if user_numbers_count > 0:
        text += f"👤 **Ваших номеров в очереди:** {user_numbers_count}\n"
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
        "📞 **Техподдержка:** @magic_work_official\n\n"
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
    text += "• Нельзя сдавать один и тот же номер повторно\n"
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
    text += "• Нельзя сдавать один и тот же номер повторно\n"
    text += "• Приоритетные номера (⭐) обрабатываются в первую очередь"
    
    buttons = [
        [InlineKeyboardButton(text="📱 Сдать еще номер", callback_data="give_number")],
        [InlineKeyboardButton(text="📊 Проверить очередь", callback_data="queue")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile")]
    ]
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="None")

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
    text += f"• Можно сдавать несколько разных номеров\n"
    text += f"• Нельзя сдавать один и тот же номер повторно"
    
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
    await callback.message.edit_text(
        "⚙️ **Админ панель**\n\nВыберите действие:",
        reply_markup=get_admin_keyboard(is_super_admin),
        parse_mode="None"
    )

# ============================================
# КОМАНДЫ ДЛЯ АДМИНИСТРАТОРОВ
# ============================================

@dp.message(Command("number"))
async def number_cmd(message: types.Message):
    """Команда /number - взять следующий номер из очереди"""
    user_id = message.from_user.id
    
    # Проверяем права: супер-админ или оператор
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return

    number = db.get_next_number_from_queue()
    if not number:
        await message.answer("📭 **Очередь пуста.**", parse_mode="None")
        return

    n_id, phone, u_id, username, is_prio = number
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
    
    await message.answer(text, reply_markup=kb, parse_mode="None")

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
    """Кнопка взять номер в админ-панели"""
    user_id = callback.from_user.id
    
    # Проверяем права: супер-админ или оператор
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return

    number = db.get_next_number_from_queue()
    if not number:
        await callback.answer("📭 Очередь пуста", show_alert=True)
        return

    n_id, phone, u_id, username, is_prio = number
    
    # Проверяем, не взят ли уже номер другим оператором
    current_status = db.cursor.execute(
        "SELECT status FROM numbers WHERE id = ?", 
        (n_id,)
    ).fetchone()
    
    if current_status and current_status[0] != 'Ожидание':
        await callback.answer("⚠️ Этот номер уже взят другим оператором!", show_alert=True)
        # Обновляем список номеров
        await admin_take_fast_handler(callback)
        return
    
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
    
    # Отправляем новое сообщение с номером
    await callback.message.answer(text, reply_markup=kb, parse_mode="None")
    await callback.answer()

@dp.callback_query(F.data == "admin_base")
async def admin_base_handler(callback: CallbackQuery):
    """Кнопка базы номеров в админ-панели"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    nums = db.get_all_numbers_limit(10)
    text = "📂 **Последние 10 номеров:**\n\n"
    for n in nums:
        safe_phone = escape_markdown(n[0])
        safe_username = escape_markdown(n[1] or '—')
        text += f"📞 `{safe_phone}` | 👤 @{safe_username} | 📊 {n[2]} | 📦 {n[3]}\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать полную базу (TXT)", callback_data="csv")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel_back")]
    ])
    
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="None")

@dp.callback_query(F.data == "csv")
async def csv_handler(callback: CallbackQuery):
    """Скачать базу номеров"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    data = db.get_all_numbers_raw()
    path = "base.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write("ID | Номер | Пользователь | Статус | Тариф | Создан | Завершен\n" + "-"*50 + "\n")
        for row in data: 
            f.write(" | ".join(map(str, row)) + "\n")
    
    try:
        await callback.message.answer_document(FSInputFile(path), caption="📂 База номеров (TXT)")
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
    
    if os.path.exists(path): 
        os.remove(path)

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
    """Номер взят в работу"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    n_id = callback.data.split("_")[1]
    
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
        
        new_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)\n\n🟡 **СТАТУС: В РАБОТЕ**"
        
        # Обновляем клавиатуру
        new_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏁 Завершить", callback_data=f"slet_{n_id}")],
            [InlineKeyboardButton(text="💬 Ответить", callback_data=f"reply_{n_id}"),
             InlineKeyboardButton(text="⏭ Ошибка", callback_data=f"err_{n_id}")]
        ])
        
        await callback.message.edit_text(new_text, reply_markup=new_kb, parse_mode="None")
    else:
        await callback.answer("❌ Ошибка при взятии номера", show_alert=True)

@dp.callback_query(F.data.startswith("slet_"))
async def slet_handler(callback: CallbackQuery):
    """Завершение работы с номером с проверкой скрытой надбавки"""
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await callback.answer("❌ У вас нет прав доступа", show_alert=True)
        return
    
    n_id = callback.data.split("_")[1]
    
    # Проверяем, может ли этот оператор завершить этот номер
    # Получаем текущий статус номера
    current_status = db.cursor.execute(
        "SELECT status FROM numbers WHERE id = ?", 
        (n_id,)
    ).fetchone()
    
    # Если номер не "В работе", значит его уже кто-то завершил или он не был взят
    if not current_status or current_status[0] != 'В работе':
        await callback.answer("⚠️ Этот номер уже обработан или не был взят вами!", show_alert=True)
        return
    
    # Определяем, является ли пользователь главным админом
    is_super_admin = user_id in ADMIN_IDS
    
    # Передаем флаг админа в метод set_number_slet
    res = db.set_number_slet(n_id, is_admin=is_super_admin)
    
    if res:
        # Проверяем, был ли начислен реферальный бонус
        if res.get('referral_bonus'):
            bonus_info = res['referral_bonus']
            try:
                await bot.send_message(
                    bonus_info['referrer_id'],
                    f"🎉 **Вы получили реферальный бонус!**\n\n"
                    f"💰 **Сумма:** ${bonus_info['bonus']:.2f}\n"
                    f"👤 **От реферала:** ID {res['user_id']}\n\n"
                    f"💵 Бонус добавлен на ваш баланс!\n"
                    f"📊 Продолжайте приглашать друзей для получения новых бонусов!",
                    parse_mode="None"
                )
            except:
                pass
        
        # Отправляем уведомление пользователю
        try: 
            await bot.send_message(res['user_id'], f"🏁 Завершено: **{res['status']}**", parse_mode="None")
        except: 
            pass
        
        # Получаем информацию о номере для обновления сообщения
        number_info = db.cursor.execute("""
            SELECT n.phone, n.user_id, u.username, n.is_priority, n.tariff_id 
            FROM numbers n 
            LEFT JOIN users u ON n.user_id = u.user_id 
            WHERE n.id = ?
        """, (n_id,)).fetchone()
        
        if number_info:
            phone, u_id, username, is_prio, tariff_id = number_info
            _, p_name = db.get_priority_settings()
            prio_label = f"⭐ [{p_name}] " if is_prio else ""
            
            safe_phone = escape_markdown(phone)
            safe_username = escape_markdown(username or 'User')
            
            # Для главного админа показываем дополнительную информацию
            if user_id in ADMIN_IDS:
                tariff_info = db.cursor.execute(
                    "SELECT name, duration_min FROM tariffs WHERE id = ?", 
                    (tariff_id,)
                ).fetchone()
                
                tariff_name = tariff_info[0] if tariff_info else "Неизвестно"
                standard_duration = tariff_info[1] if tariff_info else 0
                hidden_bonus = db.get_hidden_time_bonus(tariff_id)
                real_duration = standard_duration + hidden_bonus
                
                status_emoji = "✅" if res['real_status'] == "ОТСТОЯЛ" else "❌"
                new_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)\n\n"
                new_text += f"📊 **Тариф:** {tariff_name}\n"
                new_text += f"⏱ **Стандартное время:** {standard_duration} мин\n"
                new_text += f"➕ **Скрытая надбавка:** {hidden_bonus} мин\n"
                new_text += f"⏳ **Реальное время:** {real_duration} мин\n"
                new_text += f"⏰ **Прошло:** {res['minutes_passed']} мин\n\n"
                new_text += f"{status_emoji} **{res['status']}** (для пользователя)\n"
                new_text += f"🔒 **Реальный статус:** {res['real_status']}"
            else:
                # Для обычного оператора показываем обычный текст
                status_emoji = "✅" if "ОТСТОЯЛ" in res['status'] else "❌"
                new_text = f"{prio_label}📱 **Номер:** `{safe_phone}`\n👤 От: @{safe_username} (ID: `{u_id}`)\n\n{status_emoji} **{res['status']}**"
            
            await callback.message.edit_text(new_text, parse_mode="None")
    else: 
        await callback.answer("❌ Ошибка при завершении", show_alert=True)

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
    
    res = db.cursor.execute("SELECT user_id, phone FROM numbers WHERE id = ?", (callback.data.split("_")[1],)).fetchone()
    if res:
        await state.update_data(reply_to_user_id=res[0], reply_to_phone=res[1])
        await state.set_state(Form.waiting_for_reply_text)
        await callback.message.answer(f"💬 **Ответ по номеру {res[1]}:**", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel_back")]
        ]))
    await callback.answer()

@dp.message(Form.waiting_for_reply_text)
async def reply_send_handler(message: types.Message, state: FSMContext):
    """Отправить ответ пользователю"""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS and not db.is_admin(user_id):
        await message.answer("❌ У вас нет прав доступа")
        await state.clear()
        return
    
    data = await state.get_data()
    try:
        safe_phone = escape_markdown(data['reply_to_phone'])
        await bot.send_message(data['reply_to_user_id'], f"🔔 **Ответ по номеру {safe_phone}:**", parse_mode="None")
        await message.copy_to(data['reply_to_user_id'])
        await message.answer("✅ Отправлено")
    except: 
        await message.answer("❌ Ошибка отправки")
    await state.clear()

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
                # Если не удалось добавить номер (повторный номер)
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

async def main():
    """Основная функция запуска бота"""
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":

    asyncio.run(main())
