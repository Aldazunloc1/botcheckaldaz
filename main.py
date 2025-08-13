import asyncio
import os
import html
import re
import logging
import json
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import httpx
from aiogram import Bot, Dispatcher
from aiogram.types import Message, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

@dataclass
class Config:
    bot_token: str
    imei_api_key: str
    imei_api_url: str = "https://alpha.imeicheck.com/api/php-api/create"
    request_timeout: int = 15
    max_retries: int = 3
    users_db_path: str = "users.json"
    owner_id: int = 7655366089
    # Configuración de AutoPing
    autopinger_enabled: bool = True
    autopinger_interval: int = 300  # 5 minutos en segundos
    autopinger_url: str = ""  # URL para hacer ping (opcional)

@dataclass
class UserData:
    user_id: int
    username: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    join_date: str
    last_activity: str
    total_queries: int = 0
    balance: float = 0.0
    query_history: List[Dict] = None

    def __post_init__(self):
        if self.query_history is None:
            self.query_history = []

def load_config() -> Config:
    bot_token = os.getenv("BOT_TOKEN")
    imei_api_key = os.getenv("API_KEY")
    
    if not bot_token or not imei_api_key:
        raise RuntimeError("Faltan variables BOT_TOKEN o IMEI_CHECKER_API_KEY en .env")
    
    # Configuración de AutoPinger desde variables de entorno
    autopinger_enabled = os.getenv("AUTOPINGER_ENABLED", "true").lower() == "true"
    autopinger_interval = int(os.getenv("AUTOPINGER_INTERVAL", "300"))  # 5 minutos por defecto
    autopinger_url = os.getenv("AUTOPINGER_URL", "")
    
    return Config(
        bot_token=bot_token, 
        imei_api_key=imei_api_key,
        autopinger_enabled=autopinger_enabled,
        autopinger_interval=autopinger_interval,
        autopinger_url=autopinger_url
    )

SERVICES_DATA = [
    # Apple Services
    {"id": 1, "title": "Find My iPhone (FMI) ON/OFF", "price": "0.02", "category": "Apple"},
    {"id": 2, "title": "Warranty + Activation - PRO", "price": "0.04", "category": "Apple"},
    {"id": 3, "title": "Apple FULL INFO [No Carrier]", "price": "0.14", "category": "Apple"},
    {"id": 4, "title": "iCloud Clean/Lost Check", "price": "0.04", "category": "Apple"},
    {"id": 7, "title": "Apple Carrier + SimLock - back-up", "price": "0.22", "category": "Apple"},
    {"id": 9, "title": "SOLD BY + GSX", "price": "3.38", "category": "Apple"},
    {"id": 12, "title": "GSX Next Tether + iOS (GSX Carrier)", "price": "1.20", "category": "Apple"},
    {"id": 13, "title": "Model + Color + Storage + FMI", "price": "0.04", "category": "Apple"},
    {"id": 18, "title": "iMac FMI Status On/Off", "price": "0.60", "category": "Apple"},
    {"id": 19, "title": "Apple FULL INFO [+Carrier] B", "price": "0.24", "category": "Apple"},
    {"id": 20, "title": "Apple SimLock Check", "price": "0.04", "category": "Apple"},
    {"id": 22, "title": "Apple BASIC INFO (PRO) - new", "price": "0.08", "category": "Apple"},
    {"id": 23, "title": "Apple Carrier Check (S2)", "price": "0.08", "category": "Apple"},
    {"id": 33, "title": "Replacement Status (Active Device)", "price": "0.02", "category": "Apple"},
    {"id": 34, "title": "Replaced Status (Original Device)", "price": "0.02", "category": "Apple"},
    {"id": 39, "title": "Apple FULL INFO [+Carrier] A", "price": "0.20", "category": "Apple"},
    {"id": 41, "title": "MDM Status ON/OFF", "price": "0.44", "category": "Apple"},
    {"id": 46, "title": "MDM Status ON/OFF + GSX Policy + FMI", "price": "0.90", "category": "Apple"},
    {"id": 47, "title": "Apple FULL + MDM + GSMA PRO", "price": "1.50", "category": "Apple"},
    {"id": 50, "title": "Apple SERIAL Info (model,size,color)", "price": "0.02", "category": "Apple"},
    {"id": 51, "title": "Warranty + Activation [SN ONLY]", "price": "0.02", "category": "Apple"},
    {"id": 52, "title": "Model Description (Any Apple SN/IMEI)", "price": "0.04", "category": "Apple"},
    {"id": 61, "title": "Apple Demo Unit Device Info", "price": "0.28", "category": "Apple"},
    
    # Android Services
    {"id": 8, "title": "Samsung Info (S1)", "price": "0.08", "category": "Android"},
    {"id": 17, "title": "Huawei IMEI Info", "price": "0.14", "category": "Android"},
    {"id": 21, "title": "Samsung INFO & KNOX STATUS (S2)", "price": "0.28", "category": "Android"},
    {"id": 25, "title": "XIAOMI MI LOCK & INFO", "price": "0.10", "category": "Android"},
    {"id": 27, "title": "OnePlus IMEI INFO", "price": "0.08", "category": "Android"},
    {"id": 36, "title": "Samsung Info (S1) + Blacklist", "price": "0.12", "category": "Android"},
    {"id": 37, "title": "Samsung Info & KNOX STATUS (S1)", "price": "0.18", "category": "Android"},
    {"id": 57, "title": "Google Pixel Info", "price": "0.24", "category": "Android"},
    {"id": 58, "title": "Honor Info", "price": "0.10", "category": "Android"},
    {"id": 59, "title": "Realme Info", "price": "0.06", "category": "Android"},
    {"id": 60, "title": "Oppo Info", "price": "0.06", "category": "Android"},
    {"id": 63, "title": "Motorola Info", "price": "0.10", "category": "Android"},
    
    # General Services
    {"id": 5, "title": "Blacklist Status (GSMA)", "price": "0.04", "category": "General"},
    {"id": 6, "title": "Blacklist Pro Check (GSMA)", "price": "0.16", "category": "General"},
    {"id": 10, "title": "IMEI to Model [all brands]", "price": "0.02", "category": "General"},
    {"id": 11, "title": "IMEI to Brand/Model/Name", "price": "0.02", "category": "General"},
    {"id": 14, "title": "IMEI to SN (Full Convertor)", "price": "0.04", "category": "General"},
    {"id": 15, "title": "T-mobile (ESN) PRO Check", "price": "0.08", "category": "General"},
    {"id": 16, "title": "Verizon (ESN) Clean/Lost Status", "price": "0.06", "category": "General"},
    {"id": 55, "title": "Blacklist Status - cheap", "price": "0.02", "category": "General"},
    {"id": 62, "title": "EID INFO (IMEI TO EID)", "price": "0.04", "category": "General"},
]

class IMEIStates(StatesGroup):
    waiting_for_service_category = State()
    waiting_for_service = State()
    waiting_for_imei = State()

class APIError(Exception):
    pass

class AutoPinger:
    """Sistema de AutoPing para mantener el bot activo"""
    
    def __init__(self, config: Config, bot: Bot):
        self.config = config
        self.bot = bot
        self.is_running = False
        self.ping_count = 0
        self.last_ping = None
        self.task = None
        
    async def start(self):
        """Iniciar el servicio de AutoPing"""
        if not self.config.autopinger_enabled:
            logger.info("🚫 AutoPinger deshabilitado")
            return
            
        if self.is_running:
            logger.warning("⚠️ AutoPinger ya está ejecutándose")
            return
            
        self.is_running = True
        self.task = asyncio.create_task(self._ping_loop())
        logger.info(f"🔄 AutoPinger iniciado - Intervalo: {self.config.autopinger_interval}s")
        
    async def stop(self):
        """Detener el servicio de AutoPing"""
        self.is_running = False
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 AutoPinger detenido")
        
    async def _ping_loop(self):
        """Bucle principal del AutoPing"""
        while self.is_running:
            try:
                await self._perform_ping()
                await asyncio.sleep(self.config.autopinger_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error en AutoPing: {e}")
                await asyncio.sleep(30)  # Esperar 30s antes de reintentar
                
    async def _perform_ping(self):
        """Realizar un ping"""
        try:
            self.ping_count += 1
            self.last_ping = datetime.now()
            
            # Método 1: Ping HTTP (si hay URL configurada)
            if self.config.autopinger_url:
                await self._http_ping()
            
            # Método 2: Ping a Telegram (getMe)
            await self._telegram_ping()
            
            logger.info(f"📡 AutoPing #{self.ping_count} - {self.last_ping.strftime('%H:%M:%S')}")
            
        except Exception as e:
            logger.error(f"❌ Error en ping #{self.ping_count}: {e}")
            
    async def _http_ping(self):
        """Ping HTTP a URL externa"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(self.config.autopinger_url)
                logger.debug(f"🌐 HTTP Ping: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ HTTP Ping falló: {e}")
            
    async def _telegram_ping(self):
        """Ping a la API de Telegram"""
        try:
            bot_info = await self.bot.get_me()
            logger.debug(f"🤖 Telegram Ping: @{bot_info.username}")
        except Exception as e:
            logger.warning(f"⚠️ Telegram Ping falló: {e}")
            
    def get_status(self) -> Dict[str, Any]:
        """Obtener estado del AutoPinger"""
        return {
            "enabled": self.config.autopinger_enabled,
            "running": self.is_running,
            "ping_count": self.ping_count,
            "last_ping": self.last_ping.isoformat() if self.last_ping else None,
            "interval": self.config.autopinger_interval,
            "url": self.config.autopinger_url or "No configurada"
        }

class UserDatabase:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.users: Dict[int, UserData] = {}
        self.load_users()

    def load_users(self):
        if self.db_path.exists():
            try:
                with open(self.db_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for user_id, user_data in data.items():
                        self.users[int(user_id)] = UserData(**user_data)
                logger.info(f"Cargados {len(self.users)} usuarios")
            except Exception as e:
                logger.error(f"Error cargando base de datos: {e}")
                self.users = {}

    def save_users(self):
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            data = {str(user_id): asdict(user_data) for user_id, user_data in self.users.items()}
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error guardando base de datos: {e}")

    def get_or_create_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None) -> UserData:
        if user_id not in self.users:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.users[user_id] = UserData(
                user_id=user_id, username=username, first_name=first_name,
                last_name=last_name, join_date=now, last_activity=now
            )
            self.save_users()
        else:
            user = self.users[user_id]
            user.username = username
            user.first_name = first_name
            user.last_name = last_name
            user.last_activity = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.users[user_id]

    def update_user_query(self, user_id: int, service_title: str, price: float, imei: str, success: bool):
        if user_id in self.users:
            user = self.users[user_id]
            user.total_queries += 1
            if success:
                user.balance -= price
            
            query_record = {
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "service": service_title,
                "price": price,
                "imei": imei[-4:],
                "success": success
            }
            user.query_history.append(query_record)
            
            if len(user.query_history) > 50:
                user.query_history = user.query_history[-50:]
                
            self.save_users()

class IMEIValidator:
    @staticmethod
    def validate_imei(imei: str) -> tuple[bool, str]:
        if not imei:
            return False, "IMEI no puede estar vacío"
        
        clean_imei = re.sub(r'[^\d]', '', imei)
        
        if not clean_imei.isdigit():
            return False, "IMEI debe contener solo números"
        
        if len(clean_imei) < 8 or len(clean_imei) > 17:
            return False, "IMEI debe tener entre 8 y 17 dígitos"
        
        if len(clean_imei) == 15:
            if not IMEIValidator._luhn_check(clean_imei):
                return False, "IMEI no válido según algoritmo de verificación"
        
        return True, clean_imei

    @staticmethod
    def _luhn_check(imei: str) -> bool:
        total = 0
        reverse_digits = imei[::-1]
        
        for i, digit in enumerate(reverse_digits):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n = n // 10 + n % 10
            total += n
        
        return total % 10 == 0

class IMEIChecker:
    def __init__(self, config: Config):
        self.config = config
        self.session = None

    async def __aenter__(self):
        self.session = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.request_timeout),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.aclose()

    async def check_imei(self, imei: str, service_id: int) -> Dict[str, Any]:
        params = {
            "key": self.config.imei_api_key,
            "service": service_id,
            "imei": imei
        }

        last_error = None
        for attempt in range(self.config.max_retries):
            try:
                response = await self.session.get(self.config.imei_api_url, params=params)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    response.raise_for_status()
                    
            except httpx.TimeoutException:
                last_error = f"Timeout en intento {attempt + 1}"
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
            except httpx.RequestError as e:
                last_error = f"Error de conexión: {str(e)}"
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        raise APIError(f"Error después de {self.config.max_retries} intentos. {last_error}")

class ResponseFormatter:
    @staticmethod
    def format_imei_response(response_json: Dict[str, Any]) -> str:
        try:
            service_name = response_json.get("service_name", "No disponible")
            imei = response_json.get("imei", "No disponible")
            status = response_json.get("status", "No disponible")
            credit = response_json.get("credit", "0.00")
            balance = response_json.get("balance_left", "0.00")

            result_raw = response_json.get("result", "")
            clean_result = ResponseFormatter._clean_html_content(result_raw)

            message = (
                f"📱 <b>Consulta IMEI</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🔍 <b>Servicio:</b> {service_name}\n"
                f"📟 <b>IMEI:</b> <code>{imei}</code>\n"
                f"⚡ <b>Estado:</b> {status}\n"
                f"💰 <b>Crédito usado:</b> ${credit}\n"
                f"💳 <b>Saldo restante:</b> ${balance}\n"
            )

            if clean_result:
                message += f"\n📋 <b>Detalles:</b>\n<pre>{clean_result[:1500]}</pre>"
                if len(clean_result) > 1500:
                    message += "\n<i>... (resultado truncado)</i>"

            return message

        except Exception as e:
            logger.error(f"Error formateando respuesta: {e}")
            return "❌ Error al formatear la respuesta del servidor"

    @staticmethod
    def _clean_html_content(html_content: str) -> str:
        if not html_content:
            return "No hay información disponible"

        decoded = html.unescape(str(html_content))
        
        replacements = [
            ("\\u003Cbr\\u003E", "\n"), ("<br>", "\n"), ("<br/>", "\n"), 
            ("&nbsp;", " "), ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">")
        ]
        
        for old, new in replacements:
            decoded = decoded.replace(old, new)
        
        clean_text = re.sub(r'<[^>]*>', '', decoded)
        
        lines = []
        for line in clean_text.split('\n'):
            clean_line = line.strip()
            if clean_line:
                lines.append(clean_line)
        
        return '\n'.join(lines)

class IMEIBot:
    def __init__(self, config: Config):
        self.config = config
        self.bot = Bot(token=config.bot_token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.db = UserDatabase(config.users_db_path)
        self.autopinger = AutoPinger(config, self.bot)
        self.services_by_id = {s["id"]: s for s in SERVICES_DATA}
        self.services_by_category = {}
        
        # Cargar servicios desde archivo si existe
        self._load_services()
        
        for service in SERVICES_DATA:
            category = service["category"]
            if category not in self.services_by_category:
                self.services_by_category[category] = []
            self.services_by_category[category].append(service)
        
        self._setup_handlers()

    def _setup_handlers(self):
        self.dp.message(CommandStart())(self.cmd_start)
        self.dp.message(Command("help"))(self.cmd_help)
        self.dp.message(Command("ping"))(self.cmd_ping)
        self.dp.message(Command("cancel"))(self.cmd_cancel)
        self.dp.message(Command("account"))(self.cmd_account)
        self.dp.message(Command("addbalance"))(self.cmd_add_balance)
        self.dp.message(Command("addservice"))(self.cmd_add_service)
        self.dp.message(Command("removeservice"))(self.cmd_remove_service)
        self.dp.message(Command("listservices"))(self.cmd_list_services)
        self.dp.message(Command("stats"))(self.cmd_stats)
        self.dp.message(Command("broadcast"))(self.cmd_broadcast)
        
        # Comandos de AutoPinger
        self.dp.message(Command("autopinger"))(self.cmd_autopinger)
        self.dp.message(Command("autopingstart"))(self.cmd_autoping_start)
        self.dp.message(Command("autopingstop"))(self.cmd_autoping_stop)
        
        self.dp.callback_query()(self.handle_callback_query)
        
        self.dp.message(IMEIStates.waiting_for_service_category)(self.handle_category_selection)
        self.dp.message(IMEIStates.waiting_for_service)(self.handle_category_selection)
        self.dp.message(IMEIStates.waiting_for_imei)(self.handle_imei_input)
        
        self.dp.message()(self.handle_category_selection)

    def _is_owner(self, user_id: int) -> bool:
        return user_id == self.config.owner_id

    def _create_main_menu(self) -> ReplyKeyboardMarkup:
        buttons = [
            [KeyboardButton(text="🔍 Consultar IMEI")],
            [KeyboardButton(text="👤 Mi Cuenta"), KeyboardButton(text="❓ Ayuda")],
            [KeyboardButton(text="❌ Cancelar")]
        ]
        return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

    def _create_categories_keyboard(self) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        
        for category in self.services_by_category.keys():
            emoji = {"Apple": "🍎", "Android": "🤖", "General": "🔧"}.get(category, "📱")
            builder.button(text=f"{emoji} {category}", callback_data=f"cat_{category}")
        
        builder.button(text="❌ Cancelar", callback_data="cancel")
        builder.adjust(1)
        return builder.as_markup()

    def _create_services_keyboard(self, category: str) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        
        services = self.services_by_category.get(category, [])
        for service in services[:15]:
            text = f"${service['price']} - {service['title']}"
            if len(text) > 64:
                text = f"${service['price']} - {service['title'][:50]}..."
            builder.button(text=text, callback_data=f"svc_{service['id']}")
        
        builder.button(text="🔙 Volver", callback_data="back_to_categories")
        builder.button(text="❌ Cancelar", callback_data="cancel")
        builder.adjust(1)
        return builder.as_markup()

    async def cmd_start(self, message: Message, state: FSMContext):
        await state.clear()
        
        user = message.from_user
        self.db.get_or_create_user(
            user_id=user.id, username=user.username,
            first_name=user.first_name, last_name=user.last_name
        )
        
        welcome_text = (
            "🤖 <b>¡Bienvenido al Bot IMEI Checker Pro!</b>\n\n"
            "Consulta información detallada de dispositivos móviles.\n\n"
            "📱 <b>Servicios disponibles:</b>\n"
            f"• 🍎 Apple ({len(self.services_by_category.get('Apple', []))} servicios)\n"
            f"• 🤖 Android ({len(self.services_by_category.get('Android', []))} servicios)\n"
            f"• 🔧 General ({len(self.services_by_category.get('General', []))} servicios)\n\n"
            "💡 <b>¿Qué deseas hacer?</b>"
        )
        
        await message.answer(welcome_text, reply_markup=self._create_main_menu(), parse_mode="HTML")

    async def cmd_help(self, message: Message):
        help_text = (
            "🆘 <b>Ayuda - Bot IMEI Checker Pro</b>\n\n"
            "<b>🔍 Cómo usar:</b>\n"
            "1️⃣ Selecciona 'Consultar IMEI'\n"
            "2️⃣ Elige categoría y servicio\n"
            "3️⃣ Envía el IMEI (8-17 dígitos)\n\n"
            "<b>💰 Precios:</b> Desde $0.01 hasta $2.20\n"
            "<b>📞 Soporte:</b> Contacta al administrador\n\n"
            "<b>📡 Sistema AutoPing activo</b> - Bot siempre en línea"
        )
        await message.answer(help_text, parse_mode="HTML")

    async def cmd_ping(self, message: Message):
        ping_status = "🟢 Activo" if self.autopinger.is_running else "🔴 Inactivo"
        await message.answer(
            f"🏓 ¡Pong! Bot funcionando ✅\n"
            f"📡 AutoPing: {ping_status}",
            parse_mode="HTML"
        )

    async def cmd_autopinger(self, message: Message):
        """Comando /autopinger - Mostrar estado del AutoPinger"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        status = self.autopinger.get_status()
        
        status_emoji = "🟢" if status["running"] else "🔴"
        enabled_emoji = "✅" if status["enabled"] else "❌"
        
        status_text = (
            f"📡 <b>Estado del AutoPinger</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{enabled_emoji} <b>Habilitado:</b> {status['enabled']}\n"
            f"{status_emoji} <b>En ejecución:</b> {status['running']}\n"
            f"🔢 <b>Pings realizados:</b> {status['ping_count']}\n"
            f"⏰ <b>Intervalo:</b> {status['interval']}s\n"
            f"🌐 <b>URL externa:</b> {status['url']}\n"
        )
        
        if status["last_ping"]:
            last_ping_dt = datetime.fromisoformat(status["last_ping"])
            status_text += f"🕐 <b>Último ping:</b> {last_ping_dt.strftime('%H:%M:%S')}\n"
        
        await message.answer(status_text, parse_mode="HTML")

    async def cmd_autoping_start(self, message: Message):
        """Comando /autopingstart - Iniciar AutoPinger"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        if not self.config.autopinger_enabled:
            await message.answer("❌ AutoPinger está deshabilitado en la configuración.")
            return
        
        if self.autopinger.is_running:
            await message.answer("⚠️ AutoPinger ya está en ejecución.")
            return
        
        try:
            await self.autopinger.start()
            await message.answer(
                f"✅ <b>AutoPinger iniciado</b>\n"
                f"⏰ Intervalo: {self.config.autopinger_interval}s\n"
                f"📡 Manteniendo bot activo...",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"❌ Error iniciando AutoPinger: {str(e)}")

    async def cmd_autoping_stop(self, message: Message):
        """Comando /autopingstop - Detener AutoPinger"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        if not self.autopinger.is_running:
            await message.answer("⚠️ AutoPinger no está en ejecución.")
            return
        
        try:
            await self.autopinger.stop()
            await message.answer(
                f"🛑 <b>AutoPinger detenido</b>\n"
                f"📊 Total pings realizados: {self.autopinger.ping_count}",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"❌ Error deteniendo AutoPinger: {str(e)}")

    async def cmd_cancel(self, message: Message, state: FSMContext):
        await state.clear()
        await message.answer("❌ Operación cancelada.\n\n💡 ¿Qué deseas hacer?", reply_markup=self._create_main_menu())

    async def cmd_account(self, message: Message):
        user = self.db.get_or_create_user(
            user_id=message.from_user.id, username=message.from_user.username,
            first_name=message.from_user.first_name, last_name=message.from_user.last_name
        )
        
        full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        username_text = f"@{user.username}" if user.username else "No definido"
        
        account_text = (
            f"👤 <b>Mi Cuenta</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>ID:</b> <code>{user.user_id}</code>\n"
            f"👨‍💼 <b>Nombre:</b> {full_name or 'No definido'}\n"
            f"📱 <b>Usuario:</b> {username_text}\n"
            f"💰 <b>Balance:</b> ${user.balance:.2f}\n"
            f"📊 <b>Consultas:</b> {user.total_queries}\n"
        )
        
        if user.query_history:
            account_text += f"\n📋 <b>Historial reciente:</b>\n"
            for query in user.query_history[-3:]:
                status_emoji = "✅" if query["success"] else "❌"
                account_text += f"{status_emoji} ${query['price']} - IMEI: ...{query['imei']}\n"
        
        await message.answer(account_text, parse_mode="HTML")

    async def cmd_add_balance(self, message: Message):
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        try:
            parts = message.text.split()
            if len(parts) != 3:
                await message.answer("❌ Uso: /addbalance <user_id> <amount>")
                return
            
            target_user_id = int(parts[1])
            amount = float(parts[2])
            
            if target_user_id not in self.db.users:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.db.users[target_user_id] = UserData(
                    user_id=target_user_id, username=None, first_name="Usuario",
                    last_name=None, join_date=now, last_activity=now
                )
            
            user = self.db.users[target_user_id]
            old_balance = user.balance
            user.balance += amount
            self.db.save_users()
            
            await message.answer(
                f"✅ Balance actualizado\n"
                f"👤 Usuario: {target_user_id}\n" 
                f"💰 ${old_balance:.2f} → ${user.balance:.2f}",
                parse_mode="HTML"
            )
                
        except (ValueError, IndexError):
            await message.answer("❌ Formato inválido")

    async def cmd_add_service(self, message: Message):
        """Comando /addservice - Solo para el owner"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        try:
            # Format: /addservice <id> <title> <price> <category>
            parts = message.text.split(maxsplit=4)
            if len(parts) != 5:
                await message.answer(
                    "❌ Uso: /addservice <id> <title> <price> <category>\n"
                    "Ejemplo: /addservice 100 \"iPhone Info Pro\" 0.50 Apple\n"
                    "Categorías: Apple, Android, General"
                )
                return
            
            service_id = int(parts[1])
            title = parts[2].strip('"')
            price = parts[3]
            category = parts[4]
            
            # Validar categoría
            if category not in ["Apple", "Android", "General"]:
                await message.answer("❌ Categoría debe ser: Apple, Android o General")
                return
            
            # Verificar si el ID ya existe
            if service_id in self.services_by_id:
                await message.answer(f"❌ Ya existe un servicio con ID {service_id}")
                return
            
            # Agregar nuevo servicio
            new_service = {
                "id": service_id,
                "title": title,
                "price": price,
                "category": category
            }
            
            # Actualizar estructuras de datos
            SERVICES_DATA.append(new_service)
            self.services_by_id[service_id] = new_service
            
            if category not in self.services_by_category:
                self.services_by_category[category] = []
            self.services_by_category[category].append(new_service)
            
            # Guardar en archivo
            self._save_services()
            
            await message.answer(
                f"✅ <b>Servicio agregado</b>\n"
                f"🆔 ID: {service_id}\n"
                f"📝 Título: {title}\n"
                f"💰 Precio: ${price}\n"
                f"📂 Categoría: {category}",
                parse_mode="HTML"
            )
            
        except (ValueError, IndexError):
            await message.answer("❌ Formato inválido. Revisa la sintaxis.")

    async def cmd_remove_service(self, message: Message):
        """Comando /removeservice - Solo para el owner"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        try:
            parts = message.text.split()
            if len(parts) != 2:
                await message.answer("❌ Uso: /removeservice <service_id>")
                return
            
            service_id = int(parts[1])
            
            if service_id not in self.services_by_id:
                await message.answer(f"❌ No existe servicio con ID {service_id}")
                return
            
            # Obtener servicio antes de eliminarlo
            service = self.services_by_id[service_id]
            
            # Remover de todas las estructuras
            SERVICES_DATA[:] = [s for s in SERVICES_DATA if s["id"] != service_id]
            del self.services_by_id[service_id]
            
            # Remover de categoría
            category = service["category"]
            if category in self.services_by_category:
                self.services_by_category[category] = [
                    s for s in self.services_by_category[category] if s["id"] != service_id
                ]
                # Eliminar categoría si está vacía
                if not self.services_by_category[category]:
                    del self.services_by_category[category]
            
            # Guardar cambios
            self._save_services()
            
            await message.answer(
                f"✅ <b>Servicio eliminado</b>\n"
                f"🆔 ID: {service_id}\n"
                f"📝 Título: {service['title']}\n"
                f"📂 Categoría: {service['category']}",
                parse_mode="HTML"
            )
            
        except (ValueError, IndexError):
            await message.answer("❌ Formato inválido.")

    async def cmd_list_services(self, message: Message):
        """Comando /listservices - Solo para el owner"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        if not SERVICES_DATA:
            await message.answer("📝 No hay servicios configurados.")
            return
        
        services_text = f"📋 <b>Lista de Servicios ({len(SERVICES_DATA)})</b>\n\n"
        
        for category in self.services_by_category:
            services_text += f"📂 <b>{category}:</b>\n"
            for service in self.services_by_category[category]:
                services_text += f"• ID {service['id']}: ${service['price']} - {service['title'][:40]}...\n"
            services_text += "\n"
        
        # Dividir mensaje si es muy largo
        if len(services_text) > 4000:
            services_text = services_text[:4000] + "\n<i>... lista truncada</i>"
        
        await message.answer(services_text, parse_mode="HTML")

    async def cmd_stats(self, message: Message):
        """Comando /stats - Solo para el owner"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        total_users = len(self.db.users)
        total_queries = sum(user.total_queries for user in self.db.users.values())
        total_balance = sum(user.balance for user in self.db.users.values())
        total_services = len(SERVICES_DATA)
        
        # Usuario más activo
        most_active = max(self.db.users.values(), key=lambda u: u.total_queries, default=None)
        
        # Servicios por categoría
        cat_counts = {cat: len(services) for cat, services in self.services_by_category.items()}
        
        # Estado del AutoPinger
        autopinger_status = self.autopinger.get_status()
        ping_status = "🟢 Activo" if autopinger_status["running"] else "🔴 Inactivo"
        
        stats_text = (
            f"📊 <b>Estadísticas del Bot</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>Total usuarios:</b> {total_users}\n"
            f"🔍 <b>Total consultas:</b> {total_queries}\n"
            f"💰 <b>Balance total:</b> ${total_balance:.2f}\n"
            f"🛠️ <b>Total servicios:</b> {total_services}\n"
            f"📡 <b>AutoPing:</b> {ping_status} ({autopinger_status['ping_count']} pings)\n\n"
            f"📂 <b>Por categoría:</b>\n"
        )
        
        for category, count in cat_counts.items():
            emoji = {"Apple": "🍎", "Android": "🤖", "General": "🔧"}.get(category, "📱")
            stats_text += f"• {emoji} {category}: {count} servicios\n"
        
        if most_active and most_active.total_queries > 0:
            stats_text += (
                f"\n🏆 <b>Usuario más activo:</b>\n"
                f"👤 {most_active.first_name or 'Sin nombre'} "
                f"({most_active.user_id})\n"
                f"📊 {most_active.total_queries} consultas\n"
            )
        
        await message.answer(stats_text, parse_mode="HTML")

    async def cmd_broadcast(self, message: Message):
        """Comando /broadcast - Solo para el owner"""
        if not self._is_owner(message.from_user.id):
            await message.answer("❌ Sin permisos.")
            return
        
        try:
            # Extraer mensaje después del comando
            parts = message.text.split(maxsplit=1)
            if len(parts) < 2:
                await message.answer("❌ Uso: /broadcast <mensaje>")
                return
            
            broadcast_msg = parts[1]
            
            # Confirmar antes de enviar
            confirm_text = (
                f"📢 <b>Confirmar Broadcast</b>\n\n"
                f"👥 Se enviará a {len(self.db.users)} usuarios\n\n"
                f"<b>Mensaje:</b>\n{broadcast_msg}\n\n"
                f"¿Continuar? Responde 'SI' para confirmar."
            )
            
            await message.answer(confirm_text, parse_mode="HTML")
            
            # Aquí podrías implementar un estado para confirmar
            # Por simplicidad, enviaremos directamente
            
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")

    def _save_services(self):
        """Guardar servicios en archivo JSON"""
        try:
            services_file = Path("services.json")
            with open(services_file, 'w', encoding='utf-8') as f:
                json.dump(SERVICES_DATA, f, indent=2, ensure_ascii=False)
            logger.info("Servicios guardados en services.json")
        except Exception as e:
            logger.error(f"Error guardando servicios: {e}")

    def _load_services(self):
        """Cargar servicios desde archivo JSON"""
        try:
            services_file = Path("services.json")
            if services_file.exists():
                with open(services_file, 'r', encoding='utf-8') as f:
                    loaded_services = json.load(f)
                    SERVICES_DATA.clear()
                    SERVICES_DATA.extend(loaded_services)
                    logger.info(f"Servicios cargados desde archivo: {len(SERVICES_DATA)}")
        except Exception as e:
            logger.error(f"Error cargando servicios: {e}")

    async def handle_category_selection(self, message: Message, state: FSMContext):
        text = message.text.strip()
        
        if text == "🔍 Consultar IMEI":
            await message.answer(
                "📱 <b>Selecciona una categoría:</b>",
                reply_markup=self._create_categories_keyboard(),
                parse_mode="HTML"
            )
            await state.set_state(IMEIStates.waiting_for_service_category)
            
        elif text == "👤 Mi Cuenta":
            await self.cmd_account(message)
            
        elif text == "❓ Ayuda":
            await self.cmd_help(message)
            
        elif text == "❌ Cancelar":
            await self.cmd_cancel(message, state)
            
        else:
            await message.answer("❌ Opción no válida", reply_markup=self._create_main_menu())

    async def handle_callback_query(self, callback_query, state: FSMContext):
        try:
            data = callback_query.data
            
            if data.startswith("cat_"):
                category = data[4:]
                await callback_query.message.edit_text(
                    f"📱 <b>Servicios de {category}:</b>\n\nSelecciona el servicio:",
                    reply_markup=self._create_services_keyboard(category),
                    parse_mode="HTML"
                )
                await state.update_data(selected_category=category)
                await state.set_state(IMEIStates.waiting_for_service)
                
            elif data.startswith("svc_"):
                service_id = int(data[4:])
                service = self.services_by_id.get(service_id)
                
                if service:
                    await state.update_data(selected_service=service)
                    await callback_query.message.edit_text(
                        f"✅ <b>Servicio:</b> {service['title']}\n"
                        f"💰 <b>Precio:</b> ${service['price']}\n\n"
                        f"📟 Envía el <b>número IMEI</b> (8-17 dígitos):",
                        parse_mode="HTML"
                    )
                    await state.set_state(IMEIStates.waiting_for_imei)
                
            elif data == "back_to_categories":
                await callback_query.message.edit_text(
                    "📱 <b>Selecciona una categoría:</b>",
                    reply_markup=self._create_categories_keyboard(),
                    parse_mode="HTML"
                )
                await state.set_state(IMEIStates.waiting_for_service_category)
                
            elif data == "cancel":
                await callback_query.message.delete()
                await callback_query.message.answer(
                    "❌ Cancelado. ¿Qué deseas hacer?",
                    reply_markup=self._create_main_menu()
                )
                await state.clear()
            
            await callback_query.answer()
            
        except Exception as e:
            logger.error(f"Error en callback: {e}")
            await callback_query.answer("❌ Error procesando solicitud")

    async def handle_imei_input(self, message: Message, state: FSMContext):
        imei_input = message.text.strip()
        
        is_valid, result = IMEIValidator.validate_imei(imei_input)
        if not is_valid:
            await message.answer(f"❌ {result}\n\nEnvía un IMEI válido:")
            return

        clean_imei = result
        data = await state.get_data()
        service = data.get("selected_service")
        
        if not service:
            await message.answer("❌ Error: No hay servicio seleccionado. Usa /start")
            await state.clear()
            return

        user = self.db.get_or_create_user(
            user_id=message.from_user.id, username=message.from_user.username,
            first_name=message.from_user.first_name, last_name=message.from_user.last_name
        )
        
        service_price = float(service["price"])
        if user.balance < service_price and not self._is_owner(message.from_user.id):
            await message.answer(
                f"❌ <b>Saldo insuficiente</b>\n\n"
                f"💰 Tu balance: ${user.balance:.2f}\n"
                f"💳 Precio: ${service_price:.2f}\n"
                f"📊 Necesitas: ${service_price - user.balance:.2f} más",
                parse_mode="HTML"
            )
            await state.clear()
            await message.answer("💡 ¿Qué deseas hacer?", reply_markup=self._create_main_menu())
            return

        processing_msg = await message.answer(
            f"🔄 <b>Procesando...</b>\n"
            f"📟 IMEI: ...{clean_imei[-4:]}\n"
            f"💰 Precio: ${service_price:.2f}",
            parse_mode="HTML"
        )

        try:
            async with IMEIChecker(self.config) as checker:
                response = await checker.check_imei(clean_imei, service["id"])
                
            formatted_response = ResponseFormatter.format_imei_response(response)
            await processing_msg.delete()
            await message.answer(formatted_response, parse_mode="HTML")
            
            self.db.update_user_query(
                user_id=message.from_user.id, service_title=service["title"],
                price=service_price, imei=clean_imei, success=True
            )
            
        except APIError as e:
            await processing_msg.delete()
            await message.answer(f"❌ <b>Error en la consulta:</b>\n{str(e)}", parse_mode="HTML")
            
            self.db.update_user_query(
                user_id=message.from_user.id, service_title=service["title"],
                price=0.0, imei=clean_imei, success=False
            )
            
        except Exception as e:
            await processing_msg.delete()
            await message.answer("❌ Error inesperado. Inténtalo más tarde.", parse_mode="HTML")
            logger.error(f"Error inesperado: {e}")

        await state.clear()
        await asyncio.sleep(1)
        await message.answer("✨ <b>¿Otra consulta?</b>", reply_markup=self._create_main_menu(), parse_mode="HTML")

    async def start_polling(self):
        logger.info("🤖 Iniciando bot IMEI Checker Pro...")
        
        # Iniciar AutoPinger si está habilitado
        if self.config.autopinger_enabled:
            await self.autopinger.start()
        
        try:
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Error crítico: {e}")
            raise
        finally:
            # Detener AutoPinger y cerrar sesión
            await self.autopinger.stop()
            await self.bot.session.close()

async def main():
    try:
        config = load_config()
        bot = IMEIBot(config)
        await bot.start_polling()
    except Exception as e:
        logger.error(f"Error al iniciar: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())