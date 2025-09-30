# bot/services/yandex_metrika_service.py

import logging
import asyncio
import aiohttp
import time
from typing import Optional, Dict, Any
from urllib.parse import urlencode
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone, timedelta

from config.settings import Settings
from db.dal import yandex_tracking_dal

class YandexMetrikaService:
    """Сервис для отправки данных в Яндекс.Метрику через Measurement Protocol"""
    
    def __init__(self, settings: Settings, bot_username: str = "your_bot"):
        self.settings = settings
        self.bot_username = bot_username
        self.metrika_url = "https://mc.yandex.ru/collect"
        self.counter_id = getattr(settings, 'YANDEX_METRIKA_COUNTER_ID', None)
        self.measurement_token = getattr(settings, 'YANDEX_METRIKA_TOKEN', None)
        self.configured = bool(self.counter_id and self.measurement_token)
        
        if not self.configured:
            logging.warning(
                "Yandex Metrika service not configured. "
                "Set YANDEX_METRIKA_COUNTER_ID and YANDEX_METRIKA_TOKEN"
            )
        else:
            logging.info(f"Yandex Metrika service configured for counter {self.counter_id}")

    def _validate_client_id(self, client_id: str) -> bool:
        """Валидация Client ID для Яндекс.Метрики"""
        if not client_id or len(client_id.strip()) == 0:
            return False
        
        clean_id = client_id.strip()
        if not clean_id.replace('.', '').isdigit():
            return False
        
        if len(clean_id) < 10 or len(clean_id) > 30:
            return False
            
        return True

    async def send_install_event(
        self,
        session: AsyncSession,
        user_id: int,
        client_id: str
    ) -> bool:
        """Отправляет событие install при первом запуске бота пользователем"""
        if not self.configured or not self._validate_client_id(client_id):
            return False

        # Сохраняем или обновляем tracking
        tracking = await yandex_tracking_dal.create_yandex_tracking(
            session, user_id, client_id, self.counter_id
        )
        
        if not tracking:
            logging.error(f"Failed to create tracking for user {user_id}")
            return False

        # ВАЖНО: Сначала создаем визит через pageview
        pageview_params = {
            'tid': self.counter_id,
            'cid': client_id.strip(),
            't': 'pageview',  # Создаем визит
            'dr': 'https://yandex.ru',
            'dl': f"https://t.me/{self.bot_username}",
            'dt': 'Bot Start',
            'et': str(int(time.time())),
            'ms': self.measurement_token
        }
        
        pageview_success = await self._send_request(pageview_params, "pageview")
        
        if not pageview_success:
            logging.error(f"Failed to create visit for user {user_id}")
            return False
        
        # Небольшая задержка между запросами
        await asyncio.sleep(0.5)

        # Теперь отправляем событие install в рамках созданного визита
        event_params = {
            'tid': self.counter_id,
            'cid': client_id.strip(),
            't': 'event',
            'ea': 'install',  # JavaScript-событие install
            'et': str(int(time.time())),
            'dl': f"https://t.me/{self.bot_username}",
            'ms': self.measurement_token
        }

        success = await self._send_request(event_params, "install")
        
        if success:
            logging.info(f"Sent install event for user {user_id}, client_id: {client_id[:10]}...")
        
        return success

    async def send_purchase_event(
        self,
        session: AsyncSession,
        user_id: int,
        payment_amount: float,
        payment_id: str
    ) -> bool:
        """Отправляет событие purchase при успешной оплате"""
        if not self.configured:
            logging.warning("Yandex Metrika not configured, skipping purchase event")
            return False

        # Получаем tracking пользователя
        tracking = await yandex_tracking_dal.get_tracking_by_user_id(session, user_id)
        if not tracking:
            logging.info(f"No Yandex tracking found for user {user_id}, skipping purchase event")
            return False

        # Проверяем, не отправляли ли уже это событие
        if await yandex_tracking_dal.is_conversion_sent_for_payment(session, user_id, payment_id):
            logging.info(f"Purchase event already sent for payment {payment_id}, skipping")
            return True

        client_id = tracking.yandex_client_id
        
        # Проверяем, нужно ли создать новый визит
        last_visit = tracking.last_visit_time or tracking.first_visit_time
        time_since_visit = datetime.now(timezone.utc) - last_visit
        
        # Если прошло более 12 часов, создаем новый визит
        if time_since_visit.total_seconds() > (12 * 3600):
            logging.info(f"Creating new visit for purchase (last visit was {time_since_visit.total_seconds()/3600:.1f} hours ago)")
            
            # Создаем новый визит через pageview
            pageview_params = {
                'tid': self.counter_id,
                'cid': client_id.strip(),
                't': 'pageview',
                'dr': f"https://t.me/{self.bot_username}",
                'dl': f"https://t.me/{self.bot_username}/purchase",
                'dt': 'Purchase',
                'et': str(int(time.time())),
                'ms': self.measurement_token
            }
            
            pageview_success = await self._send_request(pageview_params, "pageview")
            
            if pageview_success:
                # Обновляем время последнего визита
                await yandex_tracking_dal.update_last_visit_time(session, tracking.tracking_id)
                await yandex_tracking_dal.increment_visit_count(session, tracking.tracking_id)
                
                # Небольшая задержка между запросами
                await asyncio.sleep(0.5)
            else:
                logging.error(f"Failed to create new visit for user {user_id}")

        # Отправляем событие purchase (в рамках существующего или нового визита)
        event_params = {
            'tid': self.counter_id,
            'cid': client_id.strip(),
            't': 'event',
            'ea': 'purchase',  # JavaScript-событие purchase
            'ev': str(int(payment_amount)),  # Ценность события
            'cu': 'RUB',  # Валюта
            'et': str(int(time.time())),
            'dl': f"https://t.me/{self.bot_username}/purchase",
            'ms': self.measurement_token
        }

        success = await self._send_request(event_params, "purchase")
        
        if success:
            # Сохраняем информацию об отправленной конверсии
            await yandex_tracking_dal.save_conversion_record(
                session, 
                user_id, 
                payment_id, 
                payment_amount
            )
            
            logging.info(f"Sent purchase event for user {user_id}, payment {payment_id}, amount {payment_amount}")
        
        return success

    async def get_tracking_statistics(self, session: AsyncSession) -> Dict[str, Any]:
        """Получает статистику по отслеживанию"""
        from db.dal import yandex_tracking_dal
        from datetime import timedelta
        from sqlalchemy import func, select
        from db.models import YandexTracking
        
        # Получаем базовую статистику
        stats = await yandex_tracking_dal.get_tracking_statistics(session)
        
        # Добавляем статистику по визитам за последние 24 часа
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        recent_visits_stmt = select(func.count(YandexTracking.tracking_id)).where(
            YandexTracking.last_visit_time >= cutoff_time
        )
        recent_visits = (await session.execute(recent_visits_stmt)).scalar() or 0
        
        # Добавляем недостающие поля
        stats['visits_last_24h'] = recent_visits
        stats['users_with_multiple_visits'] = stats.get('users_with_multiple_visits', 0)
        stats['total_visits'] = stats.get('total_visits', 0)
        stats['average_visits_per_user'] = stats.get('average_visits_per_user', 0)
        
        return stats

    async def _send_request(self, params: Dict[str, str], event_type: str) -> bool:
        """Отправляет запрос в Яндекс.Метрику"""
        try:
            query_string = urlencode(params, quote_via=lambda x, *args: x)
            url = f"{self.metrika_url}?{query_string}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    success = response.status == 200
                    if success:
                        logging.info(
                            f"Successfully sent {event_type} event to Yandex Metrika for client_id: "
                            f"{params.get('cid', 'unknown')[:10]}..."
                        )
                    else:
                        response_text = await response.text()
                        logging.error(
                            f"Failed to send {event_type} event to Yandex Metrika. "
                            f"Status: {response.status}, Response: {response_text}"
                        )
                    return success
        except asyncio.TimeoutError:
            logging.error(f"Timeout sending {event_type} event to Yandex Metrika")
            return False
        except Exception as e:
            logging.error(f"Error sending {event_type} event to Yandex Metrika: {e}", exc_info=True)
            return False

    async def close(self):
        """Cleanup method"""
        pass