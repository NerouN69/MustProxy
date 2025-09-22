# bot/services/yandex_metrika_service.py

import logging
import asyncio
import aiohttp
import time
from typing import Optional, Dict, Any
from urllib.parse import urlencode
from sqlalchemy.ext.asyncio import AsyncSession

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

    async def send_pageview(
        self, 
        client_id: str,
        page_url: Optional[str] = None,
        page_title: str = "Telegram Bot Visit",
        referrer: str = "https://yandex.ru"
    ) -> bool:
        """Отправляет просмотр страницы в Яндекс.Метрику"""
        if not self.configured:
            logging.warning("Yandex Metrika not configured, skipping pageview")
            return False

        if not self._validate_client_id(client_id):
            logging.error(f"Invalid client_id format: {client_id}")
            return False

        if page_url is None:
            page_url = f"https://t.me/{self.bot_username}"

        params = {
            'tid': self.counter_id,
            'cid': client_id.strip(),
            't': 'pageview',
            'dr': referrer,
            'dl': page_url,
            'dt': page_title,
            'et': str(int(time.time())),
            'ms': self.measurement_token
        }

        return await self._send_request(params, "pageview")

    async def send_conversion(
        self,
        client_id: str,
        goal_name: str = "purchase",
        goal_value: Optional[float] = None,
        currency: str = "RUB",
        page_url: Optional[str] = None
    ) -> bool:
        """Отправляет конверсию в Яндекс.Метрику"""
        if not self.configured:
            return False

        if not self._validate_client_id(client_id):
            return False

        if page_url is None:
            page_url = f"https://t.me/{self.bot_username}/purchase"

        params = {
            'tid': self.counter_id,
            'cid': client_id.strip(),
            't': 'event',
            'ea': goal_name,
            'et': str(int(time.time())),
            'dl': page_url,
            'ms': self.measurement_token
        }

        if goal_value is not None:
            params['ev'] = str(int(goal_value))
            params['cu'] = currency

        return await self._send_request(params, "conversion")

    async def send_ecommerce_purchase(
        self,
        client_id: str,
        transaction_id: str,
        revenue: float,
        currency: str = "RUB",
        products: Optional[list] = None,
        page_url: Optional[str] = None
    ) -> bool:
        """Отправляет покупку через ecommerce в Яндекс.Метрику"""
        if not self.configured:
            return False

        if page_url is None:
            page_url = f"https://t.me/{self.bot_username}/purchase"

        params = {
            'tid': self.counter_id,
            'cid': client_id,
            't': 'event',
            'et': str(int(time.time())),
            'pa': 'purchase',
            'ti': transaction_id,
            'tr': str(revenue),
            'cu': currency,
            'dl': page_url,
            'ms': self.measurement_token
        }

        if products:
            for i, product in enumerate(products, 1):
                params[f'pr{i}id'] = product.get('id', f'product_{i}')
                params[f'pr{i}nm'] = product.get('name', 'Subscription')
                params[f'pr{i}br'] = product.get('brand', 'Your Service')
                params[f'pr{i}ca'] = product.get('category', 'Subscription')
                params[f'pr{i}pr'] = str(product.get('price', revenue))
                params[f'pr{i}qt'] = str(product.get('quantity', 1))
                params[f'pr{i}va'] = product.get('variant', 'monthly')
        else:
            params['pr1id'] = 'subscription'
            params['pr1nm'] = 'Subscription'
            params['pr1br'] = 'Your Service'
            params['pr1ca'] = 'Subscription'
            params['pr1pr'] = str(revenue)
            params['pr1qt'] = '1'
            params['pr1va'] = 'monthly'

        return await self._send_request(params, "ecommerce_purchase")

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
                            f"Successfully sent {event_type} to Yandex Metrika for client_id: "
                            f"{params.get('cid', 'unknown')[:10]}..."
                        )
                    else:
                        logging.error(
                            f"Failed to send {event_type} to Yandex Metrika. "
                            f"Status: {response.status}, Response: {await response.text()}"
                        )
                    return success
        except asyncio.TimeoutError:
            logging.error(f"Timeout sending {event_type} to Yandex Metrika")
            return False
        except Exception as e:
            logging.error(f"Error sending {event_type} to Yandex Metrika: {e}", exc_info=True)
            return False

    async def send_full_conversion_chain(
        self,
        session: AsyncSession,
        user_id: int,
        payment_amount: float,
        payment_id: str,
        subscription_months: int = 1
    ) -> bool:
        if not self.configured:
            logging.warning("Yandex Metrika not configured, skipping conversion chain")
            return False

        tracking = await yandex_tracking_dal.get_tracking_by_user_id(session, user_id)
        if not tracking:
            logging.info(f"No Yandex tracking found for user {user_id}, skipping conversion")
            return False

        if tracking.conversion_sent:
            logging.info(f"Conversion already sent for user {user_id}, skipping")
            return True

        client_id = tracking.yandex_client_id
        
        page_url = f"https://t.me/{self.bot_username}/purchase"
        
        pageview_success = await self.send_pageview(
            client_id=client_id,
            page_url=page_url,
            page_title="Purchase Completed"
        )
        
        if not pageview_success:
            logging.error(f"Failed to send pageview for user {user_id}")
            return False

        await asyncio.sleep(1)

        products = [{
            'id': f'subscription_{subscription_months}m',
            'name': f'Subscription {subscription_months} months',
            'brand': 'Your Service',
            'category': 'Subscription',
            'price': payment_amount,
            'quantity': 1,
            'variant': 'monthly'
        }]

        ecom_success = await self.send_ecommerce_purchase(
            client_id=client_id,
            transaction_id=payment_id,
            revenue=payment_amount,
            products=products,
            page_url=page_url
        )

        if ecom_success:
            await yandex_tracking_dal.mark_conversion_sent(session, tracking.tracking_id)
            logging.info(f"Successfully sent full conversion chain for user {user_id}")
            return True
        else:
            logging.error(f"Failed to send ecommerce data for user {user_id}")
            return False

    async def close(self):
        pass