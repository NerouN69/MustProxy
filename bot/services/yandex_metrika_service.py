# bot/services/yandex_metrika_service.py

import logging
import asyncio
import aiohttp
import time
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, timezone

from config.settings import Settings
from db.dal import yandex_tracking_dal, payment_dal

class YandexMetrikaService:
    """Сервис для отправки данных в Яндекс.Метрику через Measurement Protocol"""
    
    def __init__(self, settings: Settings, bot_username: str = "your_bot"):
        self.settings = settings
        self.bot_username = bot_username
        self.metrika_url = "https://mc.yandex.ru/collect"
        self.counter_id = getattr(settings, 'YANDEX_METRIKA_COUNTER_ID', None)
        self.measurement_token = getattr(settings, 'YANDEX_METRIKA_TOKEN', None)
        self.configured = bool(self.counter_id and self.measurement_token)
        
        # Константы для управления визитами
        self.VISIT_COMPLETION_HOURS = 12  # Часы после завершения визита для дополнения
        self.SESSION_TIMEOUT_MINUTES = 30  # Тайм-аут сессии в минутах
        
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

    async def track_visit(
        self,
        session: AsyncSession,
        user_id: int,
        client_id: str,
        page_url: Optional[str] = None,
        page_title: str = "Telegram Bot Visit",
        referrer: str = "https://yandex.ru"
    ) -> bool:
        """
        Отслеживает визит пользователя, создавая новый визит при необходимости
        """
        if not self.configured or not self._validate_client_id(client_id):
            return False

        # Получаем трекинг пользователя
        tracking = await yandex_tracking_dal.get_tracking_by_user_id(session, user_id)
        
        if tracking:
            # Проверяем, нужно ли создать новый визит
            last_visit = tracking.last_visit_time or tracking.first_visit_time
            time_since_last = datetime.now(timezone.utc) - last_visit
            
            # Если прошло больше 12 часов + тайм-аут сессии, создаем новый визит
            if time_since_last > timedelta(hours=self.VISIT_COMPLETION_HOURS, minutes=self.SESSION_TIMEOUT_MINUTES):
                # Отправляем pageview для создания нового визита
                success = await self.send_pageview(
                    client_id=client_id,
                    page_url=page_url or f"https://t.me/{self.bot_username}",
                    page_title=page_title,
                    referrer=referrer
                )
                
                if success:
                    # Обновляем время последнего визита
                    await yandex_tracking_dal.update_last_visit_time(session, tracking.tracking_id)
                    # Увеличиваем счетчик визитов
                    await yandex_tracking_dal.increment_visit_count(session, tracking.tracking_id)
                
                return success
            else:
                # Визит еще активен, можно дополнять событиями
                logging.info(f"Visit still active for user {user_id}, can append events")
                return True
        else:
            # Первый визит пользователя
            tracking = await yandex_tracking_dal.create_yandex_tracking(
                session, user_id, client_id, self.counter_id
            )
            
            if tracking:
                # Отправляем первый pageview
                success = await self.send_pageview(
                    client_id=client_id,
                    page_url=page_url or f"https://t.me/{self.bot_username}",
                    page_title="First Visit",
                    referrer=referrer
                )
                return success
        
        return False

    async def send_pageview(
        self, 
        client_id: str,
        page_url: Optional[str] = None,
        page_title: str = "Telegram Bot Visit",
        referrer: str = "https://yandex.ru",
        event_time: Optional[int] = None
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
            'et': str(event_time) if event_time else str(int(time.time())),
            'ms': self.measurement_token
        }

        return await self._send_request(params, "pageview")

    async def send_conversion(
        self,
        client_id: str,
        goal_name: str = "purchase",
        goal_value: Optional[float] = None,
        currency: str = "RUB",
        page_url: Optional[str] = None,
        event_time: Optional[int] = None
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
            'et': str(event_time) if event_time else str(int(time.time())),
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
        products: Optional[List[Dict]] = None,
        page_url: Optional[str] = None,
        event_time: Optional[int] = None,
        coupon: Optional[str] = None
    ) -> bool:
        """Отправляет покупку через ecommerce в Яндекс.Метрику с полными данными"""
        if not self.configured:
            return False

        if page_url is None:
            page_url = f"https://t.me/{self.bot_username}/purchase"

        params = {
            'tid': self.counter_id,
            'cid': client_id,
            't': 'event',
            'et': str(event_time) if event_time else str(int(time.time())),
            'pa': 'purchase',
            'ti': transaction_id,
            'tr': str(revenue),
            'cu': currency,
            'dl': page_url,
            'ms': self.measurement_token
        }
        
        # Добавляем купон, если есть
        if coupon:
            params['tcc'] = coupon

        if products:
            for i, product in enumerate(products, 1):
                params[f'pr{i}id'] = product.get('id', f'product_{i}')
                params[f'pr{i}nm'] = product.get('name', 'Subscription')
                params[f'pr{i}br'] = product.get('brand', 'Proxy Service')
                params[f'pr{i}ca'] = product.get('category', 'Subscription')
                params[f'pr{i}pr'] = str(product.get('price', revenue))
                params[f'pr{i}qt'] = str(product.get('quantity', 1))
                params[f'pr{i}va'] = product.get('variant', 'monthly')
                if product.get('coupon'):
                    params[f'pr{i}cc'] = product.get('coupon')
        else:
            # Дефолтный продукт
            params['pr1id'] = 'subscription'
            params['pr1nm'] = 'Proxy Subscription'
            params['pr1br'] = 'Proxy Service'
            params['pr1ca'] = 'Subscription'
            params['pr1pr'] = str(revenue)
            params['pr1qt'] = '1'

        return await self._send_request(params, "ecommerce_purchase")

    async def send_full_conversion_chain(
        self,
        session: AsyncSession,
        user_id: int,
        payment_amount: float,
        payment_id: str,
        subscription_months: int = 1,
        promo_code: Optional[str] = None
    ) -> bool:
        """
        Отправляет полную цепочку конверсии с учетом времени покупки
        """
        if not self.configured:
            logging.warning("Yandex Metrika not configured, skipping conversion chain")
            return False

        tracking = await yandex_tracking_dal.get_tracking_by_user_id(session, user_id)
        if not tracking:
            logging.info(f"No Yandex tracking found for user {user_id}, skipping conversion")
            return False

        # Проверяем, не отправлена ли уже эта конверсия
        if await yandex_tracking_dal.is_conversion_sent_for_payment(session, user_id, payment_id):
            logging.info(f"Conversion already sent for payment {payment_id}, skipping")
            return True

        client_id = tracking.yandex_client_id
        
        # Проверяем, можем ли дополнить существующий визит
        last_visit = tracking.last_visit_time or tracking.first_visit_time
        time_since_visit = datetime.now(timezone.utc) - last_visit
        
        page_url = f"https://t.me/{self.bot_username}/purchase"
        
        # Если прошло больше 12 часов, создаем новый визит
        if time_since_visit > timedelta(hours=self.VISIT_COMPLETION_HOURS):
            logging.info(f"Creating new visit for conversion (last visit was {time_since_visit.total_seconds()/3600:.1f} hours ago)")
            
            # Создаем новый визит с pageview
            pageview_success = await self.send_pageview(
                client_id=client_id,
                page_url=page_url,
                page_title="Purchase Visit"
            )
            
            if not pageview_success:
                logging.error(f"Failed to create new visit for user {user_id}")
                return False
            
            # Обновляем время визита
            await yandex_tracking_dal.update_last_visit_time(session, tracking.tracking_id)
            await yandex_tracking_dal.increment_visit_count(session, tracking.tracking_id)
            
            # Небольшая задержка между событиями
            await asyncio.sleep(0.5)

        # Формируем данные о продукте
        products = [{
            'id': f'subscription_{subscription_months}m',
            'name': f'Proxy Subscription {subscription_months} month{"s" if subscription_months > 1 else ""}',
            'brand': 'Proxy Service',
            'category': f'{"Trial" if subscription_months == 0 else "Premium"} Subscription',
            'price': payment_amount,
            'quantity': 1,
            'variant': f'{subscription_months}_months',
            'coupon': promo_code
        }]

        # Отправляем ecommerce покупку
        ecom_success = await self.send_ecommerce_purchase(
            client_id=client_id,
            transaction_id=payment_id,
            revenue=payment_amount,
            products=products,
            page_url=page_url,
            coupon=promo_code
        )

        if ecom_success:
            # Отправляем также обычную цель-конверсию
            await asyncio.sleep(0.5)
            conversion_success = await self.send_conversion(
                client_id=client_id,
                goal_name="purchase_completed",
                goal_value=payment_amount,
                page_url=page_url
            )
            
            # Сохраняем информацию об отправленной конверсии
            await yandex_tracking_dal.save_conversion_record(
                session, 
                user_id, 
                payment_id, 
                payment_amount
            )
            
            logging.info(f"Successfully sent full conversion chain for user {user_id}, payment {payment_id}")
            return True
        else:
            logging.error(f"Failed to send ecommerce data for user {user_id}")
            return False

    async def resend_missing_conversions(
        self,
        session: AsyncSession,
        limit: int = 50
    ) -> Dict[str, int]:
        """
        Повторная отправка конверсий с реальными данными платежей
        """
        if not self.configured:
            return {"processed": 0, "success": 0, "failed": 0}
        
        results = {"processed": 0, "success": 0, "failed": 0}
        
        # Получаем пользователей с неотправленными конверсиями
        users_with_payments = await yandex_tracking_dal.get_users_with_untracked_payments(session, limit)
        
        for user_id, payment_data in users_with_payments.items():
            results["processed"] += 1
            
            try:
                # Для каждого неотслеженного платежа
                for payment in payment_data['payments']:
                    success = await self.send_full_conversion_chain(
                        session=session,
                        user_id=user_id,
                        payment_amount=payment['amount'],
                        payment_id=payment['payment_id'],
                        subscription_months=payment.get('subscription_duration_months', 1),
                        promo_code=payment.get('promo_code')
                    )
                    
                    if success:
                        results["success"] += 1
                    else:
                        results["failed"] += 1
                    
                    # Небольшая задержка между отправками
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logging.error(f"Failed to resend conversion for user {user_id}: {e}")
                results["failed"] += 1
        
        await session.commit()
        return results

    async def get_tracking_statistics(self, session: AsyncSession) -> Dict[str, Any]:
        """Расширенная статистика по отслеживанию"""
        stats = await yandex_tracking_dal.get_tracking_statistics(session)
        
        # Добавляем информацию о визитах
        visit_stats = await yandex_tracking_dal.get_visit_statistics(session)
        stats.update(visit_stats)
        
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
                            f"Successfully sent {event_type} to Yandex Metrika for client_id: "
                            f"{params.get('cid', 'unknown')[:10]}..."
                        )
                    else:
                        response_text = await response.text()
                        logging.error(
                            f"Failed to send {event_type} to Yandex Metrika. "
                            f"Status: {response.status}, Response: {response_text}"
                        )
                    return success
        except asyncio.TimeoutError:
            logging.error(f"Timeout sending {event_type} to Yandex Metrika")
            return False
        except Exception as e:
            logging.error(f"Error sending {event_type} to Yandex Metrika: {e}", exc_info=True)
            return False

    async def close(self):
        """Cleanup method"""
        pass