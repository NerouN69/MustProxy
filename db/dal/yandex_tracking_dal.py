# db/dal/yandex_tracking_dal.py

import logging
from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, delete, func, and_
from datetime import datetime, timezone

from ..models import YandexTracking, User


async def create_yandex_tracking(
    session: AsyncSession, 
    user_id: int, 
    yandex_client_id: str,
    counter_id: Optional[str] = None
) -> Optional[YandexTracking]:
    """Создает или обновляет запись отслеживания Яндекс.Метрики для пользователя"""
    
    try:
        # Проверяем, существует ли запись для user_id
        existing = await get_tracking_by_user_id(session, user_id)
        if existing:
            if existing.yandex_client_id != yandex_client_id:
                # Обновляем yandex_client_id, если он изменился
                stmt = (
                    update(YandexTracking)
                    .where(YandexTracking.user_id == user_id)
                    .values(yandex_client_id=yandex_client_id, counter_id=counter_id)
                )
                await session.execute(stmt)
                await session.refresh(existing)
                logging.info(f"Updated YandexTracking for user {user_id} with new client_id {yandex_client_id}")
            else:
                logging.info(f"YandexTracking already exists for user {user_id} with client_id {yandex_client_id}")
            return existing
        
        # Создаём новую запись, если не существует
        tracking_data = {
            "user_id": user_id,
            "yandex_client_id": yandex_client_id,
            "counter_id": counter_id,
            "first_visit_time": datetime.now(timezone.utc),
            "conversion_sent": False
        }
        new_tracking = YandexTracking(**tracking_data)
        session.add(new_tracking)
        await session.flush()
        await session.refresh(new_tracking)
        logging.info(f"YandexTracking created for user {user_id} with client_id {yandex_client_id}")
        return new_tracking
    except Exception as e:
        logging.error(f"Failed to create/update YandexTracking: {e}", exc_info=True)
        await session.rollback()
        return None


async def get_tracking_by_user_id(
    session: AsyncSession, 
    user_id: int
) -> Optional[YandexTracking]:
    """Получает запись отслеживания по ID пользователя (последнюю по дате)"""
    stmt = (
        select(YandexTracking)
        .where(YandexTracking.user_id == user_id)
        .order_by(YandexTracking.created_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_tracking_by_client_id(
    session: AsyncSession, 
    yandex_client_id: str
) -> Optional[YandexTracking]:
    """Получает запись отслеживания по Client ID Яндекса"""
    stmt = select(YandexTracking).where(YandexTracking.yandex_client_id == yandex_client_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_tracking_by_user_and_client_id(
    session: AsyncSession, 
    user_id: int, 
    yandex_client_id: str
) -> Optional[YandexTracking]:
    """Получает запись отслеживания по ID пользователя и Client ID"""
    stmt = select(YandexTracking).where(
        and_(
            YandexTracking.user_id == user_id,
            YandexTracking.yandex_client_id == yandex_client_id
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def mark_conversion_sent(
    session: AsyncSession, 
    tracking_id: int
) -> bool:
    """Помечает конверсию как отправленную"""
    stmt = (
        update(YandexTracking)
        .where(YandexTracking.tracking_id == tracking_id)
        .values(conversion_sent=True)
    )
    result = await session.execute(stmt)
    return result.rowcount > 0


async def get_users_without_conversion(
    session: AsyncSession, 
    limit: int = 100
) -> List[YandexTracking]:
    """Получает пользователей, для которых еще не отправлена конверсия"""
    stmt = (
        select(YandexTracking)
        .where(YandexTracking.conversion_sent == False)
        .limit(limit)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def delete_tracking_by_user_id(
    session: AsyncSession, 
    user_id: int
) -> bool:
    """Удаляет записи отслеживания для пользователя"""
    stmt = delete(YandexTracking).where(YandexTracking.user_id == user_id)
    result = await session.execute(stmt)
    return result.rowcount > 0


async def get_tracking_statistics(session: AsyncSession) -> Dict[str, Any]:
    """Получает статистику по отслеживанию"""
    
    # Общее количество записей
    total_stmt = select(func.count(YandexTracking.tracking_id))
    total_count = (await session.execute(total_stmt)).scalar() or 0
    
    # Количество отправленных конверсий
    sent_stmt = select(func.count(YandexTracking.tracking_id)).where(
        YandexTracking.conversion_sent == True
    )
    sent_count = (await session.execute(sent_stmt)).scalar() or 0
    
    # Количество неотправленных конверсий
    pending_stmt = select(func.count(YandexTracking.tracking_id)).where(
        YandexTracking.conversion_sent == False
    )
    pending_count = (await session.execute(pending_stmt)).scalar() or 0
    
    return {
        "total_trackings": total_count,
        "conversions_sent": sent_count,
        "conversions_pending": pending_count
    }