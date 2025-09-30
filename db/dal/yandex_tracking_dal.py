# db/dal/yandex_tracking_dal.py

import logging
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, func, and_
from datetime import datetime, timezone, timedelta

from ..models import YandexTracking, YandexConversion, Payment


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
                    .values(
                        yandex_client_id=yandex_client_id, 
                        counter_id=counter_id,
                        last_visit_time=datetime.now(timezone.utc),
                        visit_count=YandexTracking.visit_count + 1
                    )
                )
                await session.execute(stmt)
                await session.flush()
                await session.refresh(existing)
                logging.info(f"Updated YandexTracking for user {user_id} with new client_id {yandex_client_id}")
            else:
                # Обновляем только время последнего визита
                stmt = (
                    update(YandexTracking)
                    .where(YandexTracking.user_id == user_id)
                    .values(
                        last_visit_time=datetime.now(timezone.utc),
                        visit_count=YandexTracking.visit_count + 1
                    )
                )
                await session.execute(stmt)
                await session.flush()
                await session.refresh(existing)
                logging.info(f"Updated visit time for user {user_id}")
            return existing
        
        # Создаём новую запись, если не существует
        now = datetime.now(timezone.utc)
        tracking_data = {
            "user_id": user_id,
            "yandex_client_id": yandex_client_id,
            "counter_id": counter_id,
            "first_visit_time": now,
            "last_visit_time": now,
            "visit_count": 1
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
    """Получает запись отслеживания по ID пользователя"""
    stmt = select(YandexTracking).where(YandexTracking.user_id == user_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def update_last_visit_time(
    session: AsyncSession,
    tracking_id: int
) -> bool:
    """Обновляет время последнего визита"""
    stmt = (
        update(YandexTracking)
        .where(YandexTracking.tracking_id == tracking_id)
        .values(last_visit_time=datetime.now(timezone.utc))
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.rowcount > 0


async def increment_visit_count(
    session: AsyncSession,
    tracking_id: int
) -> bool:
    """Увеличивает счетчик визитов"""
    stmt = (
        update(YandexTracking)
        .where(YandexTracking.tracking_id == tracking_id)
        .values(visit_count=YandexTracking.visit_count + 1)
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.rowcount > 0


async def save_conversion_record(
    session: AsyncSession,
    user_id: int,
    payment_id: str,
    amount: float
) -> Optional[YandexConversion]:
    """Сохраняет запись об отправленной конверсии"""
    try:
        # Проверяем, нет ли уже такой записи
        existing_stmt = select(YandexConversion).where(
            and_(
                YandexConversion.user_id == user_id,
                YandexConversion.payment_id == payment_id
            )
        )
        existing = await session.execute(existing_stmt)
        if existing.scalar_one_or_none():
            logging.info(f"Conversion already exists for user {user_id}, payment {payment_id}")
            return None
        
        conversion = YandexConversion(
            user_id=user_id,
            payment_id=payment_id,
            amount=amount,
            sent_at=datetime.now(timezone.utc)
        )
        session.add(conversion)
        await session.flush()
        await session.refresh(conversion)
        logging.info(f"Saved conversion record for user {user_id}, payment {payment_id}")
        return conversion
    except Exception as e:
        logging.error(f"Failed to save conversion record: {e}")
        return None


async def is_conversion_sent_for_payment(
    session: AsyncSession,
    user_id: int,
    payment_id: str
) -> bool:
    """Проверяет, была ли отправлена конверсия для платежа"""
    stmt = select(YandexConversion).where(
        and_(
            YandexConversion.user_id == user_id,
            YandexConversion.payment_id == payment_id
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def get_tracking_statistics(session: AsyncSession) -> Dict[str, Any]:
    """Получает статистику по отслеживанию"""
    
    # Общее количество записей отслеживания
    total_stmt = select(func.count(YandexTracking.tracking_id))
    total_count = (await session.execute(total_stmt)).scalar() or 0
    
    # Количество отправленных конверсий
    sent_stmt = select(func.count(YandexConversion.conversion_id))
    sent_count = (await session.execute(sent_stmt)).scalar() or 0
    
    # Количество уникальных пользователей с конверсиями
    unique_users_stmt = select(func.count(func.distinct(YandexConversion.user_id)))
    unique_users_with_conversions = (await session.execute(unique_users_stmt)).scalar() or 0
    
    # Сумма конверсий
    revenue_stmt = select(func.sum(YandexConversion.amount))
    total_revenue = (await session.execute(revenue_stmt)).scalar() or 0
    
    # Общее количество визитов
    total_visits_stmt = select(func.sum(YandexTracking.visit_count))
    total_visits = (await session.execute(total_visits_stmt)).scalar() or 0
    
    # Средняя частота визитов
    avg_visits_stmt = select(func.avg(YandexTracking.visit_count))
    avg_visits = (await session.execute(avg_visits_stmt)).scalar() or 0
    
    # Пользователи с множественными визитами
    multiple_visits_stmt = select(func.count(YandexTracking.tracking_id)).where(
        YandexTracking.visit_count > 1
    )
    users_with_multiple_visits = (await session.execute(multiple_visits_stmt)).scalar() or 0
    
    # Визиты за последние 24 часа
    recent_visits_stmt = select(func.count(YandexTracking.tracking_id)).where(
        YandexTracking.last_visit_time >= datetime.now(timezone.utc) - timedelta(hours=24)
    )
    recent_visits = (await session.execute(recent_visits_stmt)).scalar() or 0
    
    return {
        "total_trackings": total_count,
        "conversions_sent": sent_count,
        "unique_users_with_conversions": unique_users_with_conversions,
        "total_revenue": float(total_revenue),
        "total_visits": int(total_visits) if total_visits else 0,
        "average_visits_per_user": round(float(avg_visits), 2) if avg_visits else 0,
        "users_with_multiple_visits": users_with_multiple_visits,
        "visits_last_24h": recent_visits
    }


async def cleanup_old_tracking(
    session: AsyncSession,
    days: int = 30
) -> int:
    """Удаляет старые записи отслеживания без конверсий"""
    from sqlalchemy import delete
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    
    # Удаляем только те записи, у которых нет конверсий
    stmt = (
        delete(YandexTracking)
        .where(
            and_(
                YandexTracking.last_visit_time < cutoff_date,
                ~YandexTracking.user_id.in_(
                    select(YandexConversion.user_id)
                )
            )
        )
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.rowcount