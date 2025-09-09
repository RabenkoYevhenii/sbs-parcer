#!/usr/bin/env python3
"""
First Wave Messaging Campaign for SBC Summit 2025

Цель: Запустить первую волну сообщений по всем зарегистрированным на SBC,
кто еще не получил outreach, включая невалидированных (по position),
но относящихся к онлайн типу gaming_vertical.

Автор: Система автоматизации SBC Connect
"""

import os
import sys
import pandas as pd
import time
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Set, Tuple, Optional
import json
import logging
from pathlib import Path

# Добавляем путь к родительской директории для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем основной скрапер
from api_test import SBCAttendeesScraper


class FirstWaveCampaign:
    """Класс для управления первой волной сообщений"""

    def __init__(self):
        self.data_dir = Path("restricted/data")
        self.data_dir.mkdir(exist_ok=True)

        self.csv_file = self.data_dir / "SBC - Attendees.csv"
        self.campaign_log_file = self.data_dir / "first_wave_campaign_log.json"
        self.campaign_results_file = self.data_dir / "first_wave_results.csv"

        # Инициализируем скрапер
        self.scraper = None

        # Настройки кампании
        self.test_mode = True
        self.test_contacts_limit = 3
        self.delay_between_messages = (3, 7)  # секунды между сообщениями
        self.daily_limit = 0  # без лимитов (0 = неограниченно)
        self.use_multi_account = True  # Использовать несколько аккаунтов

        # Статистика
        self.stats = {
            "total_contacts_found": 0,
            "valid_contacts": 0,
            "already_contacted": 0,
            "excluded_contacts": 0,
            "messages_sent": 0,
            "messages_failed": 0,
            "test_mode": self.test_mode,
            "multi_account_used": self.use_multi_account,
        }

        # Настройка логирования
        self.setup_logging()

        # Шаблоны сообщений (из api_test.py)
        self.message_templates = [
            "Hello {name} !\nI'm thrilled to see you at the SBC Summit in Lisbon the following month! Before things get hectic, it's always a pleasure to connect with other iGaming experts.\nI speak on behalf of Flexify Finance, a company that specializes in smooth payments for high-risk industries. Visit us at Stand E613 if you're looking into new payment options or simply want to discuss innovation.\nWhat is your main objective or priority for the expo this year? I'd love to know what you're thinking about!",
            "Hi {name} !\nExcited to connect with fellow SBC Summit attendees! I'm representing Flexify Finance - we provide payment solutions specifically designed for iGaming and high-risk industries.\nWe'll be at Stand E613 during the summit in Lisbon. Would love to learn about your current payment challenges or discuss the latest trends in our industry.\nWhat brings you to SBC Summit this year? Any specific goals or connections you're hoping to make?",
            "Hello {name} !\nLooking forward to the SBC Summit in Lisbon! As someone in the iGaming space, I always enjoy connecting with industry professionals before the event buzz begins.\nI'm with Flexify Finance - we specialize in seamless payment processing for high-risk sectors. Feel free to stop by Stand E613 if you'd like to explore new payment innovations.\nWhat are you most excited about at this year's summit? Any particular sessions or networking goals?",
            "Hi {name}, looks like we'll both be at SBC Lisbon this month!\nAlways great to meet fellow iGaming pros before the chaos begins.\nI'm with Flexify Finance, a payments provider for high-risk verticals - you'll find us at Stand E613.\nOut of curiosity, what's your main focus at the expo this year ?",
        ]

    def setup_logging(self):
        """Настройка системы логирования"""
        log_file = self.data_dir / "first_wave_campaign.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def load_campaign_log(self) -> Dict:
        """Загружает лог предыдущих кампаний"""
        if self.campaign_log_file.exists():
            try:
                with open(self.campaign_log_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Не удалось загрузить лог кампании: {e}")

        return {
            "campaigns": [],
            "contacted_users": set(),
            "last_campaign_date": None,
        }

    def save_campaign_log(self, log_data: Dict):
        """Сохраняет лог кампании"""
        # Конвертируем set в list для JSON сериализации
        log_data_copy = log_data.copy()
        if "contacted_users" in log_data_copy:
            log_data_copy["contacted_users"] = list(
                log_data_copy["contacted_users"]
            )

        try:
            with open(self.campaign_log_file, "w", encoding="utf-8") as f:
                json.dump(log_data_copy, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Ошибка сохранения лога кампании: {e}")

    def load_attendees_data(self) -> pd.DataFrame:
        """Загружает данные участников из CSV"""
        if not self.csv_file.exists():
            raise FileNotFoundError(f"CSV файл не найден: {self.csv_file}")

        try:
            df = pd.read_csv(self.csv_file)
            self.logger.info(f"Загружено {len(df)} записей из CSV")
            return df
        except Exception as e:
            self.logger.error(f"Ошибка загрузки CSV: {e}")
            raise

    def filter_target_audience(
        self, df: pd.DataFrame, contacted_users: Set[str]
    ) -> pd.DataFrame:
        """Фильтрует целевую аудиторию согласно требованиям"""
        self.logger.info("Начинаем фильтрацию целевой аудитории...")

        original_count = len(df)
        self.stats["total_contacts_found"] = original_count

        # 1. Исключаем уже отправленные контакты (по колонке 'connected')
        df_filtered = df[
            (df["connected"].isna()) | (df["connected"] != "Sent")
        ].copy()

        excluded_already_sent = original_count - len(df_filtered)
        self.logger.info(
            f"Исключено уже отправленных: {excluded_already_sent}"
        )

        # 2. Исключаем контакты из лога предыдущих кампаний
        if contacted_users:
            df_filtered = df_filtered[
                ~df_filtered["user_id"].isin(contacted_users)
            ]
            excluded_from_log = (
                len(df) - excluded_already_sent - len(df_filtered)
            )
            self.logger.info(
                f"Исключено из лога кампаний: {excluded_from_log}"
            )

        # 3. Фильтрация по компаниям (новая логика)
        df_filtered, companies_processed = self._apply_company_filtering(
            df, df_filtered
        )

        # 4. Включаем онлайн gaming_vertical ИЛИ пустые gaming_vertical
        online_gaming_keywords = [
            "online",
            "casino",
            "sports betting",
            "poker",
            "slots",
            "bingo",
            "lottery",
            "fantasy sports",
            "esports",
        ]

        # Создаем маску для онлайн gaming_vertical
        online_mask = df_filtered["gaming_vertical"].str.contains(
            "|".join(online_gaming_keywords), case=False, na=False
        )

        # Создаем маску для пустых gaming_vertical
        empty_gaming_mask = (
            df_filtered["gaming_vertical"].isna()
            | (df_filtered["gaming_vertical"] == "")
            | (df_filtered["gaming_vertical"].str.strip() == "")
        )

        # Исключаем "land" based gaming
        land_mask = df_filtered["gaming_vertical"].str.contains(
            "land", case=False, na=False
        )

        # Финальная фильтрация: (онлайн ИЛИ пустые) И НЕ land
        df_filtered = df_filtered[
            (online_mask | empty_gaming_mask) & ~land_mask
        ]

        gaming_excluded = len(df) - excluded_already_sent - len(df_filtered)
        self.logger.info(f"Исключено по gaming_vertical: {gaming_excluded}")

        # 5. Убираем дубли по user_id
        before_dedup = len(df_filtered)
        df_filtered = df_filtered.drop_duplicates(
            subset=["user_id"], keep="first"
        )
        duplicates_removed = before_dedup - len(df_filtered)
        if duplicates_removed > 0:
            self.logger.info(f"Удалено дубликатов: {duplicates_removed}")

        # 6. Проверяем наличие обязательных полей
        df_filtered = df_filtered[
            df_filtered["user_id"].notna()
            & (df_filtered["user_id"] != "")
            & df_filtered["full_name"].notna()
            & (df_filtered["full_name"] != "")
        ]

        invalid_data = before_dedup - duplicates_removed - len(df_filtered)
        if invalid_data > 0:
            self.logger.info(f"Исключено с неполными данными: {invalid_data}")

        self.stats["valid_contacts"] = len(df_filtered)
        self.stats["already_contacted"] = excluded_already_sent
        self.stats["excluded_contacts"] = original_count - len(df_filtered)
        self.stats["companies_with_responses"] = companies_processed

        self.logger.info(
            f"Результат фильтрации: {len(df_filtered)} валидных контактов"
        )

        return df_filtered

    def _apply_company_filtering(
        self, df_original: pd.DataFrame, df_filtered: pd.DataFrame
    ) -> tuple:
        """Применяет фильтрацию на уровне компаний"""
        self.logger.info("Применяем фильтрацию по компаниям...")

        # Находим компании, где кто-то уже ответил (проверяем наличие "answer" в connected, case-insensitive)
        companies_with_answers = set()
        answered_df = df_original[
            df_original["connected"].str.contains(
                "answer", case=False, na=False
            )
        ]
        if not answered_df.empty:
            companies_with_answers = set(
                answered_df["company_name"].dropna().unique()
            )
            self.logger.info(
                f"Найдено компаний с ответами: {len(companies_with_answers)}"
            )

        # Находим компании, где мы отправляли, но не получили ответа
        companies_contacted_no_answer = set()
        sent_no_answer_df = df_original[
            (df_original["connected"] == "Sent")
            & (~df_original["company_name"].isin(companies_with_answers))
        ]
        if not sent_no_answer_df.empty:
            companies_contacted_no_answer = set(
                sent_no_answer_df["company_name"].dropna().unique()
            )
            self.logger.info(
                f"Найдено компаний с отправкой но без ответа: {len(companies_contacted_no_answer)}"
            )

        # Исключаем контакты из компаний, где уже есть ответы
        before_company_filter = len(df_filtered)
        contacts_to_mark = df_filtered[
            df_filtered["company_name"].isin(companies_with_answers)
        ]

        if not contacts_to_mark.empty:
            # Обновляем статус в оригинальном DataFrame
            mask = df_original["user_id"].isin(contacts_to_mark["user_id"])
            df_original.loc[mask, "connected"] = "contacted with other worker"

            # Сохраняем изменения в CSV
            try:
                df_original.to_csv(self.csv_file, index=False)
                self.logger.info(
                    f"Обновлен статус для {len(contacts_to_mark)} контактов: 'contacted with other worker'"
                )
            except Exception as e:
                self.logger.error(f"Ошибка сохранения CSV: {e}")

            # Исключаем эти контакты из обработки
            df_filtered = df_filtered[
                ~df_filtered["company_name"].isin(companies_with_answers)
            ]

        excluded_by_company = before_company_filter - len(df_filtered)
        if excluded_by_company > 0:
            self.logger.info(
                f"Исключено по компаниям с ответами: {excluded_by_company}"
            )

        return df_filtered, len(companies_with_answers)

        return df_filtered, len(companies_with_answers)

    def prepare_message_data(self, df: pd.DataFrame) -> List[Dict]:
        """Подготавливает данные для отправки сообщений"""
        message_data = []

        for _, row in df.iterrows():
            # Получаем первое имя для персонализации
            full_name = str(row["full_name"]).strip()
            first_name = full_name.split()[0] if full_name.split() else "there"

            # Выбираем случайный шаблон сообщения
            template = random.choice(self.message_templates)
            message = template.format(name=first_name)

            message_data.append(
                {
                    "user_id": row["user_id"],
                    "full_name": full_name,
                    "first_name": first_name,
                    "message": message,
                    "position": row.get("position", ""),
                    "company": row.get("company_name", ""),
                    "gaming_vertical": row.get("gaming_vertical", ""),
                    "source_url": row.get("source_url", ""),
                    "row_index": row.name,
                }
            )

        return message_data

    def initialize_scraper(self) -> bool:
        """Инициализирует скрапер для отправки сообщений"""
        try:
            self.scraper = SBCAttendeesScraper(headless=True)

            # Запускаем браузер и логинимся с первым messenger аккаунтом
            if self.scraper.start():
                # Переключаемся на messenger1 для отправки сообщений
                if self.scraper.switch_account("messenger1"):
                    self.logger.info(
                        "Скрапер успешно инициализирован и залогинен с первым messenger аккаунтом"
                    )
                    return True
                else:
                    self.logger.error("Ошибка переключения на messenger1")
                    return False
            else:
                self.logger.error("Ошибка логина в скрапер")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка инициализации скрапера: {e}")
            return False

    def send_test_messages(self, message_data: List[Dict]) -> bool:
        """Отправляет тестовые сообщения для проверки с мультиаккаунтом"""
        self.logger.info(
            f"Отправка тестовых сообщений ({self.test_contacts_limit} контактов)"
        )

        test_contacts = message_data[: self.test_contacts_limit]

        # Применяем ту же логику распределения 50/50 что и в полной кампании
        if len(test_contacts) <= 1:
            # Если только 1 контакт, отправляем с messenger1
            messenger1_contacts = test_contacts
            messenger2_contacts = []
        else:
            # Разделяем поровну
            mid_point = len(test_contacts) // 2
            messenger1_contacts = test_contacts[:mid_point]
            messenger2_contacts = test_contacts[mid_point:]

        self.logger.info(f"📊 Распределение тестовых контактов:")
        self.logger.info(f"  Messenger1: {len(messenger1_contacts)} контактов")
        self.logger.info(f"  Messenger2: {len(messenger2_contacts)} контактов")

        total_sent = 0

        # Отправляем с messenger1
        if messenger1_contacts:
            self.logger.info("🔄 Тестовые сообщения с Messenger1...")
            for i, contact in enumerate(messenger1_contacts, 1):
                self.logger.info(
                    f"[ТЕСТ messenger1 {i}/{len(messenger1_contacts)}] Отправка сообщения: {contact['full_name']}"
                )

                try:
                    result = self.scraper.send_message_to_user(
                        contact["user_id"],
                        contact["message"],
                        contact["full_name"],
                    )

                    if result == "success":
                        self.logger.info(
                            f"✅ Тестовое сообщение отправлено (messenger1): {contact['full_name']}"
                        )
                        self.stats["messages_sent"] += 1
                        total_sent += 1
                    elif result == "already_contacted":
                        self.logger.info(
                            f"⏭️ Контакт уже обработан (messenger1): {contact['full_name']}"
                        )
                        self.stats["messages_sent"] += 1
                        total_sent += 1
                    else:  # result == "failed"
                        self.logger.warning(
                            f"❌ Ошибка отправки тестового сообщения: {contact['full_name']}"
                        )
                        self.stats["messages_failed"] += 1

                    # Задержка между сообщениями
                    if i < len(messenger1_contacts) or messenger2_contacts:
                        delay = random.uniform(*self.delay_between_messages)
                        self.logger.info(
                            f"Задержка {delay:.1f}с перед следующим сообщением..."
                        )
                        time.sleep(delay)

                except Exception as e:
                    self.logger.error(
                        f"Ошибка отправки тестового сообщения: {e}"
                    )
                    self.stats["messages_failed"] += 1

        # Переключаемся на messenger2 и отправляем остальные
        if messenger2_contacts:
            self.logger.info(
                "🔄 Переключаемся на Messenger2 для оставшихся тестовых сообщений..."
            )

            try:
                if not self.scraper.switch_account("messenger2"):
                    self.logger.error(
                        "Не удалось переключиться на messenger2 для тестов"
                    )
                    return False

                for i, contact in enumerate(messenger2_contacts, 1):
                    self.logger.info(
                        f"[ТЕСТ messenger2 {i}/{len(messenger2_contacts)}] Отправка сообщения: {contact['full_name']}"
                    )

                    try:
                        result = self.scraper.send_message_to_user(
                            contact["user_id"],
                            contact["message"],
                            contact["full_name"],
                        )

                        if result == "success":
                            self.logger.info(
                                f"✅ Тестовое сообщение отправлено (messenger2): {contact['full_name']}"
                            )
                            self.stats["messages_sent"] += 1
                            total_sent += 1
                        elif result == "already_contacted":
                            self.logger.info(
                                f"⏭️ Контакт уже обработан (messenger2): {contact['full_name']}"
                            )
                            self.stats["messages_sent"] += 1
                            total_sent += 1
                        else:  # result == "failed"
                            self.logger.warning(
                                f"❌ Ошибка отправки тестового сообщения: {contact['full_name']}"
                            )
                            self.stats["messages_failed"] += 1

                        # Задержка между сообщениями
                        if i < len(messenger2_contacts):
                            delay = random.uniform(
                                *self.delay_between_messages
                            )
                            self.logger.info(
                                f"Задержка {delay:.1f}с перед следующим сообщением..."
                            )
                            time.sleep(delay)

                    except Exception as e:
                        self.logger.error(
                            f"Ошибка отправки тестового сообщения: {e}"
                        )
                        self.stats["messages_failed"] += 1

            except Exception as e:
                self.logger.error(
                    f"Ошибка переключения на messenger2 в тестах: {e}"
                )
                return False

        # Результат теста
        success_rate = (
            (total_sent / len(test_contacts)) * 100
            if len(test_contacts) > 0
            else 0
        )
        self.logger.info(
            f"Результат теста: {total_sent}/{len(test_contacts)} успешно ({success_rate:.1f}%)"
        )

        if success_rate >= 66:  # Если успешность >= 66%
            return True
        else:
            self.logger.warning(
                "Низкая успешность тестовых сообщений. Рекомендуется проверить настройки."
            )
            return False
            return False

    def send_full_campaign(
        self, message_data: List[Dict], start_from: int = 0
    ) -> Dict:
        """Отправляет полную кампанию сообщений"""
        self.logger.info(
            f"Начинаем полную кампанию: {len(message_data)} контактов"
        )

        results = []
        sent_today = 0

        # Если это продолжение после теста, начинаем с нужного индекса
        contacts_to_send = message_data[start_from:]

        for i, contact in enumerate(contacts_to_send, start_from + 1):
            # Проверяем дневной лимит (если установлен)
            if self.daily_limit > 0 and sent_today >= self.daily_limit:
                self.logger.info(
                    f"Достигнут дневной лимит отправки: {self.daily_limit}"
                )
                break

            self.logger.info(
                f"[{i}/{len(message_data)}] Отправка: {contact['full_name']}"
            )

            try:
                # Отправляем сообщение
                send_result = self.scraper.send_message_to_user(
                    contact["user_id"],
                    contact["message"],
                    contact["full_name"],
                )

                # Записываем результат
                message_sent = send_result in ["success", "already_contacted"]
                result = {
                    "timestamp": datetime.now().isoformat(),
                    "user_id": contact["user_id"],
                    "full_name": contact["full_name"],
                    "position": contact["position"],
                    "company": contact["company"],
                    "gaming_vertical": contact["gaming_vertical"],
                    "message_sent": message_sent,
                    "status": send_result,
                    "error": None,
                    "account_used": (
                        self.scraper.current_account
                        if self.scraper
                        else "unknown"
                    ),
                }

                if send_result == "success":
                    self.logger.info(
                        f"✅ Сообщение отправлено: {contact['full_name']}"
                    )
                    self.stats["messages_sent"] += 1
                    sent_today += 1
                elif send_result == "already_contacted":
                    self.logger.info(
                        f"⏭️ Контакт уже обработан: {contact['full_name']}"
                    )
                    self.stats["messages_sent"] += 1
                    sent_today += 1
                else:  # send_result == "failed"
                    self.logger.warning(
                        f"❌ Ошибка отправки: {contact['full_name']}"
                    )
                    self.stats["messages_failed"] += 1
                    result["error"] = "Send failed"

                results.append(result)

                # Задержка между сообщениями
                if i < len(message_data):
                    delay = random.uniform(*self.delay_between_messages)
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(
                    f"Ошибка при отправке сообщения {contact['full_name']}: {e}"
                )
                self.stats["messages_failed"] += 1

                results.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "user_id": contact["user_id"],
                        "full_name": contact["full_name"],
                        "position": contact["position"],
                        "company": contact["company"],
                        "gaming_vertical": contact["gaming_vertical"],
                        "message_sent": False,
                        "error": str(e),
                        "account_used": (
                            self.scraper.current_account
                            if self.scraper
                            else "unknown"
                        ),
                    }
                )

        return {
            "results": results,
            "sent_today": sent_today,
            "completed": i >= len(message_data),
        }

    def send_full_campaign_multi_account(
        self, message_data: List[Dict], start_from: int = 0
    ) -> Dict:
        """Отправляет полную кампанию с использованием двух messenger аккаунтов"""
        self.logger.info(
            f"Начинаем мульти-аккаунтовую кампанию: {len(message_data)} контактов"
        )

        # Если это продолжение после теста, начинаем с нужного индекса
        contacts_to_send = message_data[start_from:]

        if len(contacts_to_send) == 0:
            self.logger.info("Нет контактов для отправки")
            return {"results": [], "sent_today": 0, "completed": True}

        # Разделяем контакты между двумя аккаунтами поровну (50%/50%)
        mid_point = len(contacts_to_send) // 2
        messenger1_contacts = contacts_to_send[:mid_point]
        messenger2_contacts = contacts_to_send[mid_point:]

        self.logger.info(f"Распределение контактов:")
        self.logger.info(f"  Messenger1: {len(messenger1_contacts)} контактов")
        self.logger.info(f"  Messenger2: {len(messenger2_contacts)} контактов")

        all_results = []
        total_sent = 0

        # Отправляем с первого аккаунта
        if messenger1_contacts:
            self.logger.info("🔄 Начинаем отправку с Messenger1 аккаунта...")
            try:
                # Убеждаемся что мы на правильном аккаунте
                if self.scraper.current_account != "messenger1":
                    if not self.scraper.switch_account("messenger1"):
                        self.logger.error(
                            "Не удалось переключиться на messenger1"
                        )
                        return {
                            "results": [],
                            "sent_today": 0,
                            "completed": False,
                        }

                # Загружаем чаты для текущего аккаунта
                self.scraper.load_chats_list()

                # Отправляем сообщения
                batch1_result = self._send_batch_with_account(
                    messenger1_contacts, "messenger1", start_from
                )
                all_results.extend(batch1_result["results"])
                total_sent += batch1_result["sent_today"]

                self.logger.info(
                    f"✅ Messenger1: отправлено {batch1_result['sent_today']} сообщений"
                )

            except Exception as e:
                self.logger.error(f"Ошибка при работе с messenger1: {e}")

        # Отправляем со второго аккаунта
        if messenger2_contacts:
            self.logger.info("🔄 Начинаем отправку с Messenger2 аккаунта...")
            try:
                # Переключаемся на второй аккаунт
                if not self.scraper.switch_account("messenger2"):
                    self.logger.error("Не удалось переключиться на messenger2")
                    return {
                        "results": all_results,
                        "sent_today": total_sent,
                        "completed": False,
                    }

                # Загружаем чаты для текущего аккаунта
                self.scraper.load_chats_list()

                # Отправляем сообщения
                batch2_start = start_from + len(messenger1_contacts)
                batch2_result = self._send_batch_with_account(
                    messenger2_contacts, "messenger2", batch2_start
                )
                all_results.extend(batch2_result["results"])
                total_sent += batch2_result["sent_today"]

                self.logger.info(
                    f"✅ Messenger2: отправлено {batch2_result['sent_today']} сообщений"
                )

            except Exception as e:
                self.logger.error(f"Ошибка при работе с messenger2: {e}")

        self.logger.info(
            f"📊 Мульти-аккаунтовая кампания завершена. Всего отправлено: {total_sent}"
        )

        return {
            "results": all_results,
            "sent_today": total_sent,
            "completed": True,
        }

    def _send_batch_with_account(
        self, contacts: List[Dict], account_name: str, start_index: int = 0
    ) -> Dict:
        """Отправляет батч сообщений с определенного аккаунта"""
        results = []
        sent_count = 0

        for i, contact in enumerate(contacts, start_index + 1):
            # Проверяем дневной лимит только если он установлен
            if self.daily_limit > 0 and sent_count >= (self.daily_limit // 2):
                self.logger.info(
                    f"Достигнут лимит для {account_name}: {self.daily_limit // 2}"
                )
                break

            self.logger.info(
                f"[{account_name}] [{i}] Отправка: {contact['full_name']}"
            )

            try:
                # Отправляем сообщение
                send_result = self.scraper.send_message_to_user(
                    contact["user_id"],
                    contact["message"],
                    contact["full_name"],
                )

                # Записываем результат
                message_sent = send_result in ["success", "already_contacted"]
                result = {
                    "timestamp": datetime.now().isoformat(),
                    "user_id": contact["user_id"],
                    "full_name": contact["full_name"],
                    "position": contact["position"],
                    "company": contact["company"],
                    "gaming_vertical": contact["gaming_vertical"],
                    "message_sent": message_sent,
                    "status": send_result,
                    "error": None,
                    "account_used": account_name,
                }

                if send_result == "success":
                    self.logger.info(
                        f"✅ [{account_name}] Сообщение отправлено: {contact['full_name']}"
                    )
                    self.stats["messages_sent"] += 1
                    sent_count += 1
                elif send_result == "already_contacted":
                    self.logger.info(
                        f"⏭️ [{account_name}] Контакт уже обработан: {contact['full_name']}"
                    )
                    self.stats["messages_sent"] += 1
                    sent_count += 1
                else:  # send_result == "failed"
                    self.logger.warning(
                        f"❌ [{account_name}] Ошибка отправки: {contact['full_name']}"
                    )
                    self.stats["messages_failed"] += 1
                    result["error"] = "Send failed"

                results.append(result)

                # Задержка между сообщениями
                if i < len(contacts):
                    delay = random.uniform(*self.delay_between_messages)
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(
                    f"[{account_name}] Ошибка при отправке сообщения {contact['full_name']}: {e}"
                )
                self.stats["messages_failed"] += 1

                results.append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "user_id": contact["user_id"],
                        "full_name": contact["full_name"],
                        "position": contact["position"],
                        "company": contact["company"],
                        "gaming_vertical": contact["gaming_vertical"],
                        "message_sent": False,
                        "error": str(e),
                        "account_used": account_name,
                    }
                )

        return {
            "results": results,
            "sent_today": sent_count,
            "completed": i >= len(contacts),
        }

    def save_campaign_results(self, results: List[Dict]):
        """Сохраняет результаты кампании в CSV"""
        try:
            df_results = pd.DataFrame(results)
            df_results.to_csv(
                self.campaign_results_file, index=False, encoding="utf-8"
            )
            self.logger.info(
                f"Результаты кампании сохранены: {self.campaign_results_file}"
            )
        except Exception as e:
            self.logger.error(f"Ошибка сохранения результатов: {e}")

    def update_main_csv(self, results: List[Dict]):
        """Обновляет основной CSV файл статусами отправки"""
        try:
            df = pd.read_csv(self.csv_file)

            # Создаем словарь для быстрого поиска результатов
            results_dict = {r["user_id"]: r for r in results}

            # Обновляем статусы
            for idx, row in df.iterrows():
                user_id = row["user_id"]
                if user_id in results_dict:
                    result = results_dict[user_id]
                    if result["message_sent"]:
                        df.at[idx, "connected"] = "Sent"
                        df.at[idx, "message_sent_date"] = result["timestamp"]

            # Сохраняем обновленный CSV
            df.to_csv(self.csv_file, index=False, encoding="utf-8")
            self.logger.info("Основной CSV файл обновлен статусами отправки")

        except Exception as e:
            self.logger.error(f"Ошибка обновления основного CSV: {e}")

    def generate_final_report(self, results: List[Dict]) -> Dict:
        """Генерирует итоговый отчет по кампании"""
        report = {
            "campaign_date": datetime.now().isoformat(),
            "mode": "test" if self.test_mode else "full",
            "statistics": self.stats.copy(),
            "success_rate": (
                (
                    self.stats["messages_sent"]
                    / (
                        self.stats["messages_sent"]
                        + self.stats["messages_failed"]
                    )
                    * 100
                )
                if (
                    self.stats["messages_sent"] + self.stats["messages_failed"]
                )
                > 0
                else 0
            ),
            "results_summary": {
                "total_processed": len(results),
                "successful_sends": sum(
                    1 for r in results if r["message_sent"]
                ),
                "failed_sends": sum(
                    1 for r in results if not r["message_sent"]
                ),
                "unique_companies": len(
                    set(r["company"] for r in results if r["company"])
                ),
                "gaming_verticals_covered": len(
                    set(
                        r["gaming_vertical"]
                        for r in results
                        if r["gaming_vertical"]
                    )
                ),
            },
        }

        return report

    def run_campaign(self, test_only: bool = False):
        """Основной метод запуска кампании"""
        self.logger.info("=" * 60)
        self.logger.info("ЗАПУСК ПЕРВОЙ ВОЛНЫ СООБЩЕНИЙ SBC SUMMIT 2025")
        self.logger.info("=" * 60)

        try:
            # 1. Загружаем данные
            self.logger.info("Этап 1: Загрузка данных участников")
            df = self.load_attendees_data()

            # 2. Загружаем лог предыдущих кампаний
            self.logger.info("Этап 2: Проверка предыдущих кампаний")
            campaign_log = self.load_campaign_log()
            contacted_users = set(campaign_log.get("contacted_users", []))

            # 3. Фильтруем целевую аудиторию
            self.logger.info("Этап 3: Фильтрация целевой аудитории")
            filtered_df = self.filter_target_audience(df, contacted_users)

            if len(filtered_df) == 0:
                self.logger.info("Нет контактов для отправки сообщений")
                return

            # 4. Подготавливаем данные для сообщений
            self.logger.info("Этап 4: Подготовка сообщений")
            message_data = self.prepare_message_data(filtered_df)

            if self.test_mode or test_only:
                message_data = message_data[: self.test_contacts_limit]
                self.logger.info(
                    f"ТЕСТОВЫЙ РЕЖИМ: ограничено до {len(message_data)} контактов"
                )

            # 5. Инициализируем скрапер
            self.logger.info("Этап 5: Инициализация системы отправки")
            if not self.initialize_scraper():
                raise Exception("Не удалось инициализировать скрапер")

            # 6. Отправляем сообщения
            all_results = []

            if self.test_mode and not test_only:
                # Сначала тест, потом полная отправка
                self.logger.info("Этап 6a: Тестовая отправка")
                if self.send_test_messages(message_data):
                    self.logger.info(
                        "Тест прошел успешно! Продолжаем с полной кампанией..."
                    )

                    # Переключаемся в полный режим
                    self.test_mode = False
                    self.stats["test_mode"] = False

                    # Загружаем полные данные и отправляем
                    full_message_data = self.prepare_message_data(filtered_df)

                    # Выбираем метод отправки
                    if self.use_multi_account and len(full_message_data) > 10:
                        self.logger.info(
                            "Используем мульти-аккаунтовую отправку"
                        )
                        campaign_result = (
                            self.send_full_campaign_multi_account(
                                full_message_data, self.test_contacts_limit
                            )
                        )
                    else:
                        self.logger.info("Используем одноаккаунтовую отправку")
                        campaign_result = self.send_full_campaign(
                            full_message_data, self.test_contacts_limit
                        )

                    all_results.extend(campaign_result["results"])
                else:
                    self.logger.warning("Тест не прошел. Остановка кампании.")
                    return
            else:
                # Только тест или только полная отправка
                self.logger.info("Этап 6: Отправка сообщений")
                if test_only:
                    self.send_test_messages(message_data)
                else:
                    # Выбираем метод отправки
                    if self.use_multi_account and len(message_data) > 10:
                        self.logger.info(
                            "Используем мульти-аккаунтовую отправку"
                        )
                        campaign_result = (
                            self.send_full_campaign_multi_account(message_data)
                        )
                    else:
                        self.logger.info("Используем одноаккаунтовую отправку")
                        campaign_result = self.send_full_campaign(message_data)

                    all_results.extend(campaign_result["results"])

            # 7. Сохраняем результаты
            if all_results:
                self.logger.info("Этап 7: Сохранение результатов")
                self.save_campaign_results(all_results)
                self.update_main_csv(all_results)

                # Обновляем лог кампаний
                successful_contacts = {
                    r["user_id"] for r in all_results if r["message_sent"]
                }
                campaign_log["contacted_users"].update(successful_contacts)
                campaign_log["last_campaign_date"] = datetime.now().isoformat()
                campaign_log["campaigns"].append(
                    {
                        "date": datetime.now().isoformat(),
                        "contacts_sent": len(successful_contacts),
                        "mode": "test" if test_only else "full",
                    }
                )
                self.save_campaign_log(campaign_log)

            # 8. Генерируем отчет
            self.logger.info("Этап 8: Генерация отчета")
            report = self.generate_final_report(all_results)

            # Выводим итоговый отчет
            self.logger.info("=" * 60)
            self.logger.info("ИТОГОВЫЙ ОТЧЕТ КАМПАНИИ")
            self.logger.info("=" * 60)
            self.logger.info(
                f"📊 Всего контактов найдено: {self.stats['total_contacts_found']}"
            )
            self.logger.info(
                f"📊 Валидных контактов: {self.stats['valid_contacts']}"
            )
            self.logger.info(
                f"📊 Уже связались ранее: {self.stats['already_contacted']}"
            )
            self.logger.info(
                f"📊 Исключено по фильтрам: {self.stats['excluded_contacts']}"
            )
            self.logger.info(
                f"✅ Сообщений отправлено: {self.stats['messages_sent']}"
            )
            self.logger.info(
                f"❌ Ошибок отправки: {self.stats['messages_failed']}"
            )
            self.logger.info(f"📈 Успешность: {report['success_rate']:.1f}%")
            self.logger.info(
                f"🏢 Уникальных компаний: {report['results_summary']['unique_companies']}"
            )
            self.logger.info(
                f"🎮 Gaming verticals: {report['results_summary']['gaming_verticals_covered']}"
            )
            self.logger.info(
                f"👥 Мульти-аккаунт: {'Да' if self.use_multi_account else 'Нет'}"
            )

            # Статистика по аккаунтам если использовался мульти-аккаунт
            if all_results and self.use_multi_account:
                account_stats = {}
                for result in all_results:
                    account = result.get("account_used", "unknown")
                    if account not in account_stats:
                        account_stats[account] = {"sent": 0, "failed": 0}
                    if result.get("message_sent"):
                        account_stats[account]["sent"] += 1
                    else:
                        account_stats[account]["failed"] += 1

                self.logger.info("📊 Статистика по аккаунтам:")
                for account, stats in account_stats.items():
                    total = stats["sent"] + stats["failed"]
                    success_rate = (
                        (stats["sent"] / total * 100) if total > 0 else 0
                    )
                    self.logger.info(
                        f"   {account}: {stats['sent']}/{total} ({success_rate:.1f}%)"
                    )

            # Сохраняем отчет
            report_file = (
                self.data_dir
                / f"first_wave_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            self.logger.info(f"📋 Подробный отчет сохранен: {report_file}")
            self.logger.info("=" * 60)
            self.logger.info("КАМПАНИЯ ЗАВЕРШЕНА")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Критическая ошибка в кампании: {e}")
            raise

        finally:
            # Закрываем скрапер
            if self.scraper:
                self.scraper.close()


def main():
    """Главная функция для запуска кампании"""
    import argparse

    parser = argparse.ArgumentParser(
        description="First Wave Messaging Campaign for SBC Summit 2025"
    )
    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Запустить только тестовые сообщения",
    )
    parser.add_argument(
        "--full-only",
        action="store_true",
        help="Запустить только полную кампанию (без теста)",
    )
    parser.add_argument(
        "--test-limit",
        type=int,
        default=3,
        help="Количество тестовых контактов (по умолчанию: 3)",
    )
    parser.add_argument(
        "--single-account",
        action="store_true",
        help="Использовать только один аккаунт (отключить мульти-аккаунт)",
    )
    parser.add_argument(
        "--daily-limit",
        type=int,
        default=0,
        help="Дневной лимит сообщений (0 = без лимитов, по умолчанию: 0)",
    )

    args = parser.parse_args()

    # Создаем и настраиваем кампанию
    campaign = FirstWaveCampaign()

    if args.test_limit:
        campaign.test_contacts_limit = args.test_limit

    if args.full_only:
        campaign.test_mode = False

    if args.single_account:
        campaign.use_multi_account = False

    if args.daily_limit:
        campaign.daily_limit = args.daily_limit

    try:
        # Запускаем кампанию
        campaign.run_campaign(test_only=args.test_only)

    except KeyboardInterrupt:
        print("\n⏹️ Кампания остановлена пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка выполнения кампании: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
