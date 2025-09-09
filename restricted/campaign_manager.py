#!/usr/bin/env python3
"""
Campaign Management Utility for SBC Summit 2025
Утилита для управления и мониторинга кампаний сообщений

Функции:
- Мониторинг статуса кампаний
- Анализ результатов
- Управление CSV данными
- Генерация отчетов
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import argparse

# Добавляем путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from campaign_config import *


class CampaignManager:
    """Класс для управления кампаниями"""

    def __init__(self):
        self.data_dir = Path("restricted/data")
        self.csv_file = self.data_dir / "SBC - Attendees.csv"
        self.campaign_log_file = self.data_dir / "first_wave_campaign_log.json"

    def load_campaign_log(self) -> Dict:
        """Загружает лог кампаний"""
        if self.campaign_log_file.exists():
            try:
                with open(self.campaign_log_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Ошибка загрузки лога: {e}")
        return {"campaigns": [], "contacted_users": []}

    def analyze_csv_data(self) -> Dict:
        """Анализирует данные в основном CSV файле"""
        if not self.csv_file.exists():
            return {"error": "CSV файл не найден"}

        try:
            df = pd.read_csv(self.csv_file)

            analysis = {
                "total_contacts": len(df),
                "contacted_count": len(df[df["connected"] == "Sent"]),
                "pending_contacts": len(
                    df[(df["connected"].isna()) | (df["connected"] != "Sent")]
                ),
                "with_responses": len(df[df["connected"] == "Sent Answer"]),
                "gaming_verticals": df["gaming_vertical"]
                .value_counts()
                .to_dict(),
                "top_companies": df["company_name"]
                .value_counts()
                .head(10)
                .to_dict(),
                "positions_distribution": df["position"]
                .value_counts()
                .head(15)
                .to_dict(),
                "countries": (
                    df["country"].value_counts().head(10).to_dict()
                    if "country" in df.columns
                    else {}
                ),
                "contact_quality": {
                    "with_phone": (
                        len(df[df["other_contacts"].notna()])
                        if "other_contacts" in df.columns
                        else 0
                    ),
                    "with_email": (
                        len(
                            df[
                                df["other_contacts"].str.contains(
                                    "@", na=False
                                )
                            ]
                        )
                        if "other_contacts" in df.columns
                        else 0
                    ),
                },
            }

            # Анализ качества данных
            analysis["data_quality"] = {
                "complete_profiles": len(
                    df.dropna(subset=["full_name", "user_id"])
                ),
                "missing_positions": len(df[df["position"].isna()]),
                "missing_companies": len(df[df["company_name"].isna()]),
                "missing_gaming_vertical": len(
                    df[df["gaming_vertical"].isna()]
                ),
            }

            return analysis

        except Exception as e:
            return {"error": f"Ошибка анализа CSV: {e}"}

    def get_campaign_statistics(self) -> Dict:
        """Получает статистику по всем кампаниям"""
        campaign_log = self.load_campaign_log()

        stats = {
            "total_campaigns": len(campaign_log.get("campaigns", [])),
            "total_contacted": len(campaign_log.get("contacted_users", [])),
            "last_campaign": None,
            "campaign_history": [],
        }

        campaigns = campaign_log.get("campaigns", [])
        if campaigns:
            # Сортируем по дате
            sorted_campaigns = sorted(
                campaigns, key=lambda x: x.get("date", ""), reverse=True
            )
            stats["last_campaign"] = sorted_campaigns[0]
            stats["campaign_history"] = sorted_campaigns

        return stats

    def identify_target_audience(self, apply_filters: bool = True) -> Dict:
        """Идентифицирует целевую аудиторию для новой кампании"""
        if not self.csv_file.exists():
            return {"error": "CSV файл не найден"}

        try:
            df = pd.read_csv(self.csv_file)
            campaign_log = self.load_campaign_log()
            contacted_users = set(campaign_log.get("contacted_users", []))

            # Анализ компаний
            companies_analysis = self._analyze_companies(df)

            # Базовая фильтрация
            available = df[
                (df["connected"].isna()) | (df["connected"] != "Sent")
            ].copy()

            # Исключаем уже связанных из лога
            if contacted_users:
                available = available[
                    ~available["user_id"].isin(contacted_users)
                ]

            # Применяем фильтрацию по компаниям
            companies_with_answers = set()
            answered_df = df[
                df["connected"].str.contains("answer", case=False, na=False)
            ]
            if not answered_df.empty:
                companies_with_answers = set(
                    answered_df["company_name"].dropna().unique()
                )

            # Исключаем контакты из компаний где уже есть ответы
            available = available[
                ~available["company_name"].isin(companies_with_answers)
            ]

            if apply_filters:
                # Применяем фильтры из конфигурации

                # Фильтр по gaming_vertical
                online_mask = available["gaming_vertical"].str.contains(
                    "|".join(ONLINE_GAMING_KEYWORDS), case=False, na=False
                )
                empty_gaming_mask = (
                    available["gaming_vertical"].isna()
                    | (available["gaming_vertical"] == "")
                    | (available["gaming_vertical"].str.strip() == "")
                )
                land_mask = available["gaming_vertical"].str.contains(
                    "|".join(EXCLUDED_GAMING_KEYWORDS), case=False, na=False
                )

                available = available[
                    (online_mask | empty_gaming_mask) & ~land_mask
                ]

            # Убираем дубли
            available = available.drop_duplicates(
                subset=["user_id"], keep="first"
            )

            # Проверяем обязательные поля
            available = available[
                available["user_id"].notna()
                & (available["user_id"] != "")
                & available["full_name"].notna()
                & (available["full_name"] != "")
            ]

            # Анализируем приоритетные контакты
            priority_contacts = (
                available[
                    available["position"].str.contains(
                        "|".join(PRIORITY_POSITIONS), case=False, na=False
                    )
                ]
                if "position" in available.columns
                else pd.DataFrame()
            )

            result = {
                "total_available": len(available),
                "priority_contacts": len(priority_contacts),
                "regular_contacts": len(available) - len(priority_contacts),
                "gaming_verticals": available["gaming_vertical"]
                .value_counts()
                .to_dict(),
                "top_positions": (
                    available["position"].value_counts().head(10).to_dict()
                    if "position" in available.columns
                    else {}
                ),
                "top_companies": (
                    available["company_name"].value_counts().head(10).to_dict()
                    if "company_name" in available.columns
                    else {}
                ),
                "filters_applied": apply_filters,
                "contacted_previously": len(contacted_users),
                "companies_with_answers": len(companies_with_answers),
                "companies_analysis": companies_analysis,
            }

            return result

        except Exception as e:
            return {"error": f"Ошибка анализа целевой аудитории: {e}"}

    def _analyze_companies(self, df: pd.DataFrame) -> Dict:
        """Анализирует статус компаний"""
        try:
            # Компании с ответами (проверяем наличие "answer" в connected, case-insensitive)
            companies_answered = set()
            answered_df = df[
                df["connected"].str.contains("answer", case=False, na=False)
            ]
            if not answered_df.empty:
                companies_answered = set(
                    answered_df["company_name"].dropna().unique()
                )

            # Компании с отправкой но без ответа
            companies_sent_no_answer = set()
            sent_df = df[df["connected"] == "Sent"]
            if not sent_df.empty:
                companies_sent = set(sent_df["company_name"].dropna().unique())
                companies_sent_no_answer = companies_sent - companies_answered

            # Компании помеченные как "contacted with other worker"
            companies_marked = set()
            marked_df = df[df["connected"] == "contacted with other worker"]
            if not marked_df.empty:
                companies_marked = set(
                    marked_df["company_name"].dropna().unique()
                )

            # Непроцессированные компании
            processed_companies = (
                companies_answered
                | companies_sent_no_answer
                | companies_marked
            )
            all_companies = set(df["company_name"].dropna().unique())
            companies_unprocessed = all_companies - processed_companies

            return {
                "total_companies": len(all_companies),
                "companies_answered": len(companies_answered),
                "companies_sent_no_answer": len(companies_sent_no_answer),
                "companies_marked_contacted": len(companies_marked),
                "companies_unprocessed": len(companies_unprocessed),
                "companies_answered_list": list(companies_answered)[
                    :10
                ],  # Показываем первые 10
                "companies_sent_no_answer_list": list(
                    companies_sent_no_answer
                )[:10],
            }
        except Exception as e:
            return {"error": f"Ошибка анализа компаний: {e}"}

    def generate_campaign_report(self, detailed: bool = False) -> str:
        """Генерирует отчет по кампаниям"""
        print("=" * 80)
        print("ОТЧЕТ ПО КАМПАНИЯМ SBC SUMMIT 2025")
        print("=" * 80)

        # Анализ CSV данных
        csv_analysis = self.analyze_csv_data()
        if "error" in csv_analysis:
            print(f"❌ Ошибка анализа данных: {csv_analysis['error']}")
            return

        print(f"\n📊 ОБЩАЯ СТАТИСТИКА БАЗЫ ДАННЫХ")
        print(f"   Всего контактов: {csv_analysis['total_contacts']:,}")
        print(f"   Отправлено сообщений: {csv_analysis['contacted_count']:,}")
        print(f"   Получены ответы: {csv_analysis['with_responses']:,}")
        print(f"   Ожидают отправки: {csv_analysis['pending_contacts']:,}")

        if csv_analysis["contacted_count"] > 0:
            response_rate = (
                csv_analysis["with_responses"]
                / csv_analysis["contacted_count"]
            ) * 100
            print(f"   Процент ответов: {response_rate:.1f}%")

        # Статистика кампаний
        campaign_stats = self.get_campaign_statistics()
        print(f"\n📬 СТАТИСТИКА КАМПАНИЙ")
        print(f"   Проведено кампаний: {campaign_stats['total_campaigns']}")
        print(f"   Всего связались: {campaign_stats['total_contacted']:,}")

        if campaign_stats["last_campaign"]:
            last_campaign = campaign_stats["last_campaign"]
            print(f"   Последняя кампания: {last_campaign.get('date', 'N/A')}")
            print(
                f"   Контактов в последней: {last_campaign.get('contacts_sent', 0)}"
            )

        # Целевая аудитория
        target_analysis = self.identify_target_audience()
        if "error" not in target_analysis:
            print(f"\n🎯 ДОСТУПНАЯ ЦЕЛЕВАЯ АУДИТОРИЯ")
            print(
                f"   Доступно для отправки: {target_analysis['total_available']:,}"
            )
            print(
                f"   Приоритетные контакты: {target_analysis['priority_contacts']:,}"
            )
            print(
                f"   Обычные контакты: {target_analysis['regular_contacts']:,}"
            )

            # Статистика по компаниям
            if "companies_analysis" in target_analysis:
                comp_stats = target_analysis["companies_analysis"]
                if "error" not in comp_stats:
                    print(f"\n🏢 СТАТИСТИКА ПО КОМПАНИЯМ")
                    print(
                        f"   Всего компаний: {comp_stats['total_companies']:,}"
                    )
                    print(
                        f"   С ответами (исключены): {comp_stats['companies_answered']:,}"
                    )
                    print(
                        f"   Отправлено без ответа: {comp_stats['companies_sent_no_answer']:,}"
                    )
                    print(
                        f"   Помечены как обработанные: {comp_stats['companies_marked_contacted']:,}"
                    )
                    print(
                        f"   Доступны для обработки: {comp_stats['companies_unprocessed']:,}"
                    )

                    if detailed and comp_stats["companies_answered"] > 0:
                        print(f"\n📞 КОМПАНИИ С ОТВЕТАМИ (первые 10):")
                        for company in comp_stats["companies_answered_list"][
                            :10
                        ]:
                            print(f"   • {company}")

                    if detailed and comp_stats["companies_sent_no_answer"] > 0:
                        print(f"\n⏳ КОМПАНИИ БЕЗ ОТВЕТА (первые 10):")
                        for company in comp_stats[
                            "companies_sent_no_answer_list"
                        ][:10]:
                            print(f"   • {company}")

        # Детальная информация
        if detailed:
            print(f"\n🏢 ТОП КОМПАНИИ ({len(csv_analysis['top_companies'])})")
            for company, count in list(csv_analysis["top_companies"].items())[
                :10
            ]:
                print(f"   {company}: {count}")

            print(
                f"\n💼 ТОП ПОЗИЦИИ ({len(csv_analysis['positions_distribution'])})"
            )
            for position, count in list(
                csv_analysis["positions_distribution"].items()
            )[:10]:
                print(f"   {position}: {count}")

            print(
                f"\n🎮 GAMING VERTICALS ({len(csv_analysis['gaming_verticals'])})"
            )
            for vertical, count in list(
                csv_analysis["gaming_verticals"].items()
            )[:10]:
                print(f"   {vertical}: {count}")

            if csv_analysis["countries"]:
                print(f"\n🌍 ТОП СТРАНЫ ({len(csv_analysis['countries'])})")
                for country, count in list(csv_analysis["countries"].items())[
                    :10
                ]:
                    print(f"   {country}: {count}")

        # Качество данных
        print(f"\n📋 КАЧЕСТВО ДАННЫХ")
        quality = csv_analysis["data_quality"]
        print(f"   Полные профили: {quality['complete_profiles']:,}")
        print(f"   Без позиции: {quality['missing_positions']:,}")
        print(f"   Без компании: {quality['missing_companies']:,}")
        print(
            f"   Без gaming vertical: {quality['missing_gaming_vertical']:,}"
        )

        contact_quality = csv_analysis["contact_quality"]
        print(f"   С контактными данными: {contact_quality['with_phone']:,}")
        print(f"   С email: {contact_quality['with_email']:,}")

        print("=" * 80)

        return "Отчет сгенерирован успешно"

    def export_target_audience(
        self, output_file: str = None, apply_filters: bool = True
    ) -> str:
        """Экспортирует целевую аудиторию в отдельный CSV"""
        if not self.csv_file.exists():
            return "CSV файл не найден"

        try:
            df = pd.read_csv(self.csv_file)
            campaign_log = self.load_campaign_log()
            contacted_users = set(campaign_log.get("contacted_users", []))

            # Применяем фильтрацию как в основной кампании
            available = df[
                (df["connected"].isna()) | (df["connected"] != "Sent")
            ].copy()

            if contacted_users:
                available = available[
                    ~available["user_id"].isin(contacted_users)
                ]

            if apply_filters:
                # Фильтры по gaming_vertical
                online_mask = available["gaming_vertical"].str.contains(
                    "|".join(ONLINE_GAMING_KEYWORDS), case=False, na=False
                )
                empty_gaming_mask = (
                    available["gaming_vertical"].isna()
                    | (available["gaming_vertical"] == "")
                    | (available["gaming_vertical"].str.strip() == "")
                )
                land_mask = available["gaming_vertical"].str.contains(
                    "|".join(EXCLUDED_GAMING_KEYWORDS), case=False, na=False
                )

                available = available[
                    (online_mask | empty_gaming_mask) & ~land_mask
                ]

            # Убираем дубли и проверяем обязательные поля
            available = available.drop_duplicates(
                subset=["user_id"], keep="first"
            )
            available = available[
                available["user_id"].notna()
                & (available["user_id"] != "")
                & available["full_name"].notna()
                & (available["full_name"] != "")
            ]

            # Сортируем: сначала приоритетные, потом остальные
            if "position" in available.columns:
                priority_mask = available["position"].str.contains(
                    "|".join(PRIORITY_POSITIONS), case=False, na=False
                )
                priority_contacts = available[priority_mask].copy()
                regular_contacts = available[~priority_mask].copy()

                # Добавляем флаг приоритета
                priority_contacts["is_priority"] = True
                regular_contacts["is_priority"] = False

                # Объединяем: приоритетные сверху
                available = pd.concat(
                    [priority_contacts, regular_contacts], ignore_index=True
                )

            # Определяем имя выходного файла
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = (
                    self.data_dir / f"target_audience_{timestamp}.csv"
                )
            else:
                output_file = Path(output_file)

            # Сохраняем
            available.to_csv(output_file, index=False, encoding="utf-8")

            return f"Экспортировано {len(available)} контактов в {output_file}"

        except Exception as e:
            return f"Ошибка экспорта: {e}"

    def cleanup_duplicates(self, dry_run: bool = True) -> str:
        """Очищает дубликаты в основном CSV файле"""
        if not self.csv_file.exists():
            return "CSV файл не найден"

        try:
            df = pd.read_csv(self.csv_file)
            original_count = len(df)

            # Находим дубликаты по user_id
            duplicates = df[df.duplicated(subset=["user_id"], keep=False)]
            unique_user_duplicates = duplicates["user_id"].nunique()

            if len(duplicates) == 0:
                return "Дубликаты не найдены"

            print(
                f"Найдено {len(duplicates)} дублирующихся записей для {unique_user_duplicates} уникальных user_id"
            )

            if dry_run:
                print("РЕЖИМ ПРОСМОТРА - изменения не будут сохранены")
                print("\nПримеры дубликатов:")
                for user_id in duplicates["user_id"].unique()[:5]:
                    user_dupes = duplicates[duplicates["user_id"] == user_id]
                    print(f"\nUser ID: {user_id}")
                    for _, row in user_dupes.iterrows():
                        print(
                            f"  - {row['full_name']} | {row.get('company_name', 'N/A')} | {row.get('position', 'N/A')}"
                        )

                return f"Найдено {len(duplicates)} дубликатов. Запустите с --no-dry-run для удаления."

            # Удаляем дубликаты, оставляя первый
            df_cleaned = df.drop_duplicates(subset=["user_id"], keep="first")
            removed_count = original_count - len(df_cleaned)

            # Создаем бэкап
            backup_file = self.csv_file.with_suffix(".backup.csv")
            df.to_csv(backup_file, index=False, encoding="utf-8")

            # Сохраняем очищенный файл
            df_cleaned.to_csv(self.csv_file, index=False, encoding="utf-8")

            return f"Удалено {removed_count} дубликатов. Бэкап сохранен в {backup_file}"

        except Exception as e:
            return f"Ошибка очистки дубликатов: {e}"

    def check_campaign_readiness(self) -> Dict:
        """Проверяет готовность к запуску кампании"""
        checks = {
            "csv_exists": self.csv_file.exists(),
            "csv_readable": False,
            "has_target_audience": False,
            "working_time": is_working_time(),
            "config_valid": False,
            "recommendations": [],
        }

        try:
            # Проверяем CSV
            if checks["csv_exists"]:
                df = pd.read_csv(self.csv_file)
                checks["csv_readable"] = True

                # Проверяем целевую аудиторию
                target_analysis = self.identify_target_audience()
                if "error" not in target_analysis:
                    checks["has_target_audience"] = (
                        target_analysis["total_available"] > 0
                    )

                    if target_analysis["total_available"] < 10:
                        checks["recommendations"].append(
                            "Мало доступных контактов для кампании"
                        )

                    if target_analysis["priority_contacts"] > 0:
                        checks["recommendations"].append(
                            f"Найдено {target_analysis['priority_contacts']} приоритетных контактов"
                        )
            else:
                checks["recommendations"].append("CSV файл не найден")

            # Проверяем конфигурацию
            try:
                validate_config()
                checks["config_valid"] = True
            except ValueError as e:
                checks["recommendations"].append(f"Ошибка конфигурации: {e}")

            # Проверяем рабочее время
            if not checks["working_time"]:
                checks["recommendations"].append(
                    "Сейчас нерабочее время согласно настройкам"
                )

            # Общая готовность
            checks["ready"] = all(
                [
                    checks["csv_exists"],
                    checks["csv_readable"],
                    checks["has_target_audience"],
                    checks["config_valid"],
                ]
            )

        except Exception as e:
            checks["recommendations"].append(f"Ошибка проверки: {e}")

        return checks


def main():
    """Главная функция CLI утилиты"""
    parser = argparse.ArgumentParser(description="Campaign Management Utility")

    subparsers = parser.add_subparsers(
        dest="command", help="Доступные команды"
    )

    # Команда отчета
    report_parser = subparsers.add_parser("report", help="Генерация отчета")
    report_parser.add_argument(
        "--detailed", action="store_true", help="Детальный отчет"
    )

    # Команда экспорта целевой аудитории
    export_parser = subparsers.add_parser(
        "export", help="Экспорт целевой аудитории"
    )
    export_parser.add_argument("--output", "-o", help="Выходной файл")
    export_parser.add_argument(
        "--no-filters", action="store_true", help="Без применения фильтров"
    )

    # Команда очистки дубликатов
    cleanup_parser = subparsers.add_parser(
        "cleanup", help="Очистка дубликатов"
    )
    cleanup_parser.add_argument(
        "--no-dry-run", action="store_true", help="Выполнить очистку"
    )

    # Команда проверки готовности
    check_parser = subparsers.add_parser(
        "check", help="Проверка готовности к кампании"
    )

    # Команда анализа
    analyze_parser = subparsers.add_parser("analyze", help="Анализ данных")
    analyze_parser.add_argument(
        "--target", action="store_true", help="Анализ целевой аудитории"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    manager = CampaignManager()

    try:
        if args.command == "report":
            manager.generate_campaign_report(detailed=args.detailed)

        elif args.command == "export":
            result = manager.export_target_audience(
                output_file=args.output, apply_filters=not args.no_filters
            )
            print(result)

        elif args.command == "cleanup":
            result = manager.cleanup_duplicates(dry_run=not args.no_dry_run)
            print(result)

        elif args.command == "check":
            checks = manager.check_campaign_readiness()

            print("🔍 ПРОВЕРКА ГОТОВНОСТИ К КАМПАНИИ")
            print("=" * 40)
            print(f"✅ CSV файл существует: {checks['csv_exists']}")
            print(f"✅ CSV читается: {checks['csv_readable']}")
            print(
                f"✅ Есть целевая аудитория: {checks['has_target_audience']}"
            )
            print(f"✅ Рабочее время: {checks['working_time']}")
            print(f"✅ Конфигурация валидна: {checks['config_valid']}")
            print(f"\n🎯 ГОТОВНОСТЬ: {'ДА' if checks['ready'] else 'НЕТ'}")

            if checks["recommendations"]:
                print(f"\n💡 РЕКОМЕНДАЦИИ:")
                for rec in checks["recommendations"]:
                    print(f"   - {rec}")

        elif args.command == "analyze":
            if args.target:
                analysis = manager.identify_target_audience()
                if "error" in analysis:
                    print(f"❌ {analysis['error']}")
                else:
                    print("🎯 АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ")
                    print("=" * 40)
                    print(
                        f"Доступно контактов: {analysis['total_available']:,}"
                    )
                    print(f"Приоритетные: {analysis['priority_contacts']:,}")
                    print(f"Обычные: {analysis['regular_contacts']:,}")
                    print(
                        f"Связались ранее: {analysis['contacted_previously']:,}"
                    )

                    if analysis["gaming_verticals"]:
                        print(f"\nTOП Gaming Verticals:")
                        for vertical, count in list(
                            analysis["gaming_verticals"].items()
                        )[:5]:
                            print(f"   {vertical}: {count}")
            else:
                analysis = manager.analyze_csv_data()
                if "error" in analysis:
                    print(f"❌ {analysis['error']}")
                else:
                    print("📊 АНАЛИЗ БАЗЫ ДАННЫХ")
                    print("=" * 40)
                    print(f"Всего контактов: {analysis['total_contacts']:,}")
                    print(f"Отправлено: {analysis['contacted_count']:,}")
                    print(f"С ответами: {analysis['with_responses']:,}")
                    print(f"Ожидают: {analysis['pending_contacts']:,}")

    except Exception as e:
        print(f"❌ Ошибка выполнения команды: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
