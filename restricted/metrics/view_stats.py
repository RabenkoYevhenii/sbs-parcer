#!/usr/bin/env python3
"""
Просмотр ежедневной статистики в форматированной таблице
"""

import pandas as pd
import os


def view_statistics():
    """Отобразить ежедневную статистику в форматированной таблице"""
    csv_file = "data/daily_statistics.csv"

    if not os.path.exists(csv_file):
        print("❌ Файл статистики не найден. Сначала запустите analytics.py.")
        return

    try:
        df = pd.read_csv(csv_file)

        print("\n" + "=" * 80)
        print("📊 ЕЖЕДНЕВНАЯ СТАТИСТИКА УЧАСТНИКОВ SBC")
        print("=" * 80)

        # Header
        print(
            f"{'Дата':<12} {'Новых':<8} {'Валид':<8} {'Отправ':<8} {'Ответ':<8} {'Д':<6} {'Я':<6} {'F-up':<6} {'%Вал':<6} {'%Отв':<6} {'%Д':<6} {'%Я':<6}"
        )
        print("-" * 100)

        total_scraped = 0
        total_valid = 0
        total_sent = 0
        total_answered = 0
        total_daniil = 0
        total_yaroslav = 0
        total_followups = 0

        for _, row in df.iterrows():
            # Check for both old and new column names
            if "Дата" in row:
                date = row["Дата"]
                scraped = row["Новых контактов"]
                valid = row["Провалидированых"]
                sent = row["Отправлено Сообщений"]
                answered = row["Ответили"]

                # New columns with defaults
                daniil = row.get("Даниил", 0)
                yaroslav = row.get("Ярослав", 0)
                followups = row.get("Количество follow_up", 0)

                # Percentages (expecting decimal values, not * 100)
                valid_pct = (
                    row.get(
                        "% Валидных", (valid / scraped) if scraped > 0 else 0
                    )
                    * 100
                )
                answer_pct = (
                    row.get(
                        "% Ответивших", (answered / sent) if sent > 0 else 0
                    )
                    * 100
                )
                daniil_pct = (
                    row.get("% Даниил", (daniil / sent) if sent > 0 else 0)
                    * 100
                )
                yaroslav_pct = (
                    row.get("% Ярослав", (yaroslav / sent) if sent > 0 else 0)
                    * 100
                )
            else:
                # Fallback to old column names
                date = row["date"]
                scraped = row["scraped_new_contacts"]
                valid = row["valid_by_filters"]
                sent = row["sent_messages"]
                answered = row["answered"]

                # Default values for new columns
                daniil = 0
                yaroslav = 0
                followups = 0

                if "valid_percentage" in row and pd.notna(
                    row["valid_percentage"]
                ):
                    valid_pct = row["valid_percentage"] * 100
                else:
                    valid_pct = (valid / scraped * 100) if scraped > 0 else 0

                if "answer_percentage" in row and pd.notna(
                    row["answer_percentage"]
                ):
                    answer_pct = row["answer_percentage"] * 100
                else:
                    answer_pct = (answered / sent * 100) if sent > 0 else 0

                daniil_pct = 0
                yaroslav_pct = 0

            print(
                f"{date:<12} {scraped:<8} {valid:<8} {sent:<8} {answered:<8} {daniil:<6} {yaroslav:<6} {followups:<6} {valid_pct:<5.1f} {answer_pct:<5.1f} {daniil_pct:<5.1f} {yaroslav_pct:<5.1f}"
            )

            total_scraped += scraped
            total_valid += valid
            total_sent += sent
            total_answered += answered
            total_daniil += daniil
            total_yaroslav += yaroslav
            total_followups += followups

        # Print totals
        print("-" * 100)
        total_valid_pct = (
            (total_valid / total_scraped * 100) if total_scraped > 0 else 0
        )
        total_answer_pct = (
            (total_answered / total_sent * 100) if total_sent > 0 else 0
        )
        total_daniil_pct = (
            (total_daniil / total_sent * 100) if total_sent > 0 else 0
        )
        total_yaroslav_pct = (
            (total_yaroslav / total_sent * 100) if total_sent > 0 else 0
        )

        print(
            f"{'ИТОГО':<12} {total_scraped:<8} {total_valid:<8} {total_sent:<8} {total_answered:<8} {total_daniil:<6} {total_yaroslav:<6} {total_followups:<6} {total_valid_pct:<5.1f} {total_answer_pct:<5.1f} {total_daniil_pct:<5.1f} {total_yaroslav_pct:<5.1f}"
        )

        print("\n📈 Ключевые показатели:")
        print(f"   • Всего собрано контактов: {total_scraped:,}")
        print(
            f"   • Контактов прошедших фильтры: {total_valid:,} ({total_valid_pct:.1f}%)"
        )
        print(f"   • Отправлено сообщений: {total_sent:,}")
        print(
            f"   • Получено ответов: {total_answered:,} ({total_answer_pct:.1f}%)"
        )
        print(
            f"   • Сообщений от Данила: {total_daniil:,} ({total_daniil_pct:.1f}%)"
        )
        print(
            f"   • Сообщений от Ярослава: {total_yaroslav:,} ({total_yaroslav_pct:.1f}%)"
        )
        print(f"   • Follow-up сообщений: {total_followups:,}")

        if total_sent > 0:
            conversion_rate = (
                (total_answered / total_valid * 100) if total_valid > 0 else 0
            )
            print(
                f"   • Общий коэффициент конверсии: {conversion_rate:.1f}% (ответы/валидные)"
            )

        print("\n" + "=" * 80)

    except Exception as e:
        print(f"❌ Ошибка чтения статистики: {e}")


def main():
    """Главная функция"""
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    view_statistics()


if __name__ == "__main__":
    main()
