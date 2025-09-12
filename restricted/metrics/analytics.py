#!/usr/bin/env python3
"""
SBC Attendees Analytics Script
Analyzes daily CSV files and generates statistics report
"""

import pandas as pd
import os
from datetime import datetime, date
import re
from typing import Dict, List, Tuple


class SBCAnalytics:
    def __init__(self, data_dir: str = "../data"):
        self.data_dir = data_dir
        self.main_csv = os.path.join(data_dir, "SBC - Attendees.csv")
        self.stats_csv = os.path.join(data_dir, "daily_statistics.csv")

        # Ensure the main CSV exists
        if not os.path.exists(self.main_csv):
            raise FileNotFoundError(
                f"Main CSV file not found: {self.main_csv}"
            )

    def get_daily_files(self) -> List[str]:
        """Get list of daily attendees CSV files, prioritizing _new versions"""
        daily_files = []
        date_to_file = {}  # Track dates to avoid duplicates

        for file in os.listdir(self.data_dir):
            if file.startswith("attendees_") and file.endswith(".csv"):
                file_path = os.path.join(self.data_dir, file)
                date_str = self.extract_date_from_filename(file_path)

                if date_str:
                    # If we already have a file for this date, prefer the "_new" version
                    if date_str in date_to_file:
                        if "_new" in file:
                            date_to_file[date_str] = (
                                file_path  # Replace with new version
                            )
                    else:
                        date_to_file[date_str] = file_path

        # Get list of unique files
        daily_files = list(date_to_file.values())

        # Sort by date
        daily_files.sort(key=lambda x: self.extract_date_from_filename(x))
        return daily_files

    def extract_date_from_filename(self, filename: str) -> str:
        """Extract date from filename like 'attendees_09_01.csv' or 'attendees_09_07_new.csv'"""
        basename = os.path.basename(filename)
        # Handle both regular and "_new" files
        match = re.search(r"attendees_(\d{2}_\d{2})(?:_new)?\.csv", basename)
        if match:
            month_day = match.group(1)
            # Convert to date format (assuming 2025)
            month, day = month_day.split("_")
            return f"2025-{month}-{day}"
        return ""

    def count_scraped_contacts(self, csv_file: str) -> int:
        """Count total scraped contacts from daily CSV"""
        try:
            df = pd.read_csv(csv_file)
            return len(df)
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
            return 0

    def count_valid_by_filters(self, csv_file: str) -> int:
        """Count contacts that would pass the filters used in the main script"""
        try:
            df = pd.read_csv(csv_file)

            # Apply the same filters as in the main script
            original_count = len(df)

            # Filter by gaming_vertical (exclude "land")
            if "gaming_vertical" in df.columns:
                df = df[
                    ~df["gaming_vertical"].str.contains(
                        "land", case=False, na=False
                    )
                ]

            # Filter by position (include key positions)
            position_keywords = [
                "chief executive officer",
                "ceo",
                "chief operating officer",
                "coo",
                "chief financial officer",
                "cfo",
                "chief payments officer",
                "cpo",
                "payments",
                "psp",
                "operations",
                "business development",
                "partnerships",
                "relationship",
                "country manager",
            ]

            if "position" in df.columns:
                # Convert positions to lowercase for comparison
                df["position_lower"] = df["position"].str.lower().fillna("")

                # Create mask for positions containing keywords
                position_mask = df["position_lower"].str.contains(
                    "|".join(position_keywords), case=False, na=False
                )

                # Exclude "coordinator" for COO
                coordinator_mask = df["position_lower"].str.contains(
                    "coordinator", case=False, na=False
                )
                coo_mask = df["position_lower"].str.contains(
                    "coo", case=False, na=False
                )

                # Apply filter
                df = df[position_mask & ~(coo_mask & coordinator_mask)]

                # Drop temporary column
                df = df.drop("position_lower", axis=1)

            return len(df)

        except Exception as e:
            print(f"Error filtering {csv_file}: {e}")
            return 0

    def normalize_date_string(self, date_str: str) -> str:
        """Convert various date formats to standard d.mm format"""
        if pd.isna(date_str) or str(date_str).strip() == "":
            return ""

        date_str = str(date_str).strip()

        # Try different date formats and convert to d.mm
        try:
            # Format: dd.mm.yyyy or dd.mm.yy
            if len(date_str.split(".")) == 3:
                parts = date_str.split(".")
                day = int(parts[0])
                month = int(parts[1])
                return f"{day}.{month:02d}"

            # Format: dd.mm or d.mm
            elif len(date_str.split(".")) == 2:
                parts = date_str.split(".")
                day = int(parts[0])
                month = int(parts[1])
                return f"{day}.{month:02d}"

            # If it's already in d.mm format, ensure proper formatting
            else:
                return ""
        except (ValueError, IndexError):
            return ""

    def count_sent_messages_by_date_range(
        self, start_date: str, end_date: str
    ) -> Tuple[int, int, int, int, int]:
        """Count sent messages for a date range from main CSV by account"""
        try:
            df = pd.read_csv(self.main_csv)

            # Normalize the Date column
            df["Date_normalized"] = df["Date"].apply(
                self.normalize_date_string
            )

            # Generate all dates in the range and convert to CSV format (d.mm)
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            csv_dates = []
            current_dt = start_dt
            while current_dt <= end_dt:
                # Generate both d.mm and dd.mm formats
                csv_date_d_mm = f"{current_dt.day}.{current_dt.month:02d}"
                csv_date_dd_mm = f"{current_dt.day:02d}.{current_dt.month:02d}"
                csv_dates.extend([csv_date_d_mm, csv_date_dd_mm])
                current_dt += pd.Timedelta(days=1)

            # Count records where Date matches any date in range (regardless of connection status)
            # Having a date means a message was sent on that date
            mask = df["Date_normalized"].isin(csv_dates)

            sent_df = df[mask]
            total_count = len(sent_df)

            # Count authors separately, including historical Daniil data
            sent_df = sent_df.copy()

            # First count Daniil entries (historical data) before any mapping
            daniil_count = len(
                sent_df[sent_df["author"].isin(["Daniiil", "Danil", "Daniil"])]
            )

            # Count current authors
            anton_count = len(sent_df[sent_df["author"] == "Anton"])
            yaroslav_count = len(sent_df[sent_df["author"] == "Yaroslav"])
            ihor_count = len(sent_df[sent_df["author"] == "Ihor"])

            # Handle unattributed messages
            attributed_count = (
                daniil_count + anton_count + yaroslav_count + ihor_count
            )
            unattributed = total_count - attributed_count

            if unattributed > 0:
                print(
                    f"   ⚠️ Warning: {unattributed} sent messages without proper author attribution for range {start_date} to {end_date}"
                )

                # Only split among accounts that actually exist (have messages)
                active_accounts = []
                if anton_count > 0:
                    active_accounts.append("anton")
                if yaroslav_count > 0:
                    active_accounts.append("yaroslav")
                if ihor_count > 0:
                    active_accounts.append("ihor")

                # If no current accounts are active, don't redistribute unattributed messages
                # They likely belong to historical data or system entries
                if len(active_accounts) > 0:
                    split_each = unattributed // len(active_accounts)
                    remainder = unattributed % len(active_accounts)

                    for i, account in enumerate(active_accounts):
                        additional = split_each + (1 if i < remainder else 0)
                        if account == "anton":
                            anton_count += additional
                        elif account == "yaroslav":
                            yaroslav_count += additional
                        elif account == "ihor":
                            ihor_count += additional
                else:
                    print(
                        f"   📝 Note: Unattributed messages likely belong to historical/system entries and will not be redistributed"
                    )

            return (
                total_count,
                daniil_count,
                yaroslav_count,
                anton_count,
                ihor_count,
            )

        except Exception as e:
            print(f"Error counting sent messages for range: {e}")
            return 0, 0, 0, 0, 0

    def count_answered_messages_by_date_range(
        self, start_date: str, end_date: str
    ) -> int:
        """Count answered messages for a date range from main CSV"""
        try:
            df = pd.read_csv(self.main_csv)

            # Normalize the Date column
            df["Date_normalized"] = df["Date"].apply(
                self.normalize_date_string
            )

            # Generate all dates in the range and convert to CSV format (d.mm)
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            csv_dates = []
            current_dt = start_dt
            while current_dt <= end_dt:
                # Generate both d.mm and dd.mm formats
                csv_date_d_mm = f"{current_dt.day}.{current_dt.month:02d}"
                csv_date_dd_mm = f"{current_dt.day:02d}.{current_dt.month:02d}"
                csv_dates.extend([csv_date_d_mm, csv_date_dd_mm])
                current_dt += pd.Timedelta(days=1)

            # Count records where Date matches any date in range and connected contains "answer"
            mask = (df["Date_normalized"].isin(csv_dates)) & (
                df["connected"].str.contains("answer", case=False, na=False)
            )

            return mask.sum()

        except Exception as e:
            print(f"Error counting answered messages for range: {e}")
            return 0

    def normalize_followup_date(self, date_str: str) -> str:
        """Convert various follow-up date formats to standard d.mm format"""
        if pd.isna(date_str) or str(date_str).strip() == "":
            return ""

        date_str = str(date_str).strip()

        # Try different date formats and convert to d.mm
        try:
            # Format: dd.mm.yyyy or dd.mm.yy
            if len(date_str.split(".")) == 3:
                parts = date_str.split(".")
                day = int(parts[0])
                month = int(parts[1])
                return f"{day}.{month:02d}"

            # Format: dd.mm or d.mm (already correct format)
            elif len(date_str.split(".")) == 2:
                parts = date_str.split(".")
                day = int(parts[0])
                month = int(parts[1])
                return f"{day}.{month:02d}"

            # Try to parse as float (legacy format)
            elif "." in date_str:
                try:
                    float_val = float(date_str)
                    # Convert float like 5.09 to string format
                    return str(float_val)
                except ValueError:
                    return ""
            else:
                return ""
        except (ValueError, IndexError):
            return ""

    def count_followup_messages_by_date_range(
        self, start_date: str, end_date: str
    ) -> int:
        """Count follow-up messages sent in a date range from main CSV"""
        try:
            df = pd.read_csv(self.main_csv)

            # Generate all dates in the range and convert to various CSV formats
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            csv_dates = []
            current_dt = start_dt
            while current_dt <= end_dt:
                # Generate multiple formats to match
                csv_date_d_mm = f"{current_dt.day}.{current_dt.month:02d}"
                csv_date_dd_mm = f"{current_dt.day:02d}.{current_dt.month:02d}"
                csv_date_float = f"{current_dt.day}.{current_dt.month:02d}"  # String representation of float format
                csv_dates.extend(
                    [csv_date_d_mm, csv_date_dd_mm, csv_date_float]
                )
                current_dt += pd.Timedelta(days=1)

            # Count records where follow_up_date matches any date in range
            if "follow_up_date" in df.columns:
                # Normalize follow_up_date column
                df["follow_up_date_normalized"] = df["follow_up_date"].apply(
                    self.normalize_followup_date
                )
                mask = df["follow_up_date_normalized"].isin(csv_dates)
                return mask.sum()
            else:
                return 0

        except Exception as e:
            print(f"Error counting follow-up messages for range: {e}")
            return 0

    def count_sent_messages_by_date(
        self, target_date: str
    ) -> Tuple[int, int, int, int, int]:
        """Count sent messages for specific date from main CSV by account"""
        return self.count_sent_messages_by_date_range(target_date, target_date)

    def count_answered_messages_by_date(self, target_date: str) -> int:
        """Count answered messages for specific date from main CSV"""
        return self.count_answered_messages_by_date_range(
            target_date, target_date
        )

    def count_followup_messages_by_date(self, target_date: str) -> int:
        """Count follow-up messages sent on specific date from main CSV"""
        try:
            df = pd.read_csv(self.main_csv)

            # Convert target_date to various formats used in CSV
            date_obj = datetime.strptime(target_date, "%Y-%m-%d")
            csv_date_formats = [
                f"{date_obj.day}.{date_obj.month:02d}",  # d.mm format
                f"{date_obj.day:02d}.{date_obj.month:02d}",  # dd.mm format
                str(
                    float(f"{date_obj.day}.{date_obj.month:02d}")
                ),  # float format as string
            ]

            # Count records where follow_up_date matches the target date
            if "follow_up_date" in df.columns:
                # Normalize follow_up_date column
                df["follow_up_date_normalized"] = df["follow_up_date"].apply(
                    self.normalize_followup_date
                )
                mask = df["follow_up_date_normalized"].isin(csv_date_formats)
                return mask.sum()
            else:
                return 0

        except Exception as e:
            print(f"Error counting follow-up messages: {e}")
            return 0

    def get_days_covered_by_file(
        self, current_date: str, previous_date: str = None
    ) -> Tuple[int, List[str]]:
        """Calculate how many days are covered by a file based on gaps from previous file"""
        current_dt = datetime.strptime(current_date, "%Y-%m-%d")

        if previous_date is None:
            # First file, assume it covers just one day
            return 1, [current_date]

        previous_dt = datetime.strptime(previous_date, "%Y-%m-%d")
        days_gap = (current_dt - previous_dt).days

        if days_gap == 1:
            # Consecutive days, this file covers just one day
            return 1, [current_date]
        elif days_gap > 1:
            # Gap detected, this file covers multiple days
            covered_dates = []
            for i in range(1, days_gap + 1):
                covered_date = (previous_dt + pd.Timedelta(days=i)).strftime(
                    "%Y-%m-%d"
                )
                covered_dates.append(covered_date)
            print(
                f"   📅 Gap detected: {current_date} file covers {days_gap} days: {', '.join(covered_dates)}"
            )
            return days_gap, covered_dates
        else:
            # Same day or overlapping (shouldn't happen with proper file naming)
            return 1, [current_date]

    def analyze_daily_data(self) -> List[Dict]:
        """Analyze all daily CSV files and return statistics"""
        daily_files = self.get_daily_files()
        results = []

        print(f"📊 Analyzing {len(daily_files)} daily files...")

        previous_date = None

        for file_path in daily_files:
            date_str = self.extract_date_from_filename(file_path)
            if not date_str:
                print(f"⚠️ Could not extract date from {file_path}")
                continue

            # Determine how many days this file covers
            days_covered, covered_dates = self.get_days_covered_by_file(
                date_str, previous_date
            )

            print(
                f"📅 Processing {date_str} ({os.path.basename(file_path)})..."
            )
            if days_covered > 1:
                print(
                    f"   📊 This file represents {days_covered} days of data: {', '.join(covered_dates)}"
                )

            # Count metrics
            scraped = self.count_scraped_contacts(file_path)
            valid = self.count_valid_by_filters(file_path)

            # If this file covers multiple days, we need to handle it differently
            if days_covered > 1:
                # Count messages for the entire date range
                start_date = covered_dates[0]
                end_date = covered_dates[-1]
                sent, daniil_sent, yaroslav_sent, anton_sent, ihor_sent = (
                    self.count_sent_messages_by_date_range(
                        start_date, end_date
                    )
                )
                answered = self.count_answered_messages_by_date_range(
                    start_date, end_date
                )
                followups = self.count_followup_messages_by_date_range(
                    start_date, end_date
                )

                # Create a single entry for the entire range
                result = {
                    "Дата": f"{start_date} to {end_date}",
                    "Новых контактов": scraped,
                    "Провалидированых": valid,
                    "Отправлено Сообщений": sent,
                    "Ответили": answered,
                    "Даниил": daniil_sent,  # Historical data
                    "Ярослав": yaroslav_sent,
                    "Антон": anton_sent,  # New messenger1 account
                    "Игорь": ihor_sent,  # New messenger3 account
                    "Количество follow_up": followups,
                    "% Валидных": round(
                        (valid / scraped) if scraped > 0 else 0, 3
                    ),
                    "% Ответивших": round(
                        (answered / sent) if sent > 0 else 0, 3
                    ),
                    "% Даниил": 0.0,  # Keep for backward compatibility
                    "% Ярослав": round(
                        (yaroslav_sent / sent) if sent > 0 else 0, 3
                    ),
                    "% Антон": round(
                        (anton_sent / sent) if sent > 0 else 0, 3
                    ),
                    "% Игорь": round((ihor_sent / sent) if sent > 0 else 0, 3),
                }
                results.append(result)

                print(
                    f"   📈 Scraped: {scraped}, Valid: {valid}, Sent: {sent}, Answered: {answered}"
                )
                print(
                    f"   👥 Anton: {anton_sent}, Yaroslav: {yaroslav_sent}, Ihor: {ihor_sent}, Follow-ups: {followups}"
                )
            else:
                # Single day file - use original logic
                sent, daniil_sent, yaroslav_sent, anton_sent, ihor_sent = (
                    self.count_sent_messages_by_date(date_str)
                )
                answered = self.count_answered_messages_by_date(date_str)
                followups = self.count_followup_messages_by_date(date_str)

                result = {
                    "Дата": date_str,
                    "Новых контактов": scraped,
                    "Провалидированых": valid,
                    "Отправлено Сообщений": sent,
                    "Ответили": answered,
                    "Даниил": daniil_sent,  # Historical data
                    "Ярослав": yaroslav_sent,
                    "Антон": anton_sent,  # New messenger1 account
                    "Игорь": ihor_sent,  # New messenger3 account
                    "Количество follow_up": followups,
                    "% Валидных": round(
                        (valid / scraped) if scraped > 0 else 0, 3
                    ),
                    "% Ответивших": round(
                        (answered / sent) if sent > 0 else 0, 3
                    ),
                    "% Даниил": 0.0,  # Keep for backward compatibility
                    "% Ярослав": round(
                        (yaroslav_sent / sent) if sent > 0 else 0, 3
                    ),
                    "% Антон": round(
                        (anton_sent / sent) if sent > 0 else 0, 3
                    ),
                    "% Игорь": round((ihor_sent / sent) if sent > 0 else 0, 3),
                }

                results.append(result)

                print(
                    f"   📈 Scraped: {scraped}, Valid: {valid}, Sent: {sent}, Answered: {answered}"
                )
                print(
                    f"   👥 Anton: {anton_sent}, Yaroslav: {yaroslav_sent}, Ihor: {ihor_sent}, Follow-ups: {followups}"
                )

            previous_date = date_str

        return results

    def create_statistics_csv(
        self, data: List[Dict], append: bool = False
    ) -> str:
        """Create or append to statistics CSV file"""
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(self.stats_csv), exist_ok=True)

        df = pd.DataFrame(data)

        if append and os.path.exists(self.stats_csv):
            print(f"📄 Updating existing CSV file: {self.stats_csv}")
            # Read existing data
            existing_df = pd.read_csv(self.stats_csv)

            # Check if existing file has old English column names
            if "date" in existing_df.columns:
                print("🔄 Converting old column names to Russian...")
                # Map old columns to new Russian columns
                column_mapping = {
                    "date": "Дата",
                    "scraped_new_contacts": "Новых контактов",
                    "valid_by_filters": "Провалидированых",
                    "sent_messages": "Отправлено Сообщений",
                    "answered": "Ответили",
                    "valid_percentage": "% Валидных",
                    "answer_percentage": "% Ответивших",
                }
                existing_df = existing_df.rename(columns=column_mapping)

                # Add missing new columns with default values
                new_columns = [
                    "Даниил",
                    "Ярослав",
                    "Количество follow_up",
                    "% Даниил",
                    "% Ярослав",
                ]
                for col in new_columns:
                    if col not in existing_df.columns:
                        existing_df[col] = 0

            # Remove the old "Дата последнего follow_up" column if it exists
            if "Дата последнего follow_up" in existing_df.columns:
                existing_df = existing_df.drop(
                    columns=["Дата последнего follow_up"]
                )
                print(
                    "🗑️ Removed 'Дата последнего follow_up' column from daily statistics"
                )

            # Remove duplicate dates and append new data
            existing_df = existing_df[~existing_df["Дата"].isin(df["Дата"])]
            df = pd.concat([existing_df, df], ignore_index=True)
        else:
            print(f"📄 Creating new CSV file: {self.stats_csv}")

        # Sort by date (handle date ranges properly)
        def sort_date_key(date_str):
            """Extract sortable date from date string, handling ranges"""
            if " to " in str(date_str):
                # For date ranges, use the start date for sorting
                start_date = str(date_str).split(" to ")[0]
                try:
                    return pd.to_datetime(start_date, format="%Y-%m-%d")
                except:
                    return pd.to_datetime("1900-01-01")  # fallback
            else:
                try:
                    return pd.to_datetime(date_str, format="%Y-%m-%d")
                except:
                    return pd.to_datetime("1900-01-01")  # fallback

        df["sort_key"] = df["Дата"].apply(sort_date_key)
        df = df.sort_values("sort_key")
        df = df.drop("sort_key", axis=1)

        # Reorder columns according to the specified order
        desired_column_order = [
            "Дата",
            "Новых контактов",
            "Провалидированых",
            "Отправлено Сообщений",
            "Ответили",
            "Даниил",
            "Ярослав",
            "Антон",
            "Игорь",
            "% Даниил",
            "% Ярослав",
            "% Антон",
            "% Игорь",
            "Количество follow_up",
            "% Валидных",
            "% Ответивших",
        ]

        # Ensure all desired columns exist (add missing ones with 0 values)
        for col in desired_column_order:
            if col not in df.columns:
                df[col] = 0

        # Reorder columns and keep any extra columns at the end
        extra_columns = [
            col for col in df.columns if col not in desired_column_order
        ]
        final_column_order = desired_column_order + extra_columns
        df = df[final_column_order]

        # Save to CSV
        df.to_csv(self.stats_csv, index=False)

        print(f"✅ CSV file saved with {len(df)} records")
        return self.stats_csv

    def initialize_csv_file(self) -> str:
        """Initialize an empty CSV file with proper headers"""
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(self.stats_csv), exist_ok=True)

        # Create empty DataFrame with proper columns
        columns = [
            "Дата",
            "Новых контактов",
            "Провалидированых",
            "Отправлено Сообщений",
            "Ответили",
            "% Валидных",
            "% Ответивших",
        ]
        empty_df = pd.DataFrame(columns=columns)

        # Save to CSV
        empty_df.to_csv(self.stats_csv, index=False)

        print(f"📄 Initialized empty CSV file: {self.stats_csv}")
        print(f"📋 Columns: {', '.join(columns)}")

        return self.stats_csv

    def print_summary(self, data: List[Dict]):
        """Print a formatted summary of the statistics"""
        print("\n" + "=" * 150)
        print("📊 SBC ATTENDEES DAILY STATISTICS SUMMARY")
        print("=" * 150)

        # Header with all columns
        print(
            f"{'Date':<12} {'Scraped':<8} {'Valid':<6} {'Sent':<6} {'Answered':<8} {'Daniil':<7} {'Yaroslav':<8} {'Anton':<6} {'Ihor':<6} {'Follow-ups':<10} {'Valid%':<6} {'Answer%':<7} {'Dan%':<5} {'Yar%':<5} {'Ant%':<5} {'Ihor%':<6}"
        )
        print("-" * 190)

        total_scraped = 0
        total_valid = 0
        total_sent = 0
        total_answered = 0
        total_daniil = 0  # Keep for historical data
        total_yaroslav = 0
        total_anton = 0  # New messenger1
        total_ihor = 0  # New messenger3
        total_followups = 0

        for row in data:
            date = row["Дата"]
            scraped = row["Новых контактов"]
            valid = row["Провалидированых"]
            sent = row["Отправлено Сообщений"]
            answered = row["Ответили"]
            # Handle both old and new columns separately
            daniil = row.get("Даниил", 0)  # Historical data
            yaroslav = row.get("Ярослав", 0)  # Continues
            anton = row.get("Антон", 0)  # New messenger1
            ihor = row.get("Игорь", 0)  # New messenger3
            followups = row.get("Количество follow_up", 0)

            # Calculate percentages
            valid_pct = (valid / scraped * 100) if scraped > 0 else 0
            answer_pct = (answered / sent * 100) if sent > 0 else 0
            daniil_pct = (daniil / sent * 100) if sent > 0 else 0
            yaroslav_pct = (yaroslav / sent * 100) if sent > 0 else 0
            anton_pct = (anton / sent * 100) if sent > 0 else 0
            ihor_pct = (ihor / sent * 100) if sent > 0 else 0

            print(
                f"{date:<12} {scraped:<8} {valid:<6} {sent:<6} {answered:<8} {daniil:<7.0f} {yaroslav:<8.0f} {anton:<6.0f} {ihor:<6.0f} {followups:<10.0f} {valid_pct:<5.1f}% {answer_pct:<6.1f}% {daniil_pct:<4.1f}% {yaroslav_pct:<4.1f}% {anton_pct:<4.1f}% {ihor_pct:<5.1f}%"
            )

            total_scraped += scraped
            total_valid += valid
            total_sent += sent
            total_answered += answered
            total_daniil += daniil
            total_yaroslav += yaroslav
            total_anton += anton
            total_ihor += ihor
            total_followups += followups

        # Print totals
        print("-" * 190)
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
        total_anton_pct = (
            (total_anton / total_sent * 100) if total_sent > 0 else 0
        )
        total_ihor_pct = (
            (total_ihor / total_sent * 100) if total_sent > 0 else 0
        )

        print(
            f"{'TOTAL':<12} {total_scraped:<8} {total_valid:<6} {total_sent:<6} {total_answered:<8} {total_daniil:<7.0f} {total_yaroslav:<8.0f} {total_anton:<6.0f} {total_ihor:<6.0f} {total_followups:<10.0f} {total_valid_pct:<5.1f}% {total_answer_pct:<6.1f}% {total_daniil_pct:<4.1f}% {total_yaroslav_pct:<4.1f}% {total_anton_pct:<4.1f}% {total_ihor_pct:<5.1f}%"
        )

        print("\n📈 Key Metrics:")
        print(f"   • Total contacts scraped: {total_scraped:,}")
        print(
            f"   • Contacts passing filters: {total_valid:,} ({total_valid_pct:.1f}%)"
        )
        print(f"   • Messages sent: {total_sent:,}")
        print(
            f"   • Messages answered: {total_answered:,} ({total_answer_pct:.1f}%)"
        )
        print(f"   • Follow-ups sent: {total_followups:,}")
        print(
            f"   • Daniil messages (historical): {total_daniil:,.0f} ({total_daniil_pct:.1f}%)"
        )
        print(
            f"   • Yaroslav messages: {total_yaroslav:,.0f} ({total_yaroslav_pct:.1f}%)"
        )
        print(
            f"   • Anton messages (new): {total_anton:,.0f} ({total_anton_pct:.1f}%)"
        )
        print(
            f"   • Ihor messages (new): {total_ihor:,.0f} ({total_ihor_pct:.1f}%)"
        )

        if total_sent > 0:
            conversion_rate = (
                (total_answered / total_valid * 100) if total_valid > 0 else 0
            )
            print(
                f"   • Overall conversion rate: {conversion_rate:.1f}% (answered/valid)"
            )

    def run_analysis(self, append: bool = True):
        """Run complete analysis and generate report"""
        print("🚀 Starting SBC Attendees Analytics...")

        # Analyze daily data
        data = self.analyze_daily_data()

        if not data:
            print("❌ No data found to analyze")
            return

        # Create/update CSV
        csv_path = self.create_statistics_csv(data, append=append)
        print(f"\n💾 Statistics saved to: {csv_path}")

        # Print summary
        self.print_summary(data)

        print(f"\n✅ Analysis complete! Check {csv_path} for detailed data.")


def main():
    """Main function to run the analytics"""
    try:
        # Change to the script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)

        # Initialize analytics
        analytics = SBCAnalytics()

        # Run analysis
        analytics.run_analysis(append=True)

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
