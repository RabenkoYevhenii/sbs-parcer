"""
Company filtering functionality for excluded companies and similarity matching
"""

import os
import re
from typing import List, Dict, Tuple


class CompanyFilter:
    """Handles company exclusion logic and similarity matching"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.excluded_companies = []
        self._load_excluded_companies()

    def _load_excluded_companies(self):
        """Loads the list of companies to exclude from CSV file"""
        exclude_file = os.path.join(self.data_dir, "Exclude list.csv")

        if not os.path.exists(exclude_file):
            print(f"⚠️ Файл виключень не знайдено: {exclude_file}")
            return

        try:
            self.excluded_companies = []
            with open(exclude_file, "r", encoding="utf-8") as f:
                # Skip header
                next(f)
                for line in f:
                    company_name = line.strip()
                    if company_name:
                        # Normalize company name for better matching
                        normalized = self._normalize_company_name(company_name)
                        self.excluded_companies.append(
                            {
                                "original": company_name,
                                "normalized": normalized,
                            }
                        )

            print(
                f"📋 Завантажено {len(self.excluded_companies)} компаній до списку виключень"
            )

        except Exception as e:
            print(f"❌ Помилка завантаження списку виключень: {e}")

    def _normalize_company_name(self, company_name: str) -> str:
        """Normalizes company name for better comparison"""
        if not company_name:
            return ""

        # Convert to lowercase and remove common suffixes/prefixes
        normalized = company_name.lower().strip()

        # Remove common company suffixes
        suffixes_to_remove = [
            " ltd",
            " llc",
            " inc",
            " corp",
            " corporation",
            " company",
            " co",
            " s.a.c",
            " s.a",
            " b.v",
            " gmbh",
            " ag",
            " s.r.l",
            " srl",
            " limited",
            " entertainment",
            " gaming",
            " games",
            " casino",
            " casinos",
            " betting",
            " bet",
            " pay",
            " payment",
            " payments",
        ]

        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)].strip()

        # Remove special characters but keep spaces and alphanumeric
        normalized = "".join(
            c for c in normalized if c.isalnum() or c.isspace()
        )

        # Remove extra spaces
        normalized = " ".join(normalized.split())

        return normalized

    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculates similarity between two strings using Levenshtein distance"""
        if not str1 or not str2:
            return 0.0

        # Simple Levenshtein distance implementation
        def levenshtein_distance(s1, s2):
            if len(s1) < len(s2):
                return levenshtein_distance(s2, s1)

            if len(s2) == 0:
                return len(s1)

            previous_row = list(range(len(s2) + 1))
            for i, c1 in enumerate(s1):
                current_row = [i + 1]
                for j, c2 in enumerate(s2):
                    insertions = previous_row[j + 1] + 1
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)
                    current_row.append(
                        min(insertions, deletions, substitutions)
                    )
                previous_row = current_row

            return previous_row[-1]

        distance = levenshtein_distance(str1, str2)
        max_len = max(len(str1), len(str2))

        if max_len == 0:
            return 1.0

        return 1.0 - (distance / max_len)

    def _is_company_excluded(
        self, company_name: str, similarity_threshold: float = 0.8
    ) -> Tuple[bool, str, float]:
        """Checks if company is in the exclusion list

        Returns:
            tuple: (is_excluded: bool, matched_company: str, similarity_score: float)
        """
        # Handle NaN or None values
        if (
            not company_name
            or not self.excluded_companies
            or str(company_name).lower() in ["nan", "none", ""]
        ):
            return False, "", 0.0

        normalized_input = self._normalize_company_name(company_name)

        best_match = ""
        best_similarity = 0.0

        for excluded_company in self.excluded_companies:
            excluded_normalized = excluded_company["normalized"]
            excluded_original = excluded_company["original"]

            # Skip comparison for very short company names
            if len(normalized_input) < 3 or len(excluded_normalized) < 3:
                continue

            # Direct match
            if normalized_input == excluded_normalized:
                return True, excluded_original, 1.0

            # Partial match (one contains the other)
            if (
                normalized_input in excluded_normalized
                or excluded_normalized in normalized_input
            ):
                # Calculate similarity for partial matches
                similarity = max(
                    len(normalized_input) / len(excluded_normalized),
                    len(excluded_normalized) / len(normalized_input),
                )
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = excluded_original

            # Fuzzy match using Levenshtein distance
            similarity = self._calculate_similarity(
                normalized_input, excluded_normalized
            )
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = excluded_original

        # Return True if similarity is above threshold
        is_excluded = best_similarity >= similarity_threshold
        return is_excluded, best_match, best_similarity

    def is_company_excluded(
        self, company_name: str, similarity_threshold: float = 0.8
    ) -> bool:
        """Public method to check if a company is excluded"""
        is_excluded, _, _ = self._is_company_excluded(
            company_name, similarity_threshold
        )
        return is_excluded

    def get_exclusion_details(
        self, company_name: str, similarity_threshold: float = 0.8
    ) -> Dict:
        """Get detailed exclusion information for a company"""
        is_excluded, matched_company, similarity = self._is_company_excluded(
            company_name, similarity_threshold
        )

        return {
            "is_excluded": is_excluded,
            "matched_company": matched_company,
            "similarity_score": similarity,
            "normalized_input": self._normalize_company_name(company_name),
        }

    def reload_excluded_companies(self):
        """Reloads the list of excluded companies"""
        self._load_excluded_companies()

    def show_excluded_companies(self):
        """Shows the current list of excluded companies"""
        print(f"\n🚫 СПИСОК ВИКЛЮЧЕНИХ КОМПАНІЙ")
        print("=" * 40)

        if not self.excluded_companies:
            print("📝 Список виключень порожній")
            return

        print(f"📊 Всього компаній у списку: {len(self.excluded_companies)}")
        print("\n📋 Компанії:")

        for i, company in enumerate(self.excluded_companies, 1):
            original = company["original"]
            normalized = company["normalized"]
            print(f"   {i:3d}. {original}")
            if original.lower() != normalized:
                print(f"        → {normalized}")

        print(f"\n💡 Файл виключень: restricted/data/Exclude list.csv")
        print(f"🔄 Для оновлення змініть файл та перезапустіть програму")

    def test_company_exclusion(self, company_name: str):
        """Tests if a company would be excluded"""
        print(f"\n🧪 ТЕСТ ВИКЛЮЧЕННЯ КОМПАНІЇ")
        print("=" * 40)
        print(f"🏢 Тестуємо: '{company_name}'")

        details = self.get_exclusion_details(company_name)

        print(f"📝 Нормалізовано до: '{details['normalized_input']}'")
        print(
            f"🎯 Результат: {'❌ ВИКЛЮЧЕНО' if details['is_excluded'] else '✅ ДОЗВОЛЕНО'}"
        )

        if details["matched_company"]:
            print(f"🔍 Найближчий збіг: '{details['matched_company']}'")
            print(f"📊 Схожість: {details['similarity_score']:.2%}")

        if not details["is_excluded"]:
            print(f"✅ Компанія '{company_name}' НЕ буде виключена")
            print(f"💬 Можна відправляти повідомлення")
        else:
            print(f"❌ Компанія '{company_name}' БУДЕ виключена")
            print(f"🚫 Повідомлення НЕ будуть відправлятися")
