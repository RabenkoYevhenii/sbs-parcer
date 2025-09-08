#!/usr/bin/env python3
"""
Contact Extractor Script

This script analyzes the 'introduction' column in the CSV file and extracts
contact information such as emails, phone numbers, websites, social media handles,
and other contact details into a new 'other_contacts' column.
"""

import pandas as pd
import re
import sys
from typing import List, Set


class ContactExtractor:
    def __init__(self):
        # Regex patterns for different types of contacts
        self.patterns = {
            "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
            "phone": r"(?:\+\d{1,3}[-.\s]?)?\(?[0-9]{1,4}\)?[-.\s]?[0-9]{1,4}[-.\s]?[0-9]{1,9}",
            "website": r"(?:https?://)?(?:www\.)?[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z]{2,})+(?:/[^\s,;)\]]*)?",
            "telegram": r"(?:telegram|t\.me)[:\s/]*[@]?[a-zA-Z0-9_]{3,32}",
            "whatsapp": r"(?:whatsapp|wa\.me)[:\s/]*\+?[0-9\s\-\(\)]{7,20}",
            "linkedin": r"(?:https?://)?(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_%]+/?",
            "twitter": r"(?:https?://)?(?:www\.)?(?:twitter\.com/|x\.com/)[a-zA-Z0-9_]{1,15}",
            "instagram": r"(?:https?://)?(?:www\.)?instagram\.com/[a-zA-Z0-9_.]{1,30}/?",
            "facebook": r"(?:https?://)?(?:www\.)?facebook\.com/[a-zA-Z0-9.]{5,}/?",
            "skype": r"(?:skype|teams)[:\s]+[a-zA-Z0-9\.\-_]{3,32}",
            "discord": r"(?:discord\.gg/|discord\.com/invite/)[a-zA-Z0-9]{6,16}",
            "at_mention": r"@[a-zA-Z0-9_]{3,30}(?=\s|$|[^a-zA-Z0-9_])",
        }

        # Additional patterns for common contact formats
        self.additional_patterns = [
            r"(?:contact|reach|email|call|dm|message)\s*[:\-]\s*([^\s,;.\n]+)",  # contact: info
            r"telegram\s*[:\-]\s*@?([a-zA-Z0-9_]{3,32})",  # telegram: username
            r"whatsapp\s*[:\-]\s*(\+?[0-9\s\-\(\)]{7,20})",  # whatsapp: number
            r"teams\s*[:\-]\s*([a-zA-Z0-9\.\-_@]{3,50})",  # teams: username
        ]

    def extract_contacts_from_text(self, text: str) -> Set[str]:
        """Extract all contact information from given text."""
        if not isinstance(text, str) or not text.strip():
            return set()

        contacts = set()
        text_lower = text.lower()

        # Extract using predefined patterns
        for contact_type, pattern in self.patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean up the match
                clean_match = self._clean_contact(match, contact_type)
                if clean_match:
                    contacts.add(clean_match)

        # Extract using additional patterns
        for pattern in self.additional_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match else ""
                clean_match = self._clean_contact(match, "other")
                if clean_match and len(clean_match) > 2:
                    contacts.add(clean_match)

        # Look for @ mentions (social media handles) - but not inside emails
        at_mentions = re.findall(
            r"(?<![a-zA-Z0-9])@[a-zA-Z0-9_]{3,30}(?=\s|$|[^a-zA-Z0-9_@.])",
            text,
        )
        for mention in at_mentions:
            contacts.add(mention)

        # Look for URLs that might have been missed
        urls = re.findall(r"https?://[^\s,;.)\]]+", text)
        for url in urls:
            clean_url = url.rstrip(".,;)")
            contacts.add(clean_url)

        # Look for domain names that might be contact websites
        domains = re.findall(
            r"(?<!\w)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.(?:com|org|net|io|co|ai|me|tech|app|dev|info|biz)\b",
            text,
        )
        for domain in domains:
            if not domain.startswith(("@", "www.")):
                contacts.add("https://" + domain)

        # Deduplicate similar contacts
        return self._deduplicate_contacts(contacts)

    def _clean_contact(self, contact: str, contact_type: str) -> str:
        """Clean and validate extracted contact information."""
        if not contact:
            return ""

        contact = contact.strip()

        # Remove common punctuation from the end
        contact = re.sub(r"[.,;:)\]]+$", "", contact)

        # For phone numbers, ensure they look reasonable
        if contact_type == "phone":
            # Remove non-digit characters to count digits
            digits_only = re.sub(r"[^\d]", "", contact)
            # Phone numbers should have at least 7 digits and not more than 15
            if len(digits_only) < 7 or len(digits_only) > 15:
                return ""

        # For emails, do basic validation
        if contact_type == "email":
            if not re.match(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$", contact
            ):
                return ""

        # For websites, ensure they have a valid domain
        if contact_type == "website":
            if not re.search(r"\.[a-zA-Z]{2,}", contact):
                return ""
            # Add protocol if missing for proper URLs
            if not contact.startswith(("http://", "https://")):
                if contact.startswith("www.") or "." in contact:
                    contact = "https://" + contact

        return contact

    def _deduplicate_contacts(self, contacts: Set[str]) -> Set[str]:
        """Remove duplicate contacts that represent the same information."""
        if not contacts:
            return set()

        contacts_list = list(contacts)
        final_contacts = set()

        # Group contacts by type for better deduplication
        phone_numbers = set()
        emails = set()
        usernames = set()
        websites = set()
        other_contacts = set()

        for contact in contacts_list:
            contact_lower = contact.lower()

            # Classify contact types
            if re.match(r"^[\+]?[0-9\s\-\(\)]{7,20}$", contact):
                # Phone number - normalize format
                normalized_phone = re.sub(r"[^\d+]", "", contact)
                phone_numbers.add(normalized_phone)
            elif (
                "@" in contact
                and "." in contact
                and not contact.startswith("@")
            ):
                # Email address
                emails.add(contact.lower())
            elif contact.startswith("@"):
                # Social media handle - normalize
                username = contact[1:].lower()
                usernames.add(username)
            elif contact.startswith("http") or "." in contact:
                # Website or URL
                websites.add(contact)
            else:
                other_contacts.add(contact)

        # Add unique phone numbers back
        for phone in phone_numbers:
            final_contacts.add(phone)

        # Add unique emails back
        for email in emails:
            final_contacts.add(email)

        # Add unique usernames back (prefer @ format)
        for username in usernames:
            final_contacts.add("@" + username)

        # Add unique websites back (deduplicate similar URLs)
        website_domains = {}
        for website in websites:
            # Normalize URL to get domain
            url = website.lower()
            if url.startswith("http://"):
                url = url[7:]
            elif url.startswith("https://"):
                url = url[8:]
            if url.startswith("www."):
                url = url[4:]

            # Extract domain part
            domain = url.split("/")[0].split("?")[0]

            # Keep the most complete version (prefer https over http, www over non-www)
            if domain not in website_domains:
                website_domains[domain] = website
            else:
                current = website_domains[domain]
                # Prefer https over http
                if website.startswith("https://") and current.startswith(
                    "http://"
                ):
                    website_domains[domain] = website
                # Prefer www version if both are https or both are http
                elif (
                    "www." in website
                    and "www." not in current
                    and website.startswith("https://")
                    == current.startswith("https://")
                ):
                    website_domains[domain] = website

        for website in website_domains.values():
            final_contacts.add(website)

        # Process other contacts to remove duplicates
        processed_others = set()
        for contact in other_contacts:
            contact_clean = contact.lower().strip()

            # Skip if it's essentially a duplicate of something we already have
            is_duplicate = False

            # Check if it's a duplicate username (without @)
            if contact_clean in usernames:
                is_duplicate = True

            # Check if it contains a phone number we already have
            for phone in phone_numbers:
                if phone in re.sub(r"[^\d+]", "", contact):
                    is_duplicate = True
                    break

            # Check for platform-specific duplicates (e.g., "Telegram: username" vs "@username")
            for username in usernames:
                if username in contact_clean and any(
                    platform in contact_lower
                    for platform in ["telegram", "whatsapp", "teams", "skype"]
                ):
                    is_duplicate = True
                    break

            if not is_duplicate:
                processed_others.add(contact)

        final_contacts.update(processed_others)

        return final_contacts

    def process_csv(self, input_file: str, output_file: str = None) -> None:
        """Process the CSV file and extract contacts from introduction column."""
        try:
            # Read the CSV file
            print(f"Reading CSV file: {input_file}")
            df = pd.read_csv(input_file)

            # Check if introduction column exists
            if "introduction" not in df.columns:
                raise ValueError(
                    "Column 'introduction' not found in the CSV file"
                )

            print(f"Found {len(df)} rows in the CSV file")

            # Count rows with non-null introduction for reporting
            non_null_count = (
                df["introduction"].notna()
                & (df["introduction"] != "")
                & (df["introduction"].astype(str).str.strip() != "")
            ).sum()

            print(
                f"Found {non_null_count} rows with non-empty introduction text"
            )

            # Extract contacts for each row
            contacts_list = []
            processed_count = 0
            rows_with_contacts = 0

            for idx, row in df.iterrows():
                # Check if introduction text exists and is not empty
                if (
                    pd.notna(row["introduction"])
                    and str(row["introduction"]).strip() != ""
                ):
                    introduction_text = str(row["introduction"])
                    contacts = self.extract_contacts_from_text(
                        introduction_text
                    )

                    # Join contacts with comma separator
                    contacts_str = (
                        ", ".join(sorted(contacts)) if contacts else ""
                    )
                    if contacts_str:
                        rows_with_contacts += 1
                else:
                    # Empty contacts for rows without introduction text
                    contacts_str = ""

                contacts_list.append(contacts_str)

                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"Processed {processed_count} rows...")

            # Add the new column to the original dataframe
            df["other_contacts"] = contacts_list

            # Reorder columns to place other_contacts right after other_socials
            if "other_socials" in df.columns:
                columns = df.columns.tolist()
                other_socials_index = columns.index("other_socials")

                # Remove other_contacts from its current position (which is at the end)
                columns.remove("other_contacts")

                # Insert other_contacts right after other_socials
                columns.insert(other_socials_index + 1, "other_contacts")

                # Reorder the dataframe with the new column order
                df = df[columns]

            # Create output filename if not provided
            if output_file is None:
                if input_file.endswith(".csv"):
                    output_file = input_file.replace(
                        ".csv", "_with_contacts.csv"
                    )
                else:
                    output_file = input_file + "_with_contacts.csv"

            # Save the result
            print(f"Saving results to: {output_file}")
            df.to_csv(output_file, index=False)

            # Print statistics
            print(f"\nProcessing completed!")
            print(f"Total rows processed: {len(df)}")
            print(f"Rows with extracted contacts: {rows_with_contacts}")
            print(
                f"Percentage with contacts: {rows_with_contacts/len(df)*100:.1f}%"
            )

            # Show some examples
            print(f"\nFirst 5 examples of extracted contacts:")
            for i, (idx, row) in enumerate(df.head().iterrows()):
                if row["other_contacts"]:
                    print(
                        f"{i+1}. {row['full_name']}: {row['other_contacts']}"
                    )
                else:
                    print(f"{i+1}. {row['full_name']}: No contacts extracted")

        except Exception as e:
            print(f"Error processing CSV file: {e}")
            sys.exit(1)

    def show_sample_extractions(
        self, input_file: str, num_samples: int = 10
    ) -> None:
        """Show sample extractions for review before processing the full file."""
        try:
            df = pd.read_csv(input_file)

            if "introduction" not in df.columns:
                raise ValueError(
                    "Column 'introduction' not found in the CSV file"
                )

            # Get samples with non-empty introduction
            non_null_df = df[
                df["introduction"].notna()
                & (df["introduction"] != "")
                & (df["introduction"].astype(str).str.strip() != "")
            ].copy()

            if len(non_null_df) == 0:
                print("No rows with non-empty introduction text found")
                return

            samples = non_null_df.head(num_samples)

            print(
                f"Sample extractions from first {len(samples)} rows with introduction text:\n"
            )
            print("=" * 80)

            for idx, row in samples.iterrows():
                introduction_text = str(row["introduction"])
                contacts = self.extract_contacts_from_text(introduction_text)

                print(f"\nName: {row['full_name']}")
                print(f"Company: {row['company_name']}")
                print(
                    f"Introduction: {introduction_text[:200]}{'...' if len(introduction_text) > 200 else ''}"
                )
                print(
                    f"Extracted contacts: {', '.join(sorted(contacts)) if contacts else 'None'}"
                )
                print("-" * 80)

        except Exception as e:
            print(f"Error showing samples: {e}")


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python extract_contacts.py <input_csv_file> [output_csv_file]"
        )
        print("       python extract_contacts.py <input_csv_file> --preview")
        print("\nExamples:")
        print("  python extract_contacts.py data.csv")
        print("  python extract_contacts.py data.csv output.csv")
        print("  python extract_contacts.py data.csv --preview")
        sys.exit(1)

    input_file = sys.argv[1]
    extractor = ContactExtractor()

    if len(sys.argv) > 2 and sys.argv[2] == "--preview":
        # Show sample extractions
        extractor.show_sample_extractions(input_file, num_samples=10)
    else:
        # Process the full file
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        extractor.process_csv(input_file, output_file)


if __name__ == "__main__":
    main()
