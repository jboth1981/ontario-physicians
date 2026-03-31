"""Parse CPSO physician detail pages into structured dicts."""

import re

from bs4 import BeautifulSoup


def _text(element):
    """Extract cleaned text from a BeautifulSoup element, or None."""
    if element is None:
        return None
    text = element.get_text(strip=True)
    # Collapse multiple whitespace (common in the CPSO pages)
    text = re.sub(r"\s+", " ", text)
    return text if text else None


def _texts(elements):
    """Extract cleaned text from a list of elements, deduplicated."""
    seen = set()
    results = []
    for el in elements:
        t = _text(el)
        if t and t not in seen:
            seen.add(t)
            results.append(t)
    return results


def _parse_name(full_name):
    """Split 'Last, First Middle' into (first_name, last_name).

    Returns (first_name, last_name). First name includes middle names.
    """
    if not full_name:
        return None, None
    parts = full_name.split(",", 1)
    last_name = parts[0].strip()
    first_name = parts[1].strip() if len(parts) > 1 else None
    # Collapse multiple spaces in first name
    if first_name:
        first_name = re.sub(r"\s+", " ", first_name)
    return first_name, last_name


def _parse_address_block(block):
    """Parse an address block element into a dict.

    Address blocks contain lines of text separated by <br> tags.
    The structure varies, but typically:
    - Practice name (optional)
    - Street address
    - City, Province  Postal Code
    - Phone / Fax lines
    """
    addr = {
        "name": None,
        "street": None,
        "city": None,
        "province": None,
        "postal_code": None,
        "phone": None,
        "phone_ext": None,
        "fax": None,
        "email": None,
    }

    # Get the parent/container for phone, fax, email via scrp- classes
    container = block.find_parent("div", class_="list-content") or block.parent

    phone_el = container.find(class_="scrp-phone-value") if container else None
    fax_el = container.find(class_="scrp-fax-value") if container else None
    ext_el = container.find(class_="scrp-phoneextension-value") if container else None
    email_el = container.find(class_="scrp-businessemail") if container else None

    if phone_el:
        addr["phone"] = _text(phone_el)
    if fax_el:
        addr["fax"] = _text(fax_el)
    if ext_el:
        addr["phone_ext"] = _text(ext_el)
    if email_el:
        # Email might be in an <a> tag
        a_tag = email_el.find("a")
        addr["email"] = _text(a_tag) if a_tag else _text(email_el)

    # Parse the address text itself
    # Get text with <br> replaced by newlines
    for br in block.find_all("br"):
        br.replace_with("\n")
    lines = [line.strip() for line in block.get_text().split("\n") if line.strip()]

    if not lines:
        return addr

    # Try to identify city/province/postal from the last geographical line
    # Pattern: "City, Province  PostalCode" or just "City, Province"
    postal_pattern = re.compile(r"([A-Z]\d[A-Z]\s*\d[A-Z]\d)")
    partial_postal_pattern = re.compile(r"([A-Z]\d[A-Z])(?:\s*$)")
    province_pattern = re.compile(
        r"[,\s]\s*(Ontario|Quebec|British Columbia|Alberta|Manitoba|Saskatchewan|"
        r"Nova Scotia|New Brunswick|Newfoundland(?: and Labrador)?|"
        r"PEI|Prince Edward Island|"
        r"Northwest Territories|Nunavut|Yukon|"
        r"ON|QC|BC|AB|MB|SK|NS|NB|NL|PE|NT|NU|YT)\b",
        re.IGNORECASE,
    )

    geo_line_idx = None
    # Postal code is the most reliable anchor — find the line that has one
    for i, line in enumerate(lines):
        if postal_pattern.search(line):
            geo_line_idx = i
            break
    # Fall back to the last line with a province match
    if geo_line_idx is None:
        for i, line in enumerate(lines):
            if province_pattern.search(line):
                geo_line_idx = i

    if geo_line_idx is not None:
        geo_line = lines[geo_line_idx]

        # Extract postal code (full or partial FSA)
        postal_match = postal_pattern.search(geo_line)
        if postal_match:
            addr["postal_code"] = postal_match.group(1).strip()
            geo_line = geo_line[: postal_match.start()].strip()
        else:
            partial_match = partial_postal_pattern.search(geo_line)
            if partial_match:
                addr["postal_code"] = partial_match.group(1).strip()
                geo_line = geo_line[: partial_match.start()].strip()

        # Extract province
        prov_match = province_pattern.search(geo_line)
        if prov_match:
            addr["province"] = prov_match.group(1).strip()
            addr["city"] = geo_line[: prov_match.start()].strip().rstrip(",")
        else:
            addr["city"] = geo_line.strip().rstrip(",")

        # Everything before geo line is name/street
        pre_lines = lines[:geo_line_idx]
        if len(pre_lines) >= 2:
            addr["name"] = pre_lines[0]
            addr["street"] = " ".join(pre_lines[1:])
        elif len(pre_lines) == 1:
            addr["street"] = pre_lines[0]
    else:
        # No geo line found — just dump everything as street
        addr["street"] = " ".join(lines)

    return addr


def parse_physician_page(html, cpso_number):
    """Parse a physician detail page HTML into a structured dict.

    Returns a dict with all fields needed by db.insert_physician().
    """
    soup = BeautifulSoup(html, "lxml")
    data = {"cpso_number": cpso_number, "raw_html": html}

    # --- Basic info ---
    # Use only the first occurrence of each to avoid desktop/mobile duplicates
    name_el = soup.find(class_="scrp-contactname-value")
    data["full_name"] = _text(name_el)
    data["first_name"], data["last_name"] = _parse_name(data["full_name"])

    data["gender"] = _text(soup.find(class_="scrp-gender-value"))
    # Note: CPSO has a typo — "laguage" not "language"
    data["languages"] = _text(soup.find(class_="scrp-laguage-value"))
    data["former_name"] = _text(soup.find(class_="scrp-formername"))
    data["member_status"] = _text(soup.find(class_="scrp-memberstatus-label"))
    data["registration_status"] = _text(soup.find(class_="scrp-registrationstatus-value"))
    data["status_date"] = _text(soup.find(class_="scrp-statusdate-value"))
    data["registration_class"] = _text(soup.find(class_="scrp-registrationclass-value"))

    # --- Education ---
    edu_el = soup.find(class_="scrp-education-value") or soup.find(
        class_="scrp-medicalschool-value"
    )
    if edu_el:
        edu_text = _text(edu_el)
        if edu_text:
            # Sometimes format is "School Name, Year"
            year_match = re.search(r",?\s*(\d{4})\s*$", edu_text)
            if year_match:
                data["medical_school"] = edu_text[: year_match.start()].strip()
                data["graduation_year"] = year_match.group(1)
            else:
                data["medical_school"] = edu_text
                data["graduation_year"] = None
        else:
            data["medical_school"] = None
            data["graduation_year"] = None
    else:
        data["medical_school"] = None
        data["graduation_year"] = None

    # --- Addresses ---
    data["addresses"] = []

    # Primary practice address
    primary_addr_el = soup.find(class_="scrp-practiceaddress-value")
    if primary_addr_el:
        addr = _parse_address_block(primary_addr_el)
        addr["address_type"] = "primary"
        data["addresses"].append(addr)

    # Additional addresses — look for sections after the primary
    additional_sections = soup.find_all(class_="scrp-additionaladdress")
    for section in additional_sections:
        addr_el = section.find(class_="scrp-practiceaddress-value")
        if addr_el:
            addr = _parse_address_block(addr_el)
            addr["address_type"] = "additional"
            data["addresses"].append(addr)

    # --- Specialties ---
    data["specialties"] = []
    specialty_names = soup.find_all(class_="scrp-specialtyname-value")
    issued_dates = soup.find_all(class_="scrp-issuedon-value")
    certifying_bodies = soup.find_all(class_="scrp-certifyingbody-value")

    # Deduplicate (desktop + mobile renders duplicate elements)
    seen_specs = set()
    for i in range(len(specialty_names)):
        name = _text(specialty_names[i])
        if not name or name in seen_specs:
            continue
        seen_specs.add(name)
        data["specialties"].append(
            {
                "specialty_name": name,
                "certifying_body": _text(certifying_bodies[i]) if i < len(certifying_bodies) else None,
                "effective_date": _text(issued_dates[i]) if i < len(issued_dates) else None,
            }
        )

    # --- Hospital privileges ---
    data["hospital_privileges"] = []
    hosp_rows = soup.find_all(class_="scrp-hospitalprivilege-row")
    seen_hosps = set()
    for row in hosp_rows:
        name = _text(row.find(class_="scrp-hospitalname-value"))
        location = _text(row.find(class_="scrp-location-value"))
        key = (name, location)
        if not name or key in seen_hosps:
            continue
        seen_hosps.add(key)
        data["hospital_privileges"].append(
            {"hospital_name": name, "hospital_location": location}
        )

    # --- Professional corporations ---
    data["professional_corporations"] = []
    corp_rows = soup.find_all(class_="scrp-profcorp-row")
    seen_corps = set()
    for row in corp_rows:
        corp_name = _text(row.find(class_="scrp-corpname-value"))
        if not corp_name:
            # Try getting the name from the row header
            header = row.find("h4") or row.find("strong")
            corp_name = _text(header)
        if not corp_name or corp_name in seen_corps:
            continue
        seen_corps.add(corp_name)

        status = _text(row.find(class_="scrp-status-label"))
        end_date = _text(row.find(class_="scrp-enddate-value"))

        # Business address
        biz_addr_row = row.find(class_="scrp-businessaddress-row")
        biz_addr = _text(biz_addr_row.find(class_="scrp-address-value")) if biz_addr_row else None

        # Shareholders
        shareholders_container = row.find(class_="scrp-shareholders")
        shareholders = None
        if shareholders_container:
            names = shareholders_container.find_all(class_="scrp-shareholdername-value")
            shareholder_list = _texts(names)
            if shareholder_list:
                shareholders = "; ".join(shareholder_list)

        data["professional_corporations"].append(
            {
                "corp_name": corp_name,
                "corp_status": status,
                "end_date": end_date,
                "business_address": biz_addr,
                "shareholders": shareholders,
            }
        )

    # --- Registration history ---
    data["registration_history"] = []
    hist_rows = soup.find_all(class_="scrp-reghistory-row")
    seen_hist = set()
    for row in hist_rows:
        details = _text(row.find(class_="scrp-details-value"))
        eff_date = _text(row.find(class_="scrp-effectivedate-value"))
        key = (details, eff_date)
        if not details or key in seen_hist:
            continue
        seen_hist.add(key)
        data["registration_history"].append(
            {"details": details, "effective_date": eff_date}
        )

    # --- Practice conditions ---
    data["practice_conditions"] = []
    conditions_container = soup.find(class_="scrp-term-clauses")
    if conditions_container:
        clauses = conditions_container.find_all(class_="scrp-term-clause-value")
        data["practice_conditions"] = _texts(clauses)

    # --- Public notifications ---
    data["public_notifications"] = []

    # Current tribunal proceedings / referrals
    referrals_container = soup.find(class_="scrp-current-referrals")
    if referrals_container:
        # Each referral may be in a list item, paragraph, or div
        items = referrals_container.find_all(["li", "p", "div"], recursive=False)
        if items:
            for item in items:
                text = _text(item)
                if text:
                    data["public_notifications"].append(
                        {"notification_type": "current_referral", "summary": text}
                    )
        else:
            # Fallback: take all text
            text = _text(referrals_container)
            if text:
                data["public_notifications"].append(
                    {"notification_type": "current_referral", "summary": text}
                )

    # Past findings
    findings_container = soup.find(class_="scrp-past-findings")
    if findings_container:
        items = findings_container.find_all(["li", "p", "div"], recursive=False)
        if items:
            for item in items:
                text = _text(item)
                if text:
                    data["public_notifications"].append(
                        {"notification_type": "past_finding", "summary": text}
                    )
        else:
            text = _text(findings_container)
            if text:
                data["public_notifications"].append(
                    {"notification_type": "past_finding", "summary": text}
                )

    return data
