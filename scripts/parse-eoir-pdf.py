#!/usr/bin/env python3
"""
Parse EOIR Accredited Representatives Roster PDF using pdfplumber
(table-aware extraction — preserves column structure).

Source: https://www.justice.gov/eoir/page/file/942311/dl
Output: output/eoir-immigration-reps.csv

~2,641 accredited immigration reps in the US (non-lawyer immigration
representatives who work at recognized nonprofit organizations).
"""

import csv
import os
import re
import sys
import pdfplumber

PDF_PATHS = [
    '/tmp/eoir-reps.pdf',
    '/tmp/eoir-1398081.bin',
]
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'eoir-immigration-reps.csv')


def parse_pdf(path):
    """Extract table rows from PDF using pdfplumber."""
    entries = []
    with pdfplumber.open(path) as pdf:
        print(f'  Pages: {len(pdf.pages)}')
        for page_num, page in enumerate(pdf.pages, 1):
            tables = page.extract_tables()
            if not tables:
                # Fallback: extract text and try to parse row-wise
                text = page.extract_text() or ''
                continue

            for table in tables:
                for row in table:
                    if row and any(cell for cell in row if cell):
                        entries.append(row)
            if page_num % 20 == 0:
                print(f'    Parsed {page_num}/{len(pdf.pages)} pages, {len(entries)} rows so far')
    return entries


def normalize_row(row):
    """Extract name, accreditation, org, dates from a table row."""
    # Clean cells
    cells = [((c or '').strip().replace('\n', ' ')) for c in row]

    # Skip header rows and empty rows
    if not any(cells): return None
    header_markers = {'name', 'accreditation', 'organization', 'address',
                      'expiration', 'expires', 'status', 'appointment'}
    if sum(1 for c in cells if c.lower() in header_markers) >= 2:
        return None

    # Heuristic: find name cell (typically first cell with Lastname, Firstname pattern)
    name_cell = ''
    for c in cells:
        if re.match(r'^[A-Z][a-zA-Z\-\' ]+,?\s+[A-Z][a-zA-Z\-\'\s]+$', c) and len(c) > 3:
            name_cell = c
            break
    if not name_cell and cells:
        name_cell = cells[0]

    # Extract accreditation, org, expiration from remaining cells
    accred = ''
    org = ''
    expires = ''
    address = ''

    for c in cells:
        if not c or c == name_cell: continue
        if 'partial' in c.lower() or 'full accreditation' in c.lower():
            accred = c
        elif re.match(r'^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}', c):
            if not expires: expires = c
        elif len(c) > 10 and not org:
            org = c
        elif len(c) > 20 and org and not address:
            address = c

    # Parse first/last
    first_name, last_name = '', ''
    if ',' in name_cell:
        parts = name_cell.split(',', 1)
        last_name = parts[0].strip()
        first_name = parts[1].strip().replace('(DHS only)', '').strip()
    else:
        parts = name_cell.split()
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = ' '.join(parts[1:])
        else:
            first_name = name_cell

    # DHS marker
    dhs_only = '(DHS only)' in name_cell or 'DHS' in name_cell
    first_name = first_name.replace('(DHS only)', '').strip()
    last_name = last_name.replace('(DHS only)', '').strip()

    if not first_name and not last_name:
        return None

    return {
        'first_name': first_name,
        'last_name': last_name,
        'accreditation': accred,
        'dhs_only': 'Y' if dhs_only else '',
        'organization': org,
        'address': address,
        'expires': expires,
        'raw': ' | '.join(cells)[:300],
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print('=== EOIR Accredited Immigration Reps Parser (pdfplumber) ===')

    all_rows = []
    for path in PDF_PATHS:
        if not os.path.exists(path):
            print(f'  Skip: {path} (not found)')
            continue
        print(f'  Parsing {path}...')
        try:
            rows = parse_pdf(path)
            print(f'    Raw rows: {len(rows)}')
            all_rows.extend(rows)
        except Exception as e:
            print(f'  ERROR parsing {path}: {e}')

    # Normalize
    entries = []
    seen = set()
    for r in all_rows:
        e = normalize_row(r)
        if not e: continue
        key = (e['first_name'] + '|' + e['last_name'] + '|' + e['organization']).lower()
        if key in seen: continue
        seen.add(key)
        entries.append(e)

    # Write CSV
    cols = ['first_name', 'last_name', 'accreditation', 'dhs_only',
            'organization', 'address', 'expires', 'niche', 'source', 'country', 'raw']
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for e in entries:
            e['niche'] = 'immigration'
            e['source'] = 'eoir_accredited_reps'
            e['country'] = 'US'
            writer.writerow(e)

    print(f'\n=== DONE ===')
    print(f'Total raw rows: {len(all_rows)}')
    print(f'Unique entries: {len(entries)}')
    print(f'With org:       {sum(1 for e in entries if e.get("organization"))}')
    print(f'Output: {OUTPUT_FILE}')

    # Sample
    print('\nSample (first 5):')
    for e in entries[:5]:
        print(f'  {e["first_name"]} {e["last_name"]} ({e.get("accreditation","?")}) — {e.get("organization","?")[:60]}')


if __name__ == '__main__':
    main()
