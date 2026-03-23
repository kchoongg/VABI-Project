"""
VAST Challenge 2025 MC1 - Data Wrangling Script
================================================
This script reads mc1_nodes.csv and mc1_edges.csv and produces
6 Tableau-ready CSV files.

Instructions:
1. Place mc1_nodes.csv and mc1_edges.csv in the SAME folder as this script
2. Open terminal/command prompt
3. Navigate to that folder: cd path/to/folder
4. Run: python wrangle.py
5. The 6 output CSVs will appear in the same folder

No external packages needed - uses only Python standard library.
"""

import csv
import os
from collections import defaultdict, Counter

# ============================================================
# CONFIGURATION
# ============================================================
SAILOR_ID = '17255'  # Sailor Shift's node ID
INFLUENCE_TYPES = ('InterpolatesFrom', 'InStyleOf', 'CoverOf', 'DirectlySamples', 'LyricalReferenceTo')
CONTRIBUTOR_TYPES = ('PerformerOf', 'ComposerOf', 'LyricistOf', 'ProducerOf')

# ============================================================
# STEP 1: LOAD AND INDEX DATA
# ============================================================
print("Step 1: Loading data...")

nodes = {}
with open('mc1_nodes.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        nodes[row['id']] = row

edges = []
with open('mc1_edges.csv', 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        edges.append(row)

print(f"  Loaded {len(nodes)} nodes and {len(edges)} edges")

# ============================================================
# STEP 2: BUILD LOOKUP MAPPINGS
# ============================================================
print("Step 2: Building lookup mappings...")

# person -> set of song/album IDs they performed
person_songs = defaultdict(set)

# song/album -> set of person IDs who performed it
song_performers = defaultdict(set)

# song/album -> {role -> set of person names}
work_contributors = defaultdict(lambda: defaultdict(set))

# song/album -> record label name
work_label = {}

for e in edges:
    if e['Edge Type'] == 'PerformerOf':
        person_songs[e['source']].add(e['target'])
        song_performers[e['target']].add(e['source'])

    if e['Edge Type'] in CONTRIBUTOR_TYPES:
        person = nodes.get(e['source'], {})
        work_contributors[e['target']][e['Edge Type']].add(person.get('name', '?'))

    if e['Edge Type'] == 'RecordedBy':
        work_label[e['source']] = nodes.get(e['target'], {}).get('name', 'Unknown')

# song/album -> number of times referenced by others via influence edges
song_influence_count = Counter()
for e in edges:
    if e['Edge Type'] in INFLUENCE_TYPES:
        song_influence_count[e['target']] += 1

print("  Done")

# ============================================================
# STEP 3: IDENTIFY KEY ENTITIES
# ============================================================
print("Step 3: Identifying key entities...")

# Sailor Shift's works (songs + albums she performed/composed/wrote/produced)
sailor_work_ids = set()
for e in edges:
    if e['source'] == SAILOR_ID and e['Edge Type'] in CONTRIBUTOR_TYPES:
        sailor_work_ids.add(e['target'])

# All Oceanus Folk songs/albums
of_song_ids = set(n['id'] for n in nodes.values() if n.get('genre', '').strip() == 'Oceanus Folk')

print(f"  Sailor Shift works: {len(sailor_work_ids)}")
print(f"  Oceanus Folk works: {len(of_song_ids)}")

# ============================================================
# TABLE 1: sailor_shift_career.csv
# ============================================================
print("Generating Table 1: sailor_shift_career.csv ...")

rows1 = []
for wid in sailor_work_ids:
    w = nodes.get(wid, {})
    contribs = work_contributors.get(wid, {})
    performers = ', '.join(sorted(contribs.get('PerformerOf', set())))
    composers = ', '.join(sorted(contribs.get('ComposerOf', set())))
    lyricists = ', '.join(sorted(contribs.get('LyricistOf', set())))
    label = work_label.get(wid, 'Unknown')

    # Count unique collaborators (excluding Sailor Shift herself)
    all_people = set()
    for role_people in contribs.values():
        all_people.update(role_people)
    all_people.discard('Sailor Shift')

    # What influenced this work
    influences_from = []
    for e in edges:
        if e['source'] == wid and e['Edge Type'] in INFLUENCE_TYPES:
            tgt = nodes.get(e['target'], {})
            influences_from.append(f"{tgt.get('name', '?')} ({tgt.get('genre', '?')})")

    rows1.append({
        'Song': w.get('name', '?'),
        'Type': w.get('Node Type', '?'),
        'Genre': w.get('genre', '?'),
        'Release_Year': w.get('release_date', '?'),
        'Notable': w.get('notable', '?'),
        'Single': w.get('single', '?'),
        'Record_Label': label,
        'Performers': performers,
        'Composers': composers,
        'Lyricists': lyricists,
        'Num_Collaborators': len(all_people),
        'Influenced_By': '; '.join(influences_from) if influences_from else 'None',
        'Num_Influences': len(influences_from),
    })

rows1.sort(key=lambda x: x['Release_Year'])
with open('sailor_shift_career.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=rows1[0].keys())
    writer.writeheader()
    writer.writerows(rows1)
print(f"  -> {len(rows1)} rows")

# ============================================================
# TABLE 2: sailor_shift_influences.csv
# ============================================================
print("Generating Table 2: sailor_shift_influences.csv ...")

rows2 = []

# Who influenced Sailor Shift (her works reference other works)
for e in edges:
    if e['source'] in sailor_work_ids and e['Edge Type'] in INFLUENCE_TYPES:
        src = nodes.get(e['source'], {})
        tgt = nodes.get(e['target'], {})
        tgt_perfs = work_contributors.get(e['target'], {}).get('PerformerOf', set())
        tgt_performers = ', '.join(sorted(tgt_perfs)) if tgt_perfs else 'Unknown'
        rows2.append({
            'Direction': 'Influenced Sailor Shift',
            'Influence_Type': e['Edge Type'],
            'Sailor_Song': src.get('name', '?'),
            'Sailor_Song_Year': src.get('release_date', '?'),
            'Other_Song': tgt.get('name', '?'),
            'Other_Song_Genre': tgt.get('genre', '?'),
            'Other_Song_Year': tgt.get('release_date', '?'),
            'Other_Artist': tgt_performers,
        })

# Who Sailor Shift influenced (other works reference her works)
for e in edges:
    if e['target'] in sailor_work_ids and e['Edge Type'] in INFLUENCE_TYPES:
        src = nodes.get(e['source'], {})
        tgt = nodes.get(e['target'], {})
        src_perfs = work_contributors.get(e['source'], {}).get('PerformerOf', set())
        src_performers = ', '.join(sorted(src_perfs)) if src_perfs else 'Unknown'
        rows2.append({
            'Direction': 'Sailor Shift Influenced',
            'Influence_Type': e['Edge Type'],
            'Sailor_Song': tgt.get('name', '?'),
            'Sailor_Song_Year': tgt.get('release_date', '?'),
            'Other_Song': src.get('name', '?'),
            'Other_Song_Genre': src.get('genre', '?'),
            'Other_Song_Year': src.get('release_date', '?'),
            'Other_Artist': src_performers,
        })

# Person-level references to Sailor Shift
for e in edges:
    if e['target'] == SAILOR_ID and e['Edge Type'] in INFLUENCE_TYPES:
        src = nodes.get(e['source'], {})
        rows2.append({
            'Direction': 'Sailor Shift Influenced',
            'Influence_Type': e['Edge Type'],
            'Sailor_Song': 'Sailor Shift (person)',
            'Sailor_Song_Year': '',
            'Other_Song': src.get('name', '?'),
            'Other_Song_Genre': src.get('genre', '?') or src.get('Node Type', '?'),
            'Other_Song_Year': src.get('release_date', '?'),
            'Other_Artist': src.get('name', '?'),
        })

with open('sailor_shift_influences.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=rows2[0].keys())
    writer.writeheader()
    writer.writerows(rows2)
print(f"  -> {len(rows2)} rows")

# ============================================================
# TABLE 3: sailor_shift_collaborators.csv
# ============================================================
print("Generating Table 3: sailor_shift_collaborators.csv ...")

rows3 = []
for wid in sailor_work_ids:
    w = nodes.get(wid, {})
    contribs = work_contributors.get(wid, {})
    for role, people in contribs.items():
        for person_name in people:
            if person_name != 'Sailor Shift':
                rows3.append({
                    'Song': w.get('name', '?'),
                    'Release_Year': w.get('release_date', '?'),
                    'Genre': w.get('genre', '?'),
                    'Collaborator': person_name,
                    'Role': role.replace('Of', ''),
                })

with open('sailor_shift_collaborators.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Song', 'Release_Year', 'Genre', 'Collaborator', 'Role'])
    writer.writeheader()
    writer.writerows(rows3)
print(f"  -> {len(rows3)} rows")

# ============================================================
# TABLE 4: oceanus_folk_spread.csv
# ============================================================
print("Generating Table 4: oceanus_folk_spread.csv ...")

rows4 = []

# Songs influenced BY Oceanus Folk (non-OF songs referencing OF songs)
for e in edges:
    if e['target'] in of_song_ids and e['Edge Type'] in INFLUENCE_TYPES:
        src = nodes.get(e['source'], {})
        tgt = nodes.get(e['target'], {})
        src_perfs = work_contributors.get(e['source'], {}).get('PerformerOf', set())
        tgt_perfs = work_contributors.get(e['target'], {}).get('PerformerOf', set())
        rows4.append({
            'Direction': 'OF Influenced Others',
            'Influence_Type': e['Edge Type'],
            'OF_Song': tgt.get('name', '?'),
            'OF_Song_Year': tgt.get('release_date', '?'),
            'OF_Artist': ', '.join(sorted(tgt_perfs)) if tgt_perfs else 'Unknown',
            'Other_Song': src.get('name', '?'),
            'Other_Genre': src.get('genre', '?'),
            'Other_Year': src.get('release_date', '?'),
            'Other_Artist': ', '.join(sorted(src_perfs)) if src_perfs else 'Unknown',
        })

# Songs that influenced Oceanus Folk (OF songs referencing non-OF songs)
for e in edges:
    if e['source'] in of_song_ids and e['Edge Type'] in INFLUENCE_TYPES:
        src = nodes.get(e['source'], {})
        tgt = nodes.get(e['target'], {})
        if tgt.get('id') not in of_song_ids:
            src_perfs = work_contributors.get(e['source'], {}).get('PerformerOf', set())
            tgt_perfs = work_contributors.get(e['target'], {}).get('PerformerOf', set())
            rows4.append({
                'Direction': 'Others Influenced OF',
                'Influence_Type': e['Edge Type'],
                'OF_Song': src.get('name', '?'),
                'OF_Song_Year': src.get('release_date', '?'),
                'OF_Artist': ', '.join(sorted(src_perfs)) if src_perfs else 'Unknown',
                'Other_Song': tgt.get('name', '?'),
                'Other_Genre': tgt.get('genre', '?'),
                'Other_Year': tgt.get('release_date', '?'),
                'Other_Artist': ', '.join(sorted(tgt_perfs)) if tgt_perfs else 'Unknown',
            })

with open('oceanus_folk_spread.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=rows4[0].keys())
    writer.writeheader()
    writer.writerows(rows4)
print(f"  -> {len(rows4)} rows")

# ============================================================
# TABLE 5: oceanus_folk_timeline.csv
# ============================================================
print("Generating Table 5: oceanus_folk_timeline.csv ...")

rows5 = []
for sid in of_song_ids:
    s = nodes.get(sid, {})
    performers = ', '.join(sorted(nodes.get(p, {}).get('name', '?') for p in song_performers.get(sid, set())))
    label = work_label.get(sid, 'Unknown')
    inf_in = sum(1 for e in edges if e['source'] == sid and e['Edge Type'] in INFLUENCE_TYPES)
    inf_out = sum(1 for e in edges if e['target'] == sid and e['Edge Type'] in INFLUENCE_TYPES)

    rows5.append({
        'Song': s.get('name', '?'),
        'Release_Year': s.get('release_date', '?'),
        'Notable': s.get('notable', '?'),
        'Single': s.get('single', '?'),
        'Artist': performers,
        'Record_Label': label,
        'Draws_From_Count': inf_in,
        'Influenced_Others_Count': inf_out,
        'Is_Sailor_Shift': 'Yes' if sid in sailor_work_ids else 'No',
    })

rows5.sort(key=lambda x: x['Release_Year'])
with open('oceanus_folk_timeline.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=rows5[0].keys())
    writer.writeheader()
    writer.writerows(rows5)
print(f"  -> {len(rows5)} rows")

# ============================================================
# TABLE 6: artist_careers.csv
# ============================================================
print("Generating Table 6: artist_careers.csv ...")

rows6 = []
for pid, songs in person_songs.items():
    if len(songs) < 3:
        continue
    person = nodes.get(pid, {})
    if person.get('Node Type') not in ('Person', 'MusicalGroup'):
        continue

    song_details = [nodes.get(s, {}) for s in songs if nodes.get(s, {}).get('Node Type') in ('Song', 'Album')]
    if not song_details:
        continue

    genres = Counter(s.get('genre', '?') for s in song_details)
    years = [int(s.get('release_date', '0')) for s in song_details
             if s.get('release_date', 'NA') not in ('NA', '') and s.get('release_date', '0').isdigit()]
    notable_count = sum(1 for s in song_details if s.get('notable', '') == 'TRUE')
    inf_score = sum(song_influence_count.get(s, 0) for s in songs)

    all_collabs = set()
    for s in songs:
        for role_people in work_contributors.get(s, {}).values():
            all_collabs.update(role_people)
    all_collabs.discard(person.get('name', '?'))

    rows6.append({
        'Artist': person.get('name', '?'),
        'Node_Type': person.get('Node Type', '?'),
        'Num_Songs': len(song_details),
        'Primary_Genre': genres.most_common(1)[0][0] if genres else '?',
        'All_Genres': '; '.join(f"{g}({c})" for g, c in genres.most_common()),
        'First_Release': min(years) if years else '',
        'Last_Release': max(years) if years else '',
        'Career_Span': (max(years) - min(years)) if len(years) > 1 else 0,
        'Notable_Songs': notable_count,
        'Influence_Score': inf_score,
        'Num_Collaborators': len(all_collabs),
        'Is_Oceanus_Folk': 'Yes' if any(s.get('genre', '') == 'Oceanus Folk' for s in song_details) else 'No',
    })

rows6.sort(key=lambda x: -x['Influence_Score'])
with open('artist_careers.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=rows6[0].keys())
    writer.writeheader()
    writer.writerows(rows6)
print(f"  -> {len(rows6)} rows")

# ============================================================
# DONE
# ============================================================
print("\n" + "=" * 50)
print("ALL 6 CSV FILES GENERATED SUCCESSFULLY!")
print("=" * 50)
print("\nOutput files:")
for fname in ['sailor_shift_career.csv', 'sailor_shift_influences.csv',
              'sailor_shift_collaborators.csv', 'oceanus_folk_spread.csv',
              'oceanus_folk_timeline.csv', 'artist_careers.csv']:
    size = os.path.getsize(fname)
    print(f"  {fname} ({size:,} bytes)")
print("\nYou can now import these into Tableau!")