"""
Gephi Data Preparation for Sailor Shift Career Analysis
========================================================
Run this script once. It outputs 6 CSV files (nodes + edges for each question)
that you import directly into Gephi.

Usage:
    python gephi_prep.py

Output files:
    q1_nodes.csv / q1_edges.csv  — Influences ON Sailor Shift (over time)
    q2_nodes.csv / q2_edges.csv  — Collaborators & people she influenced
    q3_nodes.csv / q3_edges.csv  — Her influence on Oceanus Folk community
"""

import pandas as pd

# ── Load raw data ────────────────────────────────────────────────────────────
nodes = pd.read_csv("mc1_nodes.csv")
edges = pd.read_csv("mc1_edges.csv")

SAILOR_ID = nodes[nodes["name"] == "Sailor Shift"]["id"].values[0]
print(f"Sailor Shift node ID: {SAILOR_ID}")

# ── Shared helper ─────────────────────────────────────────────────────────────
def build_gephi_files(sub_edges, all_nodes, prefix):
    """
    Given a filtered edge DataFrame, pull the relevant nodes,
    rename columns to Gephi convention, and save to CSV.
    """
    relevant_ids = set(sub_edges["Source"]) | set(sub_edges["Target"])
    sub_nodes = all_nodes[all_nodes["Id"].isin(relevant_ids)].copy()

    node_file = f"{prefix}_nodes.csv"
    edge_file = f"{prefix}_edges.csv"
    sub_nodes.to_csv(node_file, index=False)
    sub_edges.to_csv(edge_file, index=False)
    print(f"  {node_file}: {len(sub_nodes)} nodes")
    print(f"  {edge_file}: {len(sub_edges)} edges")


# ── Rename columns to Gephi convention ───────────────────────────────────────
nodes_g = nodes.rename(columns={"id": "Id", "name": "Label"})
edges_g = edges.rename(columns={
    "source": "Source",
    "target": "Target",
    "Edge Type": "Type"
})

# Add a "year" column for Gephi's Timeline feature (uses release_date on songs)
nodes_g["year"] = nodes_g["release_date"].fillna(nodes_g["written_date"])

# ═══════════════════════════════════════════════════════════════════════════════
# Q1 — Who has Sailor Shift been most influenced by, over time?
# ═══════════════════════════════════════════════════════════════════════════════
#
# Logic: Find songs/works that Sailor Shift created (as performer/composer/
# lyricist), then trace OUTWARD influence edges FROM those songs to older
# works/artists. Also include direct InStyleOf edges FROM Sailor herself.
#
# Edge types that express musical influence:
INFLUENCE_TYPES = [
    "InStyleOf",          # Sailor's style draws from an artist/song
    "InterpolatesFrom",   # Sailor's song melodically borrows from another
    "LyricalReferenceTo", # Sailor's lyrics reference another work
    "DirectlySamples",    # Sailor's song directly samples another
    "CoverOf",            # Sailor covered someone else's song
]

print("\n── Q1: Influences on Sailor Shift ──")

# Step 1: Songs that Sailor performed / composed / wrote
sailor_work_types = ["PerformerOf", "ComposerOf", "LyricistOf", "ProducerOf"]
sailor_song_ids = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["Type"].isin(sailor_work_types))
]["Target"].unique()

# Step 2: Influence edges FROM those songs (song → older source)
q1_song_influence = edges_g[
    (edges_g["Source"].isin(sailor_song_ids)) &
    (edges_g["Type"].isin(INFLUENCE_TYPES))
]

# Step 3: Direct influence edges FROM Sailor herself (artist → artist)
q1_direct_influence = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["Type"].isin(INFLUENCE_TYPES))
]

# Step 4: Also include the Sailor→Song creation edges so the graph shows context
q1_creation_edges = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["Type"].isin(sailor_work_types)) &
    (edges_g["Target"].isin(sailor_song_ids))
]

q1_edges = pd.concat([
    q1_song_influence,
    q1_direct_influence,
    q1_creation_edges
]).drop_duplicates()

build_gephi_files(q1_edges, nodes_g, "q1")

# ── Gephi tips for Q1 ──
# • Layout:       Yifan Hu or Force Atlas 2
# • Node colour:  Partition → Node Type  (Person=blue, Song=orange)
# • Node size:    Degree (larger = more connections)
# • Edge colour:  Partition → Type  (each influence type gets its own colour)
# • Timeline:     Enable Dynamic filter on the "year" column to animate
#                 which influences appeared at different points in her career


# ═══════════════════════════════════════════════════════════════════════════════
# Q2 — Who has Sailor Shift collaborated with and directly/indirectly influenced?
# ═══════════════════════════════════════════════════════════════════════════════
#
# Logic:
#   Collaborators  = people who share a song/album credit with Sailor
#   Direct influence = people/songs with InStyleOf/CoverOf/etc. pointing TO
#                      Sailor's songs
#   Indirect influence = collaborators' connections one hop further out

print("\n── Q2: Collaborators & people Sailor influenced ──")

COLLAB_TYPES = ["PerformerOf", "ComposerOf", "LyricistOf", "ProducerOf"]

# Sailor's songs
sailor_songs = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["Type"].isin(COLLAB_TYPES))
]["Target"].unique()

# Others credited on the same songs
collab_edges = edges_g[
    (edges_g["Target"].isin(sailor_songs)) &
    (edges_g["Type"].isin(COLLAB_TYPES)) &
    (edges_g["Source"] != SAILOR_ID)
]
collaborator_ids = collab_edges["Source"].unique()

# Sailor → song creation edges
sailor_creation = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["Type"].isin(COLLAB_TYPES))
]

# People who directly reference / cover / sample Sailor's songs (she influenced them)
direct_influence_on_others = edges_g[
    (edges_g["Target"].isin(sailor_songs)) &
    (edges_g["Type"].isin(INFLUENCE_TYPES))
]
directly_influenced_ids = direct_influence_on_others["Source"].unique()

# Indirect: songs made by those directly influenced people, then others who
# reference THOSE songs (2nd-degree influence)
influenced_songs = edges_g[
    (edges_g["Source"].isin(directly_influenced_ids)) &
    (edges_g["Type"].isin(COLLAB_TYPES))
]["Target"].unique()

indirect_influence = edges_g[
    (edges_g["Target"].isin(influenced_songs)) &
    (edges_g["Type"].isin(INFLUENCE_TYPES))
]

q2_edges = pd.concat([
    sailor_creation,
    collab_edges,
    direct_influence_on_others,
    indirect_influence
]).drop_duplicates()

build_gephi_files(q2_edges, nodes_g, "q2")

# ── Gephi tips for Q2 ──
# • Layout:       Force Atlas 2 (gravity=1, scaling=10) — separates clusters
# • Node colour:  Partition → Node Type
# • Node size:    Betweenness Centrality (Statistics panel → run it first)
#                 Larger nodes = bridges between clusters
# • Edge colour:  Partition → Type  (collab vs influence visually distinct)
# • Filter:       Use Ego Network filter centred on Sailor Shift to highlight
#                 just her 1-hop or 2-hop neighbourhood


# ═══════════════════════════════════════════════════════════════════════════════
# Q3 — How has Sailor Shift influenced the broader Oceanus Folk community?
# ═══════════════════════════════════════════════════════════════════════════════
#
# Logic:
#   1. Find all Oceanus Folk songs and the people who made them
#   2. Check which of those songs/people reference Sailor's works
#   3. Also find Oceanus Folk artists who are stylistically connected to Sailor
#      even indirectly (via shared influence chains)

print("\n── Q3: Sailor Shift's influence on Oceanus Folk ──")

# Oceanus Folk songs and their creators
oceanus_songs = nodes_g[
    (nodes_g["genre"] == "Oceanus Folk") &
    (nodes_g["Node Type"] == "Song")
]["Id"].unique()

oceanus_creators = edges_g[
    (edges_g["Target"].isin(oceanus_songs)) &
    (edges_g["Type"].isin(COLLAB_TYPES))
]["Source"].unique()

# Sailor's songs (already computed above)
# Oceanus Folk songs that directly reference Sailor's works
oceanus_influence_edges = edges_g[
    (edges_g["Source"].isin(oceanus_songs)) &
    (edges_g["Target"].isin(sailor_songs)) &
    (edges_g["Type"].isin(INFLUENCE_TYPES))
]

# Oceanus Folk artists who are InStyleOf Sailor directly
artist_style_edges = edges_g[
    (edges_g["Source"].isin(oceanus_creators)) &
    (edges_g["Target"] == SAILOR_ID) &
    (edges_g["Type"] == "InStyleOf")
]

# Sailor's creation edges (so her songs appear as nodes in the graph)
sailor_songs_edges = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["Type"].isin(COLLAB_TYPES)) &
    (edges_g["Target"].isin(sailor_songs))
]

# Oceanus Folk creators → their songs (so community structure is visible)
oceanus_creation_edges = edges_g[
    (edges_g["Source"].isin(oceanus_creators)) &
    (edges_g["Target"].isin(oceanus_songs)) &
    (edges_g["Type"].isin(COLLAB_TYPES))
]

q3_edges = pd.concat([
    oceanus_influence_edges,
    artist_style_edges,
    sailor_songs_edges,
    oceanus_creation_edges
]).drop_duplicates()

build_gephi_files(q3_edges, nodes_g, "q3")

# ── Gephi tips for Q3 ──
# • Layout:       Force Atlas 2 — the Oceanus Folk community will cluster
# • Node colour:  Partition → genre  (Oceanus Folk one colour, others another)
#                 OR Partition → Node Type
# • Node size:    In-Degree (songs/people that many others reference = larger)
# • Edge arrow:   Make sure directed arrows are ON — direction shows flow of
#                 influence FROM Sailor's works INTO the community
# • Highlight:    Use the Filter panel → Partition → select Sailor Shift's node
#                 then Expand to see her reach into the community


print("\n✅ All done! Import each pair into Gephi:")
print("   File > Open  →  select q1_nodes.csv  (choose 'Nodes table')")
print("   File > Open  →  select q1_edges.csv  (choose 'Edges table')")
print("   Repeat for q2 and q3 pairs.")
