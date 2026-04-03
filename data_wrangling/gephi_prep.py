"""
Gephi Data Preparation for Sailor Shift Career Analysis
========================================================
Run this script once. It outputs 6 CSV files (nodes + edges for each question)
that you import directly into Gephi.

Usage:
    python gephi_prep.py

Output files:
    q1a_*_nodes.csv / q1a_*_edges.csv  — Influences ON Sailor Shift (over time)
    q1b_artists_nodes.csv / q1b_artists_edges.csv  — Collaborators & people she influenced
    q1c_artists_nodes.csv / q1c_artists_edges.csv  — Her influence on Oceanus Folk community
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
    "Edge Type": "EdgeType"  # "Type" is reserved by Gephi — use EdgeType for partition colouring
})

# Add a "year" column for Gephi's Timeline feature (uses release_date on songs)
nodes_g["year"] = nodes_g["release_date"].fillna(nodes_g["written_date"])

# Q1a — Four layered graphs
SAILOR_ID = nodes[nodes["name"] == "Sailor Shift"]["id"].values[0]

INFLUENCE_TYPES = [
    "InStyleOf",
    "InterpolatesFrom",
    "DirectlySamples",
    "CoverOf",
    "LyricalReferenceTo",
]
CREATION_TYPES = ["PerformerOf", "ComposerOf", "LyricistOf", "ProducerOf"]

# Rename to Gephi convention
nodes_g = nodes.rename(columns={"id": "Id", "name": "Label"})
nodes_g["year"] = nodes_g["release_date"].fillna(nodes_g["written_date"])

edges_g = edges.rename(columns={
    "source": "Source",
    "target": "Target",
    "Edge Type": "EdgeType"
})

def save(edges_df, prefix):
    relevant_ids = set(edges_df["Source"]) | set(edges_df["Target"])
    sub_nodes = nodes_g[nodes_g["Id"].isin(relevant_ids)].copy()
    edges_df.to_csv(f"{prefix}_edges.csv", index=False)
    sub_nodes.to_csv(f"{prefix}_nodes.csv", index=False)
    print(f"  {prefix}: {len(sub_nodes)} nodes, {len(edges_df)} edges")

# ── Shared base: Sailor's songs ───────────────────────────────────────────────
sailor_song_ids = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]["Target"].unique()

# Influence edges FROM Sailor's songs → influenced songs/albums
song_influence = edges_g[
    (edges_g["Source"].isin(sailor_song_ids)) &
    (edges_g["EdgeType"].isin(INFLUENCE_TYPES))
][["Source", "Target", "EdgeType"]]

# Direct influence edges FROM Sailor → Person/Group (e.g. direct InStyleOf)
# Excludes MemberOf intentionally
valid_person_ids = set(nodes_g[nodes_g["Node Type"].isin(["Person", "MusicalGroup"])]["Id"])
direct_influence = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(INFLUENCE_TYPES)) &
    (edges_g["Target"].isin(valid_person_ids))
][["Source", "Target", "EdgeType"]]

# Sailor → her own songs (creation edges, for context in q1a full graph)
sailor_creation = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (edges_g["Target"].isin(sailor_song_ids))
][["Source", "Target", "EdgeType"]]

print("── Q1a: Full graph (Sailor songs → influenced songs/albums) ──")
q1_edges = pd.concat([
    sailor_creation,
    song_influence,
    direct_influence
]).drop_duplicates()
save(q1_edges, "q1a")

# ── Q1a CLEAN: Collapse sailor songs out — Sailor → influenced songs/albums ───
print("── Q1a Clean: Sailor → influenced songs/albums ──")
song_influence_remapped = song_influence.copy()
song_influence_remapped["Source"] = SAILOR_ID

q1_clean_edges = pd.concat([
    song_influence_remapped,
    direct_influence
]).drop_duplicates(subset=["Source", "Target", "EdgeType"])
save(q1_clean_edges, "q1a_clean")

# ── Q1a EXTENDED: Add creator layer — songs → their artists ───────────────────
print("── Q1a Extended: Sailor → influenced songs/albums → their artists ──")
influence_target_ids = song_influence["Target"].unique()

creator_edges = edges_g[
    (edges_g["Target"].isin(influence_target_ids)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
][["Source", "Target", "EdgeType"]].copy()
creator_edges["EdgeCategory"] = "creation"

influence_tagged = q1_clean_edges.copy()
influence_tagged["EdgeCategory"] = "influence"

q1_extended_edges = pd.concat([
    influence_tagged,
    creator_edges
]).drop_duplicates()
save(q1_extended_edges, "q1a_extended")

# ── Q1a ARTISTS: Collapse all — Sailor directly to artists only ───────────────
print("── Q1a Artists: Sailor → artists only (no songs/albums) ──")

# Route 1: via songs — map influenced song → its creators, keep EdgeType
creator_map = edges_g[
    (edges_g["Target"].isin(influence_target_ids)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
][["Source", "Target"]].rename(columns={"Source": "ArtistId", "Target": "SongId"})

merged = song_influence.merge(creator_map, left_on="Target", right_on="SongId")
via_songs = (
    merged.groupby(["EdgeType", "ArtistId"])
    .size()
    .reset_index(name="Weight")
)
via_songs["Source"] = SAILOR_ID
via_songs = via_songs.rename(columns={"ArtistId": "Target"})[
    ["Source", "Target", "EdgeType", "Weight"]
]
via_songs = via_songs[via_songs["Target"].isin(valid_person_ids)]

# Route 2: direct Sailor → Person/Group influence edges
direct_tagged = direct_influence.copy()
direct_tagged["Weight"] = 1

q1_artists_edges = pd.concat([
    via_songs,
    direct_tagged
]).drop_duplicates(subset=["Source", "Target", "EdgeType"])

save(q1_artists_edges, "q1a_artists")

print("\n✅ All 4 Q1a CSVs rebuilt consistently.")
print("\nStory layers:")
print("  q1a          — Most granular: which specific songs carried which influences")
print("  q1_clean    — Mid level: what songs/albums Sailor drew from")
print("  q1_extended — With context: who made those influenced songs")
print("  q1_artists  — Clearest: just Sailor and the artists who influenced her")


# ═══════════════════════════════════════════════════════════════════════════════
# Q1b — Who has Sailor Shift collaborated with and directly or indirectly influenced?
# ═══════════════════════════════════════════════════════════════════════════════
#
# All graphs are artist-only (Person/MusicalGroup) — no song/album nodes.
# Output: q1b_artists_nodes.csv / q1b_artists_edges.csv
#
# Three relationship types encoded in the Relationship column:
#   Collaborator          — shared song credits on Sailor's songs (48 artists)
#   DirectlyInfluenced    — artists that point TO Sailor with influence edges (7)
#   IndirectlyInfluenced  — Person/MusicalGroup artists that reference
#                           collaborator songs (4, RecordLabels excluded)
#
# Gephi: Node colour → Partition → Relationship
#        Edge colour → Partition → EdgeType

# ── Shared base ───────────────────────────────────────────────────────────────
# Sailor's songs
sailor_songs = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]["Target"].unique()

# 1. COLLABORATOR edges — others credited on Sailor's songs
collab_edges = edges_g[
    (edges_g["Target"].isin(sailor_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (edges_g["Source"] != SAILOR_ID)
][["Source","Target","EdgeType"]].copy()
collab_ids = set(collab_edges["Source"].unique())

# Sailor's creation edges (for full/extended graphs)
sailor_creation = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (edges_g["Target"].isin(sailor_songs))
][["Source","Target","EdgeType"]]

# 2. DIRECTLY INFLUENCED — point to Sailor herself
direct_inf_edges = edges_g[
    (edges_g["Target"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(INFLUENCE_TYPES))
][["Source","Target","EdgeType"]].copy()
direct_inf_ids = set(direct_inf_edges["Source"].unique())

# 3. INDIRECTLY INFLUENCED — reference songs made by collaborators
collab_songs = edges_g[
    (edges_g["Source"].isin(collab_ids)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]["Target"].unique()

# Songs that reference collaborator songs
indirect_ref_edges = edges_g[
    (edges_g["Target"].isin(collab_songs)) &
    (edges_g["EdgeType"].isin(INFLUENCE_TYPES))
][["Source","Target","EdgeType"]].copy()
indirect_song_ids = set(indirect_ref_edges["Source"].unique())

# Creators of those referencing songs (the indirectly influenced people)
indirect_creator_edges = edges_g[
    (edges_g["Target"].isin(indirect_song_ids)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
][["Source","Target","EdgeType"]].copy()
# Filter to Person and MusicalGroup only — excludes RecordLabels which are not influenced artists
valid_person_ids_q2 = set(nodes_g[nodes_g["Node Type"].isin(["Person", "MusicalGroup"])]["Id"])
indirect_ids = (set(indirect_creator_edges["Source"].unique()) - collab_ids - direct_inf_ids - {SAILOR_ID}) & valid_person_ids_q2

# Collab song creation edges (for extended graph)
collab_creation = edges_g[
    (edges_g["Source"].isin(collab_ids)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (edges_g["Target"].isin(sailor_songs))
][["Source","Target","EdgeType"]]

print("── Q1b: Building all graphs ──")

# ── Q1b CLEAN edges (shared base for all Q1b outputs) ──────────────────────────
# All Q1b graphs are artist-only — no song/album intermediary nodes
collab_direct = pd.DataFrame({
    "Source": SAILOR_ID,
    "Target": list(collab_ids),
    "EdgeType": "CollaboratedWith",
    "Relationship": "Collaborator"
})

direct_inf_direct = pd.DataFrame({
    "Source": list(direct_inf_ids),
    "Target": SAILOR_ID,
    "EdgeType": direct_inf_edges.set_index("Source")["EdgeType"].reindex(list(direct_inf_ids)).values,
    "Relationship": "DirectlyInfluenced"
})

indirect_direct = pd.DataFrame({
    "Source": SAILOR_ID,
    "Target": list(indirect_ids),
    "EdgeType": "IndirectlyInfluenced",
    "Relationship": "IndirectlyInfluenced"
})

q2_clean_edges = pd.concat([
    collab_direct, direct_inf_direct, indirect_direct
]).drop_duplicates()

# ── Q1b — Artist-only graph (all relationships, tagged by role) ────────────────
print("  Building q2_artists...")
valid_ids = set(nodes_g[nodes_g["Node Type"].isin(["Person","MusicalGroup"])]["Id"])

nodes_q2 = nodes_g.copy()
nodes_q2["Relationship"] = "Other"
nodes_q2.loc[nodes_q2["Id"] == SAILOR_ID, "Relationship"] = "SailorShift"
nodes_q2.loc[nodes_q2["Id"].isin(collab_ids), "Relationship"] = "Collaborator"
nodes_q2.loc[nodes_q2["Id"].isin(direct_inf_ids), "Relationship"] = "DirectlyInfluenced"
nodes_q2.loc[nodes_q2["Id"].isin(indirect_ids), "Relationship"] = "IndirectlyInfluenced"

q2_artist_edges = q2_clean_edges[
    q2_clean_edges["Target"].isin(valid_ids) | (q2_clean_edges["Source"] == SAILOR_ID)
]

relevant_ids = set(q2_artist_edges["Source"]) | set(q2_artist_edges["Target"])
sub_nodes = nodes_q2[nodes_q2["Id"].isin(relevant_ids)].copy()

q2_artist_edges.to_csv("q1b_artists_edges.csv", index=False)
sub_nodes.to_csv("q1b_artists_nodes.csv", index=False)
print(f"  q2_artists: {len(sub_nodes)} nodes, {len(q2_artist_edges)} edges")

print("\n✅ Q1b CSVs built (artist-only — no songs/albums).")
print("\nRelationship breakdown:")
print(f"  Collaborators: {len(collab_ids)}")
print(f"  Directly influenced: {len(direct_inf_ids)}")
print(f"  Indirectly influenced: {len(indirect_ids)}")
print("\nGephi tip:")
print("  Node colour → Partition → Relationship")
print("  (SailorShift / Collaborator / DirectlyInfluenced / IndirectlyInfluenced)")


# ═══════════════════════════════════════════════════════════════════════════════
# Q1c — How has Sailor Shift influenced collaborators of the broader Oceanus Folk community?
# ═══════════════════════════════════════════════════════════════════════════════
#
# All graphs are artist-only (Person/MusicalGroup) — no song/album nodes.
# Output: q1c_artists_nodes.csv / q1c_artists_edges.csv
#
# Key finding: Sailor's influence on Oceanus Folk is indirect — channelled
# through 11 collaborators who bridge her world and the OF community.
# Only 1 OF artist (Copper Canyon Ghosts) directly references Sailor.
#
# Node roles encoded in the Relationship column:
#   SailorShift        — Sailor Shift (1)
#   CollabOFArtist     — Sailor's collaborators who are also OF artists (11)
#   OFCommunity        — OF artists reached via bridge collaborators (3)
#   DirectlyInfluenced — OF artists that directly reference Sailor (1)
#
# Gephi: Node colour → Partition → Relationship
#        Edge colour → Partition → EdgeType
#        CollaboratedWith = Sailor↔bridge, SharedOFSong = bridge↔OF community

# ── Shared base ───────────────────────────────────────────────────────────────
# Oceanus Folk songs and their artists
oceanus_songs = set(nodes_g[
    (nodes_g["genre"] == "Oceanus Folk") &
    (nodes_g["Node Type"] == "Song")
]["Id"].unique())

oceanus_artists = set(edges_g[
    (edges_g["Target"].isin(oceanus_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]["Source"].unique())

# Sailor's songs and collaborators
sailor_songs = set(edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]["Target"].unique())

collab_ids = set(edges_g[
    (edges_g["Target"].isin(sailor_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (edges_g["Source"] != SAILOR_ID)
]["Source"].unique())

# KEY: collaborators who are ALSO Oceanus Folk artists — the bridge group
collab_oceanus_ids = collab_ids & oceanus_artists

# OF songs made by those bridge collaborators
collab_oceanus_songs = set(edges_g[
    (edges_g["Source"].isin(collab_oceanus_ids)) &
    (edges_g["Target"].isin(oceanus_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]["Target"].unique())

# Other OF artists who also worked on those same songs (broader community reach)
other_of_on_collab_songs = set(edges_g[
    (edges_g["Target"].isin(collab_oceanus_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (~edges_g["Source"].isin(collab_oceanus_ids)) &
    (edges_g["Source"] != SAILOR_ID)
]["Source"].unique()) & oceanus_artists

# OF artists/songs that directly reference Sailor (Copper Canyon Ghosts)
direct_of_to_sailor = edges_g[
    (edges_g["Source"].isin(oceanus_artists)) &
    (edges_g["Target"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(INFLUENCE_TYPES))
][["Source","Target","EdgeType"]]
direct_of_ids = set(direct_of_to_sailor["Source"].unique())

valid_person_ids = set(nodes_g[nodes_g["Node Type"].isin(["Person","MusicalGroup"])]["Id"])

print("── Q1c: Building artist-only graphs ──")
print(f"  Oceanus Folk songs: {len(oceanus_songs)}")
print(f"  Oceanus Folk artists: {len(oceanus_artists)}")
print(f"  Bridge collaborators (collab + OF): {len(collab_oceanus_ids)}")
print(f"  OF songs with Sailor's collab footprint: {len(collab_oceanus_songs)}")
print(f"  Other OF artists reached via bridge: {len(other_of_on_collab_songs)}")
print(f"  Direct OF → Sailor references: {len(direct_of_ids)}")
print()

# Sailor creation edges (used to identify bridge collabs, not output as nodes)
sailor_creation = edges_g[
    (edges_g["Source"] == SAILOR_ID) &
    (edges_g["EdgeType"].isin(CREATION_TYPES)) &
    (edges_g["Target"].isin(sailor_songs))
][["Source","Target","EdgeType"]]

# Bridge collabs → Sailor's songs
collab_sailor_edges = edges_g[
    (edges_g["Source"].isin(collab_oceanus_ids)) &
    (edges_g["Target"].isin(sailor_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
][["Source","Target","EdgeType"]]

# Bridge collabs → their OF songs
collab_of_edges = edges_g[
    (edges_g["Source"].isin(collab_oceanus_ids)) &
    (edges_g["Target"].isin(collab_oceanus_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
][["Source","Target","EdgeType"]]

# Other OF artists → those same OF songs
other_of_edges = edges_g[
    (edges_g["Source"].isin(other_of_on_collab_songs)) &
    (edges_g["Target"].isin(collab_oceanus_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
][["Source","Target","EdgeType"]]

# ── Q1c — All graphs are artist-only (no song/album nodes) ────────────────────
# Sailor → bridge collabs (CollaboratedWith edges)
sailor_to_bridge = pd.DataFrame({
    "Source": SAILOR_ID,
    "Target": list(collab_oceanus_ids),
    "EdgeType": "CollaboratedWith",
    "Relationship": "CollabOFArtist"
})

# Verify bridge↔other OF connections via shared songs
bridge_of_song_map = edges_g[
    (edges_g["Source"].isin(collab_oceanus_ids | other_of_on_collab_songs)) &
    (edges_g["Target"].isin(collab_oceanus_songs)) &
    (edges_g["EdgeType"].isin(CREATION_TYPES))
]
valid_bridge_other = set()
for song in collab_oceanus_songs:
    artists_on_song = set(bridge_of_song_map[bridge_of_song_map["Target"]==song]["Source"].unique())
    if artists_on_song & collab_oceanus_ids and artists_on_song & other_of_on_collab_songs:
        valid_bridge_other |= artists_on_song & other_of_on_collab_songs

# Bridge collabs → other OF artists (SharedOFSong edges)
bridge_to_other = pd.DataFrame({
    "Source": list(collab_oceanus_ids) * len(other_of_on_collab_songs),
    "Target": [x for x in other_of_on_collab_songs for _ in collab_oceanus_ids],
    "EdgeType": "SharedOFSong",
    "Relationship": "OFCommunity"
}).drop_duplicates()
bridge_to_other = bridge_to_other[bridge_to_other["Target"].isin(valid_bridge_other)]

# Direct OF → Sailor
direct_of_clean = direct_of_to_sailor.copy()
direct_of_clean["Relationship"] = "DirectlyInfluenced"

q3_base_edges = pd.concat([
    sailor_to_bridge,
    bridge_to_other,
    direct_of_clean
]).drop_duplicates()

# Tag nodes with their role
nodes_q3 = nodes_g.copy()
nodes_q3["Relationship"] = "Other"
nodes_q3.loc[nodes_q3["Id"] == SAILOR_ID, "Relationship"] = "SailorShift"
nodes_q3.loc[nodes_q3["Id"].isin(collab_oceanus_ids), "Relationship"] = "CollabOFArtist"
nodes_q3.loc[nodes_q3["Id"].isin(valid_bridge_other & valid_person_ids), "Relationship"] = "OFCommunity"
nodes_q3.loc[nodes_q3["Id"].isin(direct_of_ids), "Relationship"] = "DirectlyInfluenced"

# Filter to Person/MusicalGroup only
q3_artist_edges = q3_base_edges[
    (q3_base_edges["Source"].isin(valid_person_ids) | (q3_base_edges["Source"] == SAILOR_ID)) &
    (q3_base_edges["Target"].isin(valid_person_ids) | (q3_base_edges["Target"] == SAILOR_ID))
]
relevant_ids = set(q3_artist_edges["Source"]) | set(q3_artist_edges["Target"])
sub_nodes_q3 = nodes_q3[nodes_q3["Id"].isin(relevant_ids)].copy()

q3_artist_edges.to_csv("q1c_artists_edges.csv", index=False)
sub_nodes_q3.to_csv("q1c_artists_nodes.csv", index=False)
print(f"  q3_artists: {len(sub_nodes_q3)} nodes, {len(q3_artist_edges)} edges")

print("\n✅ Q1c CSVs built (artist-only — no songs/albums).")
print("\nNode roles:")
print(sub_nodes_q3["Relationship"].value_counts().to_string())
print("\nGephi tips:")
print("  Node colour → Partition → Relationship")
print("  SailorShift / CollabOFArtist / OFCommunity / DirectlyInfluenced")
print("  Edge colour → Partition → EdgeType")
print("  CollaboratedWith = Sailor↔bridge, SharedOFSong = bridge↔OF community")


print("\n✅ All done! Import each pair into Gephi:")
print("   File > Open  →  select q1a_nodes.csv  (choose 'Nodes table')")
print("   File > Open  →  select q1a_edges.csv  (choose 'Edges table')")
print("   Repeat for q1b_artists and q1c_artists pairs.")