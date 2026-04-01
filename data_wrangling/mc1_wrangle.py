
"""
VAST Challenge 2025 MC1 - Improved wrangling script for Tableau
---------------------------------------------------------------
Creates normalized bridge tables + a few summary tables that are safer for Tableau
than one giant flattened table.

Required input files in same folder:
- mc1_nodes.csv
- mc1_edges.csv

Outputs:
- works_master.csv
- contributors_bridge.csv
- influence_bridge.csv
- memberof_bridge.csv
- sailor_shift_career_v2.csv
- sailor_shift_influences_v2.csv
- oceanus_folk_spread_v2.csv
- oceanus_folk_year_summary.csv
- artist_careers_v2.csv
"""

from __future__ import annotations
import os
import pandas as pd

SAILOR_NAME = "Sailor Shift"
INFLUENCE_TYPES = ["InterpolatesFrom", "InStyleOf", "CoverOf", "DirectlySamples", "LyricalReferenceTo"]
WORK_ROLE_TYPES = ["PerformerOf", "ComposerOf", "LyricistOf", "ProducerOf"]
LABEL_ROLE_TYPES = ["RecordedBy", "DistributedBy"]

def first_valid(series):
    vals = [v for v in series if pd.notna(v) and str(v).strip() != ""]
    return vals[0] if vals else pd.NA

def join_unique(series):
    vals = sorted({str(v).strip() for v in series if pd.notna(v) and str(v).strip() != ""})
    return "; ".join(vals)

def load_inputs():
    nodes = pd.read_csv("mc1_nodes.csv")
    edges = pd.read_csv("mc1_edges.csv")
    return nodes, edges

def normalize_nodes(nodes):
    out = nodes.copy()
    out["id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
    for c in ["release_date", "written_date", "notoriety_date"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    out["stage_name"] = out["stage_name"].astype("string")
    out["name"] = out["name"].astype("string")
    out["display_name"] = out["stage_name"].fillna("").str.strip()
    out["display_name"] = out["display_name"].where(out["display_name"] != "", out["name"])
    out["is_work"] = out["Node Type"].isin(["Song", "Album"])
    out["is_person_or_group"] = out["Node Type"].isin(["Person", "MusicalGroup"])
    out["is_oceanus_folk"] = out["genre"].eq("Oceanus Folk")
    return out

def normalize_edges(edges):
    out = edges.copy()
    out["source"] = pd.to_numeric(out["source"], errors="coerce").astype("Int64")
    out["target"] = pd.to_numeric(out["target"], errors="coerce").astype("Int64")
    out["key"] = pd.to_numeric(out["key"], errors="coerce").astype("Int64")
    return out

def enrich_edges(edges, nodes):
    src = nodes.add_prefix("source_")
    tgt = nodes.add_prefix("target_")
    out = edges.merge(src, left_on="source", right_on="source_id", how="left")
    out = out.merge(tgt, left_on="target", right_on="target_id", how="left")
    return out

def build_works_master(nodes, enriched):
    works = nodes[nodes["is_work"]].copy()
    work_roles = enriched[enriched["Edge Type"].isin(WORK_ROLE_TYPES)].copy()
    label_roles = enriched[enriched["Edge Type"].isin(LABEL_ROLE_TYPES)].copy()

    if not work_roles.empty:
        role_pivot = (work_roles.groupby(["target", "Edge Type"])["source_display_name"].agg(join_unique).unstack(fill_value="").reset_index().rename(columns={"target": "id"}))
        role_count = (work_roles.groupby(["target", "Edge Type"])["source"].nunique().unstack(fill_value=0).reset_index().rename(columns={"target": "id"}))
    else:
        role_pivot = pd.DataFrame({"id": []})
        role_count = pd.DataFrame({"id": []})

    if not label_roles.empty:
        label_pivot = (label_roles.groupby(["source", "Edge Type"])["target_name"].agg(join_unique).unstack(fill_value="").reset_index().rename(columns={"source": "id"}))
        label_count = (label_roles.groupby(["source", "Edge Type"])["target"].nunique().unstack(fill_value=0).reset_index().rename(columns={"source": "id"}))
    else:
        label_pivot = pd.DataFrame({"id": []})
        label_count = pd.DataFrame({"id": []})

    out = works.merge(role_pivot, on="id", how="left").merge(role_count, on="id", how="left", suffixes=("_names", "_count"))
    out = out.merge(label_pivot, on="id", how="left").merge(label_count, on="id", how="left", suffixes=("_names2", "_count2"))

    rename_map = {
        "name": "work_name","Node Type": "work_type","release_date": "release_year",
        "written_date": "written_year","notoriety_date": "notoriety_year","genre": "genre",
        "notable": "notable","single": "single",
        "PerformerOf_names": "performers","ComposerOf_names": "composers",
        "LyricistOf_names": "lyricists","ProducerOf_names": "producers",
        "RecordedBy_names2": "recorded_by_labels","DistributedBy_names2": "distributed_by_labels",
        "PerformerOf_count": "num_performers","ComposerOf_count": "num_composers",
        "LyricistOf_count": "num_lyricists","ProducerOf_count": "num_producers",
        "RecordedBy_count2": "num_recorded_by_labels","DistributedBy_count2": "num_distributed_by_labels",
    }
    out = out.rename(columns=rename_map)
    influence = enriched[enriched["Edge Type"].isin(INFLUENCE_TYPES)].copy()
    outgoing = influence.groupby("source").size().rename("draws_from_count")
    incoming = influence.groupby("target").size().rename("influenced_others_count")
    out = out.merge(outgoing, left_on="id", right_index=True, how="left")
    out = out.merge(incoming, left_on="id", right_index=True, how="left")
    for c in ["draws_from_count","influenced_others_count","num_performers","num_composers","num_lyricists","num_producers","num_recorded_by_labels","num_distributed_by_labels"]:
        if c in out.columns:
            out[c] = out[c].fillna(0).astype("Int64")
    keep = ["id","work_name","work_type","genre","release_year","written_year","notoriety_year","notable","single","is_oceanus_folk","performers","composers","lyricists","producers","num_performers","num_composers","num_lyricists","num_producers","recorded_by_labels","distributed_by_labels","num_recorded_by_labels","num_distributed_by_labels","draws_from_count","influenced_others_count"]
    for c in keep:
        if c not in out.columns:
            out[c] = pd.NA
    return out[keep].sort_values(["release_year","work_name"], na_position="last")

def build_contributors_bridge(enriched):
    df = enriched[enriched["Edge Type"].isin(WORK_ROLE_TYPES + LABEL_ROLE_TYPES)].copy()
    df["role_group"] = df["Edge Type"].map(lambda x: "label" if x in LABEL_ROLE_TYPES else "contributor")
    out = df[["Edge Type","role_group","source","source_name","source_display_name","source_Node Type","target","target_name","target_Node Type","target_genre","target_release_date"]].rename(columns={"Edge Type":"edge_type","source":"source_id","source_name":"source_name","source_display_name":"source_display_name","source_Node Type":"source_type","target":"work_id","target_name":"work_name","target_Node Type":"work_type","target_genre":"work_genre","target_release_date":"work_release_year"})
    return out.sort_values(["work_release_year","work_id","edge_type","source_display_name"], na_position="last")

def build_influence_bridge(enriched):
    df = enriched[enriched["Edge Type"].isin(INFLUENCE_TYPES)].copy()
    out = df[["Edge Type","source","source_name","source_display_name","source_Node Type","source_genre","source_release_date","target","target_name","target_display_name","target_Node Type","target_genre","target_release_date"]].rename(columns={"Edge Type":"influence_type","source":"source_id","source_name":"source_name","source_display_name":"source_display_name","source_Node Type":"source_type","source_genre":"source_genre","source_release_date":"source_year","target":"target_id","target_name":"target_name","target_display_name":"target_display_name","target_Node Type":"target_type","target_genre":"target_genre","target_release_date":"target_year"})
    return out.sort_values(["source_year","target_year","influence_type"], na_position="last")

def build_memberof_bridge(enriched):
    df = enriched[enriched["Edge Type"].eq("MemberOf")].copy()
    return df[["source","source_name","source_display_name","source_Node Type","target","target_name","target_display_name","target_Node Type"]].rename(columns={"source":"person_id","source_name":"person_name","source_display_name":"person_display_name","source_Node Type":"person_type","target":"group_id","target_name":"group_name","target_display_name":"group_display_name","target_Node Type":"group_type"}).sort_values(["group_name","person_display_name"])

def build_sailor_tables(nodes, enriched, works_master):
    sailor_id = int(nodes.loc[nodes["name"].eq(SAILOR_NAME), "id"].iloc[0])
    member_groups = set(enriched.loc[(enriched["Edge Type"] == "MemberOf") & (enriched["source"] == sailor_id), "target"])
    direct_work_edges = enriched.loc[(enriched["source"] == sailor_id) & (enriched["Edge Type"].isin(WORK_ROLE_TYPES))].copy()
    group_performed_edges = enriched.loc[(enriched["source"].isin(member_groups)) & (enriched["Edge Type"] == "PerformerOf")].copy()
    direct_work_ids = set(direct_work_edges["target"])
    group_work_ids = set(group_performed_edges["target"])
    sailor_work_ids = direct_work_ids | group_work_ids

    involvement_rows = []
    for _, r in direct_work_edges.iterrows():
        involvement_rows.append({"work_id": r["target"],"involvement_mode": "direct_person_edge","role": r["Edge Type"],"linked_group": pd.NA})
    for _, r in group_performed_edges.iterrows():
        involvement_rows.append({"work_id": r["target"],"involvement_mode": "via_group_membership","role": "PerformerOf","linked_group": r["source_name"]})
    involvement = pd.DataFrame(involvement_rows)
    inv_agg = (involvement.groupby("work_id").agg(sailor_involvement_modes=("involvement_mode", join_unique),sailor_roles=("role", join_unique),sailor_linked_groups=("linked_group", join_unique)).reset_index())

    career = works_master[works_master["id"].isin(sailor_work_ids)].copy()
    career = career.merge(inv_agg, left_on="id", right_on="work_id", how="left")
    career["is_direct_person_work"] = career["id"].isin(direct_work_ids)
    career["is_group_membership_work"] = career["id"].isin(group_work_ids)

    contrib = enriched[(enriched["target"].isin(sailor_work_ids)) & (enriched["Edge Type"].isin(WORK_ROLE_TYPES)) & (enriched["source"] != sailor_id)].copy()
    collab = (contrib.groupby("target").agg(collaborators=("source_display_name", join_unique),num_distinct_collaborators=("source", "nunique")).reset_index().rename(columns={"target":"id"}))
    career = career.merge(collab, on="id", how="left")
    career["num_distinct_collaborators"] = career["num_distinct_collaborators"].fillna(0).astype("Int64")
    career = career.sort_values(["release_year","work_name"], na_position="last")

    inf = enriched[enriched["Edge Type"].isin(INFLUENCE_TYPES)].copy()
    sailor_inf = inf[(inf["source"].eq(sailor_id)) | (inf["target"].eq(sailor_id)) | (inf["source"].isin(sailor_work_ids)) | (inf["target"].isin(sailor_work_ids))].copy()

    def classify_direction(r):
        if r["source"] == sailor_id or r["source"] in sailor_work_ids:
            return "Sailor -> Other"
        if r["target"] == sailor_id or r["target"] in sailor_work_ids:
            return "Other -> Sailor"
        return "Unclassified"

    sailor_inf["direction"] = sailor_inf.apply(classify_direction, axis=1)
    sailor_inf_out = sailor_inf[["direction","Edge Type","source","source_name","source_display_name","source_Node Type","source_genre","source_release_date","target","target_name","target_display_name","target_Node Type","target_genre","target_release_date"]].rename(columns={"Edge Type":"influence_type","source":"source_id","source_name":"source_name","source_display_name":"source_display_name","source_Node Type":"source_type","source_genre":"source_genre","source_release_date":"source_year","target":"target_id","target_name":"target_name","target_display_name":"target_display_name","target_Node Type":"target_type","target_genre":"target_genre","target_release_date":"target_year"}).sort_values(["direction","source_year","target_year"], na_position="last")
    return career, sailor_inf_out

def build_oceanus_tables(nodes, enriched):
    of_ids = set(nodes.loc[nodes["genre"].eq("Oceanus Folk") & nodes["Node Type"].isin(["Song","Album"]), "id"])
    inf = enriched[enriched["Edge Type"].isin(INFLUENCE_TYPES)].copy()
    of_inf = inf[(inf["source"].isin(of_ids)) | (inf["target"].isin(of_ids))].copy()

    def direction(row):
        src_of = row["source"] in of_ids
        tgt_of = row["target"] in of_ids
        if src_of and not tgt_of: return "OF -> Other"
        if not src_of and tgt_of: return "Other -> OF"
        if src_of and tgt_of: return "OF -> OF"
        return "Other -> Other"

    of_inf["direction"] = of_inf.apply(direction, axis=1)
    of_inf["other_genre"] = pd.NA
    of_inf.loc[of_inf["direction"].eq("OF -> Other"), "other_genre"] = of_inf["target_genre"]
    of_inf.loc[of_inf["direction"].eq("Other -> OF"), "other_genre"] = of_inf["source_genre"]

    spread = of_inf[["direction","Edge Type","source","source_name","source_display_name","source_Node Type","source_genre","source_release_date","target","target_name","target_display_name","target_Node Type","target_genre","target_release_date","other_genre"]].rename(columns={"Edge Type":"influence_type","source":"source_id","source_name":"source_name","source_display_name":"source_display_name","source_Node Type":"source_type","source_genre":"source_genre","source_release_date":"source_year","target":"target_id","target_name":"target_name","target_display_name":"target_display_name","target_Node Type":"target_type","target_genre":"target_genre","target_release_date":"target_year"}).sort_values(["direction","source_year","target_year"], na_position="last")

    of_works = nodes[nodes["id"].isin(of_ids)].copy()
    yearly_release = (of_works.groupby("release_date").agg(of_works_released=("id","nunique"),of_notable_works=("notable", lambda s: int(pd.Series(s).fillna(False).sum()))).reset_index().rename(columns={"release_date":"year"}))
    out_counts = (of_inf[of_inf["direction"].eq("OF -> Other")].groupby("source_release_date").size().rename("of_to_other_influence_edges").reset_index().rename(columns={"source_release_date":"year"}))
    in_counts = (of_inf[of_inf["direction"].eq("Other -> OF")].groupby("target_release_date").size().rename("other_to_of_influence_edges").reset_index().rename(columns={"target_release_date":"year"}))
    year_summary = yearly_release.merge(out_counts, on="year", how="left").merge(in_counts, on="year", how="left")
    year_summary["of_to_other_influence_edges"] = year_summary["of_to_other_influence_edges"].fillna(0).astype("Int64")
    year_summary["other_to_of_influence_edges"] = year_summary["other_to_of_influence_edges"].fillna(0).astype("Int64")
    return spread, year_summary.sort_values("year")

def build_artist_careers(nodes, enriched, influence_bridge):
    artist_nodes = nodes[nodes["Node Type"].isin(["Person","MusicalGroup"])][["id","name","display_name","Node Type","stage_name"]].copy()
    perf = enriched[enriched["Edge Type"].eq("PerformerOf")].copy()
    perf_summary = (perf.groupby("source").agg(num_performed_works=("target","nunique"),first_performance_year=("target_release_date","min"),last_performance_year=("target_release_date","max"),primary_genre=("target_genre", lambda s: first_valid(pd.Series(s).mode())),all_performance_genres=("target_genre", join_unique),num_notable_performed_works=("target_notable", lambda s: int(pd.Series(s).fillna(False).sum()))).reset_index().rename(columns={"source":"id"}))
    all_roles = enriched[enriched["Edge Type"].isin(WORK_ROLE_TYPES)].copy()
    role_summary = (all_roles.groupby("source").agg(num_all_contributed_works=("target","nunique"),all_role_types=("Edge Type", join_unique),num_distinct_collaborators=("target", "nunique")).reset_index().rename(columns={"source":"id"}))
    work_owner = perf[["source","target"]].drop_duplicates().rename(columns={"source":"id","target":"work_id"})
    influence_received = (influence_bridge[influence_bridge["target_id"].isin(work_owner["work_id"])].merge(work_owner, left_on="target_id", right_on="work_id", how="left").groupby("id").size().rename("performed_work_influence_received").reset_index())
    out = artist_nodes.merge(perf_summary, on="id", how="left").merge(role_summary, on="id", how="left").merge(influence_received, on="id", how="left")
    cond = (out["num_performed_works"].fillna(0).ge(3) | out["num_all_contributed_works"].fillna(0).ge(3))
    out = out[cond].copy()
    out["career_span_years"] = out["last_performance_year"] - out["first_performance_year"]
    for c in ["num_performed_works","num_all_contributed_works","num_notable_performed_works","performed_work_influence_received"]:
        out[c] = out[c].fillna(0).astype("Int64")
    out["career_span_years"] = out["career_span_years"].fillna(0).astype("Int64")
    out["is_oceanus_folk_artist"] = out["all_performance_genres"].fillna("").str.contains("Oceanus Folk", regex=False)
    out = out.rename(columns={"id":"artist_id","name":"artist_name","display_name":"artist_display_name","Node Type":"artist_type"})
    keep = ["artist_id","artist_name","artist_display_name","artist_type","stage_name","num_performed_works","num_all_contributed_works","all_role_types","primary_genre","all_performance_genres","first_performance_year","last_performance_year","career_span_years","num_notable_performed_works","performed_work_influence_received","is_oceanus_folk_artist"]
    for c in keep:
        if c not in out.columns:
            out[c] = pd.NA
    return out[keep].sort_values(["performed_work_influence_received","num_performed_works"], ascending=[False,False])

def build_sailor_influence_chain(sailor_inf: pd.DataFrame, influence_bridge: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a table of direct AND indirect influence from Sailor Shift.

    Direct   — works/entities that reference SS or her works (direction = "Sailor -> Other")
    Indirect — works that reference those direct targets (one hop further)

    Columns:
        influence_degree        : "Direct" or "Indirect"
        influence_type          : edge type (InStyleOf, CoverOf, etc.)
        influenced_entity       : name of the work/entity being influenced
        influenced_entity_genre : its genre
        influenced_entity_year  : its release year
        ss_work_or_reference    : the SS work it drew from (Direct) or the intermediate work (Indirect)
        reference_genre         : genre of that reference work
        reference_year          : release year of that reference work
        via                     : "Sailor Shift" for Direct rows, intermediate work name for Indirect
    """
    direct = sailor_inf[sailor_inf["direction"] == "Sailor -> Other"].copy()
    direct_target_ids = set(direct["target_id"].dropna().astype(int))
    direct_target_names = set(direct["target_display_name"].dropna())

    # Direct rows
    direct_out = direct[[
        "influence_type", "source_display_name", "source_genre",
        "target_display_name", "target_genre", "target_year"
    ]].copy()
    direct_out["influence_degree"] = "Direct"
    direct_out["via"] = "Sailor Shift"
    direct_out.columns = [
        "influence_type", "influenced_entity", "influenced_entity_genre",
        "ss_work_or_reference", "reference_genre", "reference_year",
        "influence_degree", "via"
    ]

    # Indirect rows — works that reference the direct targets,
    # excluding SS's own works and the direct targets themselves
    ss_work_names = set(direct["source_display_name"].dropna())
    indirect = influence_bridge[
        (influence_bridge["target_id"].isin(direct_target_ids)) &
        (~influence_bridge["source_display_name"].isin(direct_target_names)) &
        (~influence_bridge["source_display_name"].isin(ss_work_names))
    ].copy()

    indirect_out = indirect[[
        "influence_type", "source_display_name", "source_genre",
        "target_display_name", "target_genre", "target_year"
    ]].copy()
    indirect_out["influence_degree"] = "Indirect"
    indirect_out["via"] = indirect["target_display_name"].values
    indirect_out.columns = [
        "influence_type", "influenced_entity", "influenced_entity_genre",
        "ss_work_or_reference", "reference_genre", "reference_year",
        "influence_degree", "via"
    ]

    combined = pd.concat([direct_out, indirect_out], ignore_index=True)
    combined["reference_year"] = pd.to_numeric(combined["reference_year"], errors="coerce").astype("Int64")
    return combined.sort_values(["influence_degree", "influenced_entity_genre", "influenced_entity"], na_position="last")


def main():
    nodes, edges = load_inputs()
    nodes = normalize_nodes(nodes)
    edges = normalize_edges(edges)
    enriched = enrich_edges(edges, nodes)
    works_master = build_works_master(nodes, enriched)
    contributors_bridge = build_contributors_bridge(enriched)
    influence_bridge = build_influence_bridge(enriched)
    memberof_bridge = build_memberof_bridge(enriched)
    sailor_career, sailor_inf = build_sailor_tables(nodes, enriched, works_master)
    of_spread, of_year = build_oceanus_tables(nodes, enriched)
    artist_careers = build_artist_careers(nodes, enriched, influence_bridge)
    sailor_influence_chain = build_sailor_influence_chain(sailor_inf, influence_bridge)
    outputs = {
        "works_master.csv": works_master,
        "contributors_bridge.csv": contributors_bridge,
        "influence_bridge.csv": influence_bridge,
        "memberof_bridge.csv": memberof_bridge,
        "sailor_shift_career_v2.csv": sailor_career,
        "sailor_shift_influences_v2.csv": sailor_inf,
        "oceanus_folk_spread_v2.csv": of_spread,
        "oceanus_folk_year_summary.csv": of_year,
        "artist_careers_v2.csv": artist_careers,
        "ss_direct_indirect_influence.csv": sailor_influence_chain,
    }
    for fname, df in outputs.items():
        df.to_csv(fname, index=False)
        print(f"Saved {fname}: {len(df):,} rows x {len(df.columns)} cols")

if __name__ == "__main__":
    main()
