#!/usr/bin/env python3
# =============================================================================
#  Run ONLY the datasets you list -- for loading the ones skipped on the first
#  run, without re-processing everything already in the graph.
#
#  All loaders use MERGE, so re-running a dataset never duplicates data.
#  Requires ingest_opentargets_neo4j.py (the corrected one) in the same folder.
#
#  Edit SELECT below, then:  python run_selected.py
# =============================================================================
import ingest_opentargets_neo4j as ing

# Pick the dataset keys to load. Uncomment what you want.
SELECT = {
    # ---- association roll-ups (light-to-moderate) ----
     "association_overall_indirect",
     "association_by_datatype_direct",
     "association_by_datatype_indirect",
     "association_by_datasource_direct",
     "association_by_datasource_indirect",

    # ---- opt-in extras ----
     "target_essentiality",     # Target-ESSENTIAL_IN->CellLine (DepMap)  ~small
     "literature_vector",       # Term embedding nodes                     ~small

    # ---- HEAVY: expect long runtimes through the driver ----
    # "enhancer_to_gene",        # ~49M  Enhancer-REGULATES->Target
    # "interaction_evidence",    # ~27M  Target-PPI_EVIDENCE->Target
    # "literature",              # ~164M Publication-MENTIONS->...
     "colocalisation",          # ~218M CredibleSet-COLOCALISES_WITH->CredibleSet
}

def main():
    if not SELECT:
        print("Nothing selected. Uncomment dataset keys in SELECT and re-run.")
        return
    print("=" * 66)
    print(f"  RUN SELECTED: {sorted(SELECT)}")
    print("=" * 66)

    specs = ing.build_specs()
    todo = [s for s in specs if s[0] in SELECT]
    if not todo:
        print("  No matching loaders found -- check the keys against ENABLED names.")
        return

    for key, sql, cypher, label, required, count in todo:
        try:
            ing.ingest(key, sql, cypher, label, required=required, count=count)
        except Exception as e:
            print(f"  !! ERROR on {label}: {e}")

    ing.driver.close()
    ing.duck.close()
    print("\nSelected datasets loaded.")

if __name__ == "__main__":
    main()
