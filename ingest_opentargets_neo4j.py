#!/usr/bin/env python3
# =============================================================================
#  Open Targets Platform 26.03  ->  Neo4j knowledge-graph ingestion
# -----------------------------------------------------------------------------
#  Reads the parquet datasets directly with DuckDB (streamed in batches so that
#  multi-million-row tables never have to fit in memory), flattens the nested
#  struct/array columns in SQL, and loads nodes + relationships into Neo4j with
#  batched UNWIND ... MERGE queries.
#
#  Scope for this build:
#    * ALL non-evidence datasets are ingested.
#    * The 20 `evidence_*` datasets are intentionally SKIPPED.
#    * The four giant tables (colocalisation, enhancer_to_gene,
#      interaction_evidence, literature/-vector) have ready loaders but are
#      DISABLED by default -- flip them on in ENABLED below when you are ready.
#
#  Requirements:  pip install duckdb neo4j
#  Every DuckDB query below was validated against the real 26.03 parquet files.
# =============================================================================

import time
import math
import duckdb
from neo4j import GraphDatabase

# -----------------------------------------------------------------------------
# 1. CONFIGURATION
# -----------------------------------------------------------------------------
NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "OpenT@123"          # <-- change me

# Folder that contains the dataset sub-directories (target/, disease/, ...).
# Windows path is fine; backslashes are normalised to forward slashes for SQL.
DATA_DIR   = r"opentargets-26.03"

BATCH_SIZE = 5000                       # rows per Neo4j write transaction
LOG_EVERY  = 1                          # print progress every N batches

# Toggle any dataset on/off here. Anything not listed defaults to True.
# The heavy tables are shipped OFF; turn them on deliberately.
ENABLED = {
    # ---- giant tables: OFF by default ----
    "colocalisation":         False,    # ~218 M rows
    "enhancer_to_gene":       False,    # ~49 M rows
    "interaction_evidence":   False,    # ~27 M rows
    "literature":             False,    # ~164 M rows
    "literature_vector":      False,
    "target_essentiality":    False,    # deeply nested DepMap; opt-in
    # ---- association roll-ups: keep the headline edge, skip the bulky variants ----
    "association_overall_direct":       True,   # the Target-ASSOCIATED_WITH-Disease spine
    "association_overall_indirect":     False,
    "association_by_datatype_direct":   False,
    "association_by_datatype_indirect": False,
    "association_by_datasource_direct": False,
    "association_by_datasource_indirect": False,
}

DATA_DIR_SQL = DATA_DIR.replace("\\", "/")

# -----------------------------------------------------------------------------
# 2. CONNECTIONS
# -----------------------------------------------------------------------------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
duck   = duckdb.connect()               # in-process, reads parquet directly


def pq(name: str) -> str:
    """read_parquet() glob for a dataset directory."""
    return f"read_parquet('{DATA_DIR_SQL}/{name}/*.parquet')"


def run_write(cypher: str, rows):
    with driver.session() as session:
        session.run(cypher, {"rows": rows})


def run_plain(cypher: str):
    with driver.session() as session:
        session.run(cypher)


# -----------------------------------------------------------------------------
# 3. STREAMING BATCH ENGINE
# -----------------------------------------------------------------------------
def clean_batch(rows, columns, required):
    """dict-ify DuckDB tuples, strip strings, drop rows missing required keys."""
    out = []
    for r in rows:
        rec = dict(zip(columns, r))
        for k, v in rec.items():
            if isinstance(v, str):
                rec[k] = v.strip()
        if required and any(rec.get(k) in (None, "") for k in required):
            continue
        out.append(rec)
    return out


def ingest(name, sql, cypher, label, required=None, count=True):
    """Stream one DuckDB query into Neo4j in batches, with verbose logging."""
    print(f"\n{'-'*66}\n  {label}\n{'-'*66}")

    total = None
    if count:
        try:
            total = duck.execute(f"SELECT count(*) FROM ({sql}) _q").fetchone()[0]
            print(f"  source rows : {total:,}")
        except Exception as e:
            print(f"  (row count skipped: {str(e)[:80]})")
    batches = math.ceil(total / BATCH_SIZE) if total else "?"
    print(f"  batches     : {batches}  (batch size {BATCH_SIZE})")

    cur = duck.execute(sql)
    columns = [d[0] for d in cur.description]

    t0, i, written = time.time(), 0, 0
    while True:
        raw = cur.fetchmany(BATCH_SIZE)
        if not raw:
            break
        i += 1
        rows = clean_batch(raw, columns, required)
        if not rows:
            print(f"    batch {i}: skipped (no valid rows after key filter)")
            continue
        run_write(cypher, rows)
        written += len(rows)
        if i % LOG_EVERY == 0:
            of = f"/{total:,}" if total else ""
            print(f"    batch {i}: +{len(rows):>5} rows  (cumulative {written:,}{of})")

    dt = time.time() - t0
    print(f"  DONE {label}: {written:,} rows in {dt:,.1f}s "
          f"({written/dt:,.0f} rows/s)" if dt > 0 else f"  DONE {label}: {written:,} rows")


# -----------------------------------------------------------------------------
# 4. CONSTRAINTS  (uniqueness = fast MERGE + de-dup)
# -----------------------------------------------------------------------------
def create_constraints():
    print("\n=== Creating constraints & indexes ===")
    stmts = [
        "CREATE CONSTRAINT target_id     IF NOT EXISTS FOR (n:Target)        REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT disease_id    IF NOT EXISTS FOR (n:Disease)       REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT hpo_id        IF NOT EXISTS FOR (n:Hpo)           REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT drug_id       IF NOT EXISTS FOR (n:Drug)          REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT variant_id    IF NOT EXISTS FOR (n:Variant)       REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT study_id      IF NOT EXISTS FOR (n:Study)         REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT credset_id    IF NOT EXISTS FOR (n:CredibleSet)   REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT biosample_id  IF NOT EXISTS FOR (n:Biosample)     REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT go_id         IF NOT EXISTS FOR (n:GOTerm)        REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT so_id         IF NOT EXISTS FOR (n:SOTerm)        REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT mp_id         IF NOT EXISTS FOR (n:MousePhenotype)REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT pathway_id    IF NOT EXISTS FOR (n:Pathway)       REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT trial_id      IF NOT EXISTS FOR (n:ClinicalTrial) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT ae_id         IF NOT EXISTS FOR (n:AdverseEvent)  REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT enhancer_id   IF NOT EXISTS FOR (n:Enhancer)      REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT cellline_id   IF NOT EXISTS FOR (n:CellLine)      REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT publication_id IF NOT EXISTS FOR (n:Publication)  REQUIRE n.id IS UNIQUE",
    ]
    for s in stmts:
        run_plain(s)
    print("Constraints ready.")


# -----------------------------------------------------------------------------
# 5. SPECS  --  (enabled_key, sql, cypher, label, required_keys)
#     Nodes are loaded before relationships. Relationship loaders MERGE their
#     endpoints defensively so ordering can never orphan an edge.
# -----------------------------------------------------------------------------
def build_specs():
    S = []
    def add(key, sql, cypher, label, required=None, count=True):
        S.append((key, sql, cypher, label, required, count))

    # ===================== NODES =====================
    add("target",
        f"""SELECT id, approvedSymbol, approvedName, biotype,
                   genomicLocation.chromosome AS chromosome,
                   genomicLocation['start']   AS geneStart,
                   genomicLocation['end']     AS geneEnd,
                   genomicLocation.strand      AS strand,
                   tss
            FROM {pq('target')}""",
        """UNWIND $rows AS row
           MERGE (n:Target {id: row.id})
           SET n.approvedSymbol=row.approvedSymbol, n.approvedName=row.approvedName,
               n.biotype=row.biotype, n.chromosome=row.chromosome,
               n.geneStart=row.geneStart, n.geneEnd=row.geneEnd,
               n.strand=row.strand, n.tss=row.tss""",
        "NODE Target", ["id"])

    add("target_prioritisation",
        f"""SELECT targetId AS id, isInMembrane, isSecreted, hasSafetyEvent, hasPocket,
                   hasLigand, hasSmallMoleculeBinder, geneticConstraint,
                   paralogMaxIdentityPercentage, mouseOrthologMaxIdentityPercentage,
                   isCancerDriverGene, hasTEP, mouseKOScore, hasHighQualityChemicalProbes,
                   maxClinicalStage, tissueSpecificity, tissueDistribution
            FROM {pq('target_prioritisation')}""",
        """UNWIND $rows AS row
           MERGE (n:Target {id: row.id})
           SET n += row""",
        "NODE Target (prioritisation attrs)", ["id"])

    add("disease",
        f"""SELECT id, name, description, therapeuticAreas FROM {pq('disease')}""",
        """UNWIND $rows AS row
           MERGE (n:Disease {id: row.id})
           SET n.name=row.name, n.description=row.description,
               n.therapeuticAreas=row.therapeuticAreas""",
        "NODE Disease", ["id"])

    add("disease_hpo",
        f"""SELECT id, name, description FROM {pq('disease_hpo')}""",
        """UNWIND $rows AS row
           MERGE (n:Hpo {id: row.id})
           SET n.name=row.name, n.description=row.description""",
        "NODE Hpo (phenotype)", ["id"])

    add("drug_molecule",
        f"""SELECT id, name, drugType, canonicalSmiles, inchiKey,
                   maximumClinicalStage, tradeNames, synonyms, parentId
            FROM {pq('drug_molecule')}""",
        """UNWIND $rows AS row
           MERGE (n:Drug {id: row.id})
           SET n.name=row.name, n.drugType=row.drugType,
               n.canonicalSmiles=row.canonicalSmiles, n.inchiKey=row.inchiKey,
               n.maximumClinicalStage=row.maximumClinicalStage,
               n.tradeNames=row.tradeNames, n.synonyms=row.synonyms""",
        "NODE Drug", ["id"])

    add("variant",
        f"""SELECT variantId AS id, chromosome, position, referenceAllele,
                   alternateAllele, mostSevereConsequenceId, rsIds, hgvsId
            FROM {pq('variant')}""",
        """UNWIND $rows AS row
           MERGE (n:Variant {id: row.id})
           SET n.chromosome=row.chromosome, n.position=row.position,
               n.referenceAllele=row.referenceAllele, n.alternateAllele=row.alternateAllele,
               n.mostSevereConsequenceId=row.mostSevereConsequenceId,
               n.rsIds=row.rsIds, n.hgvsId=row.hgvsId""",
        "NODE Variant", ["id"])

    add("study",
        f"""SELECT studyId AS id, studyType, traitFromSource, projectId,
                   nSamples, nCases, nControls, pubmedId, publicationTitle,
                   biosampleId, geneId
            FROM {pq('study')}""",
        """UNWIND $rows AS row
           MERGE (n:Study {id: row.id})
           SET n.studyType=row.studyType, n.traitFromSource=row.traitFromSource,
               n.projectId=row.projectId, n.nSamples=row.nSamples,
               n.nCases=row.nCases, n.nControls=row.nControls,
               n.pubmedId=row.pubmedId, n.publicationTitle=row.publicationTitle""",
        "NODE Study", ["id"])

    add("credible_set",
        f"""SELECT studyLocusId AS id, studyId, variantId, chromosome, position,
                   finemappingMethod, confidence, studyType, beta,
                   pValueMantissa, pValueExponent, credibleSetlog10BF
            FROM {pq('credible_set')}""",
        """UNWIND $rows AS row
           MERGE (n:CredibleSet {id: row.id})
           SET n.chromosome=row.chromosome, n.position=row.position,
               n.finemappingMethod=row.finemappingMethod, n.confidence=row.confidence,
               n.studyType=row.studyType, n.beta=row.beta,
               n.pValueMantissa=row.pValueMantissa, n.pValueExponent=row.pValueExponent,
               n.log10BF=row.credibleSetlog10BF""",
        "NODE CredibleSet", ["id"])

    add("biosample",
        f"""SELECT biosampleId AS id, biosampleName, description FROM {pq('biosample')}""",
        """UNWIND $rows AS row
           MERGE (n:Biosample {id: row.id})
           SET n.name=row.biosampleName, n.description=row.description""",
        "NODE Biosample", ["id"])

    add("go",
        f"""SELECT id, label, namespace FROM {pq('go')}""",
        """UNWIND $rows AS row
           MERGE (n:GOTerm {id: row.id})
           SET n.label=row.label, n.namespace=row.namespace""",
        "NODE GOTerm", ["id"])

    add("so",
        f"""SELECT id, label FROM {pq('so')}""",
        """UNWIND $rows AS row
           MERGE (n:SOTerm {id: row.id}) SET n.label=row.label""",
        "NODE SOTerm", ["id"])

    add("mouse_phenotype",
        f"""SELECT DISTINCT modelPhenotypeId AS id, modelPhenotypeLabel AS label
            FROM {pq('mouse_phenotype')}""",
        """UNWIND $rows AS row
           MERGE (n:MousePhenotype {id: row.id}) SET n.label=row.label""",
        "NODE MousePhenotype", ["id"])

    add("clinical_report",
        f"""SELECT id, trialPhase, trialOverallStatus, trialWhyStopped,
                   year, url, trialStudyType
            FROM {pq('clinical_report')}""",
        """UNWIND $rows AS row
           MERGE (n:ClinicalTrial {id: row.id})
           SET n.phase=row.trialPhase, n.status=row.trialOverallStatus,
               n.whyStopped=row.trialWhyStopped, n.year=row.year,
               n.url=row.url, n.studyType=row.trialStudyType""",
        "NODE ClinicalTrial", ["id"])

    # ===================== RELATIONSHIPS =====================
    # Ontology hierarchies -------------------------------------------------
    add("disease",  # SUBCLASS_OF via disease.parents
        f"""SELECT id AS child, p AS parent
            FROM {pq('disease')}, UNNEST(parents) AS pp(p)""",
        """UNWIND $rows AS row
           MERGE (c:Disease {id: row.child})
           MERGE (p:Disease {id: row.parent})
           MERGE (c)-[:SUBCLASS_OF]->(p)""",
        "REL Disease-SUBCLASS_OF->Disease", ["child", "parent"])

    add("disease_hpo",
        f"""SELECT id AS child, p AS parent
            FROM {pq('disease_hpo')}, UNNEST(parents) AS pp(p)""",
        """UNWIND $rows AS row
           MERGE (c:Hpo {id: row.child})
           MERGE (p:Hpo {id: row.parent})
           MERGE (c)-[:SUBCLASS_OF]->(p)""",
        "REL Hpo-SUBCLASS_OF->Hpo", ["child", "parent"])

    add("go",
        f"""SELECT id AS child, a AS parent
            FROM {pq('go')}, UNNEST(isA) AS aa(a)""",
        """UNWIND $rows AS row
           MERGE (c:GOTerm {id: row.child})
           MERGE (p:GOTerm {id: row.parent})
           MERGE (c)-[:IS_A]->(p)""",
        "REL GOTerm-IS_A->GOTerm", ["child", "parent"])

    add("biosample",
        f"""SELECT biosampleId AS child, p AS parent
            FROM {pq('biosample')}, UNNEST(parents) AS pp(p)""",
        """UNWIND $rows AS row
           MERGE (c:Biosample {id: row.child})
           MERGE (p:Biosample {id: row.parent})
           MERGE (c)-[:SUBCLASS_OF]->(p)""",
        "REL Biosample-SUBCLASS_OF->Biosample", ["child", "parent"])

    # Target functional annotations ---------------------------------------
    add("target",  # ANNOTATED_WITH GO
        f"""SELECT id AS targetId, gg.id AS goId, gg.aspect AS aspect, gg.evidence AS evidence
            FROM (SELECT id, unnest(go) AS gg FROM {pq('target')})""",
        """UNWIND $rows AS row
           MERGE (t:Target {id: row.targetId})
           MERGE (g:GOTerm {id: row.goId})
           MERGE (t)-[r:ANNOTATED_WITH]->(g)
           SET r.aspect=row.aspect, r.evidence=row.evidence""",
        "REL Target-ANNOTATED_WITH->GOTerm", ["targetId", "goId"])

    add("target",  # PARTICIPATES_IN pathway
        f"""SELECT id AS targetId, pw.pathwayId AS pathwayId, pw.pathway AS pathway,
                   pw.topLevelTerm AS topLevelTerm
            FROM (SELECT id, unnest(pathways) AS pw FROM {pq('target')})""",
        """UNWIND $rows AS row
           MERGE (t:Target {id: row.targetId})
           MERGE (p:Pathway {id: row.pathwayId})
           SET p.name=row.pathway, p.topLevelTerm=row.topLevelTerm
           MERGE (t)-[:PARTICIPATES_IN]->(p)""",
        "REL Target-PARTICIPATES_IN->Pathway", ["targetId", "pathwayId"])

    # Disease -> phenotype -------------------------------------------------
    add("disease_phenotype",
        f"""SELECT disease AS diseaseId, phenotype AS hpoId
            FROM {pq('disease_phenotype')}""",
        """UNWIND $rows AS row
           MERGE (d:Disease {id: row.diseaseId})
           MERGE (h:Hpo {id: row.hpoId})
           MERGE (d)-[:HAS_PHENOTYPE]->(h)""",
        "REL Disease-HAS_PHENOTYPE->Hpo", ["diseaseId", "hpoId"])

    # Drug hierarchy & pharmacology ---------------------------------------
    add("drug_molecule",
        f"""SELECT id AS childId, parentId FROM {pq('drug_molecule')} WHERE parentId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (c:Drug {id: row.childId})
           MERGE (p:Drug {id: row.parentId})
           MERGE (c)-[:HAS_PARENT]->(p)""",
        "REL Drug-HAS_PARENT->Drug", ["childId", "parentId"])

    add("drug_mechanism_of_action",
        f"""SELECT d.drugId AS drugId, t.targetId AS targetId,
                   m.actionType AS actionType, m.mechanismOfAction AS moa
            FROM {pq('drug_mechanism_of_action')} m,
                 UNNEST(m.chemblIds) AS d(drugId),
                 UNNEST(m.targets)   AS t(targetId)""",
        """UNWIND $rows AS row
           MERGE (d:Drug {id: row.drugId})
           MERGE (t:Target {id: row.targetId})
           MERGE (d)-[r:TARGETS]->(t)
           SET r.actionType=row.actionType, r.mechanismOfAction=row.moa""",
        "REL Drug-TARGETS->Target", ["drugId", "targetId"])

    add("clinical_indication",
        f"""SELECT drugId, diseaseId, maxClinicalStage FROM {pq('clinical_indication')}""",
        """UNWIND $rows AS row
           MERGE (d:Drug {id: row.drugId})
           MERGE (s:Disease {id: row.diseaseId})
           MERGE (d)-[r:INDICATED_FOR]->(s)
           SET r.maxClinicalStage=row.maxClinicalStage""",
        "REL Drug-INDICATED_FOR->Disease", ["drugId", "diseaseId"])

    add("clinical_target",
        f"""SELECT drugId, targetId, maxClinicalStage FROM {pq('clinical_target')}""",
        """UNWIND $rows AS row
           MERGE (d:Drug {id: row.drugId})
           MERGE (t:Target {id: row.targetId})
           MERGE (d)-[r:CLINICALLY_TARGETS]->(t)
           SET r.maxClinicalStage=row.maxClinicalStage""",
        "REL Drug-CLINICALLY_TARGETS->Target", ["drugId", "targetId"])

    add("clinical_report",  # Drug -> Trial
        f"""SELECT id AS trialId, drug.drugId AS drugId
            FROM {pq('clinical_report')} r, UNNEST(r.drugs) AS u(drug)""",
        """UNWIND $rows AS row
           MERGE (t:ClinicalTrial {id: row.trialId})
           MERGE (d:Drug {id: row.drugId})
           MERGE (d)-[:TESTED_IN]->(t)""",
        "REL Drug-TESTED_IN->ClinicalTrial", ["trialId", "drugId"])

    add("clinical_report",  # Trial -> Disease
        f"""SELECT id AS trialId, dis.diseaseId AS diseaseId
            FROM {pq('clinical_report')} r, UNNEST(r.diseases) AS u(dis)""",
        """UNWIND $rows AS row
           MERGE (t:ClinicalTrial {id: row.trialId})
           MERGE (s:Disease {id: row.diseaseId})
           MERGE (t)-[:INVESTIGATES]->(s)""",
        "REL ClinicalTrial-INVESTIGATES->Disease", ["trialId", "diseaseId"])

    add("openfda_significant_adverse_drug_reactions",
        f"""SELECT chembl_id AS drugId, meddraCode, event, count, llr, critval
            FROM {pq('openfda_significant_adverse_drug_reactions')}""",
        """UNWIND $rows AS row
           MERGE (d:Drug {id: row.drugId})
           MERGE (ae:AdverseEvent {id: row.meddraCode})
           SET ae.term=row.event
           MERGE (d)-[r:HAS_ADVERSE_EVENT]->(ae)
           SET r.count=row.count, r.llr=row.llr, r.critval=row.critval""",
        "REL Drug-HAS_ADVERSE_EVENT->AdverseEvent", ["drugId", "meddraCode"])

    add("drug_warning",
        f"""SELECT w.cid AS drugId, efoId, warningType, toxicityClass, country, year
            FROM {pq('drug_warning')} ww, UNNEST(ww.chemblIds) AS w(cid)
            WHERE efoId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (d:Drug {id: row.drugId})
           MERGE (s:Disease {id: row.efoId})
           MERGE (d)-[r:HAS_WARNING]->(s)
           SET r.warningType=row.warningType, r.toxicityClass=row.toxicityClass,
               r.country=row.country, r.year=row.year""",
        "REL Drug-HAS_WARNING->Disease", ["drugId", "efoId"])

    # Protein-protein interactions ----------------------------------------
    add("interaction",
        f"""SELECT targetA, targetB, scoring, sourceDatabase
            FROM {pq('interaction')}
            WHERE targetA IS NOT NULL AND targetB IS NOT NULL
              AND speciesA.taxonId = 9606 AND speciesB.taxonId = 9606""",
        """UNWIND $rows AS row
           MERGE (a:Target {id: row.targetA})
           MERGE (b:Target {id: row.targetB})
           MERGE (a)-[r:INTERACTS_WITH]->(b)
           SET r.score=row.scoring, r.source=row.sourceDatabase""",
        "REL Target-INTERACTS_WITH->Target", ["targetA", "targetB"])

    # Variant annotations & genetics --------------------------------------
    add("variant",  # HAS_CONSEQUENCE_ON target
        f"""SELECT variantId, tc.targetId AS targetId, tc.impact AS impact,
                   tc.consequenceScore AS consequenceScore, tc.aminoAcidChange AS aminoAcidChange
            FROM (SELECT variantId, unnest(transcriptConsequences) AS tc FROM {pq('variant')})
            WHERE tc.targetId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (v:Variant {id: row.variantId})
           MERGE (t:Target {id: row.targetId})
           MERGE (v)-[r:HAS_CONSEQUENCE_ON]->(t)
           SET r.impact=row.impact, r.consequenceScore=row.consequenceScore,
               r.aminoAcidChange=row.aminoAcidChange""",
        "REL Variant-HAS_CONSEQUENCE_ON->Target", ["variantId", "targetId"])

    add("variant",  # MOST_SEVERE_CONSEQUENCE -> SO
        f"""SELECT variantId, mostSevereConsequenceId AS soId
            FROM {pq('variant')} WHERE mostSevereConsequenceId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (v:Variant {id: row.variantId})
           MERGE (s:SOTerm {id: row.soId})
           MERGE (v)-[:MOST_SEVERE_CONSEQUENCE]->(s)""",
        "REL Variant-MOST_SEVERE_CONSEQUENCE->SOTerm", ["variantId", "soId"])

    add("study",  # STUDIES disease
        f"""SELECT studyId, d AS diseaseId
            FROM {pq('study')}, UNNEST(diseaseIds) AS dd(d)""",
        """UNWIND $rows AS row
           MERGE (st:Study {id: row.studyId})
           MERGE (s:Disease {id: row.diseaseId})
           MERGE (st)-[:STUDIES]->(s)""",
        "REL Study-STUDIES->Disease", ["studyId", "diseaseId"])

    add("study",  # MEASURES target (QTL)
        f"""SELECT studyId, geneId FROM {pq('study')} WHERE geneId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (st:Study {id: row.studyId})
           MERGE (t:Target {id: row.geneId})
           MERGE (st)-[:MEASURES]->(t)""",
        "REL Study-MEASURES->Target (QTL)", ["studyId", "geneId"])

    add("credible_set",  # FROM_STUDY + HAS_LEAD_VARIANT
        f"""SELECT studyLocusId, studyId, variantId FROM {pq('credible_set')}""",
        """UNWIND $rows AS row
           MERGE (cs:CredibleSet {id: row.studyLocusId})
           MERGE (st:Study {id: row.studyId})
           MERGE (cs)-[:FROM_STUDY]->(st)
           FOREACH (_ IN CASE WHEN row.variantId IS NULL THEN [] ELSE [1] END |
             MERGE (v:Variant {id: row.variantId})
             MERGE (cs)-[:HAS_LEAD_VARIANT]->(v))""",
        "REL CredibleSet-FROM_STUDY->Study", ["studyLocusId", "studyId"])

    add("credible_set",  # locus members IN_CREDIBLE_SET
        f"""SELECT studyLocusId, lo.variantId AS variantId,
                   lo.posteriorProbability AS pp, lo.is95CredibleSet AS is95
            FROM (SELECT studyLocusId, unnest(locus) AS lo FROM {pq('credible_set')})
            WHERE lo.variantId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (v:Variant {id: row.variantId})
           MERGE (cs:CredibleSet {id: row.studyLocusId})
           MERGE (v)-[r:IN_CREDIBLE_SET]->(cs)
           SET r.posteriorProbability=row.pp, r.is95=row.is95""",
        "REL Variant-IN_CREDIBLE_SET->CredibleSet", ["variantId", "studyLocusId"])

    add("l2g_prediction",
        f"""SELECT studyLocusId, geneId, score FROM {pq('l2g_prediction')}""",
        """UNWIND $rows AS row
           MERGE (cs:CredibleSet {id: row.studyLocusId})
           MERGE (t:Target {id: row.geneId})
           MERGE (cs)-[r:L2G]->(t)
           SET r.score=row.score""",
        "REL CredibleSet-L2G->Target", ["studyLocusId", "geneId"])

    # Mouse models --------------------------------------------------------
    add("mouse_phenotype",
        f"""SELECT targetFromSourceId AS targetId, modelPhenotypeId AS mpId
            FROM {pq('mouse_phenotype')} WHERE targetFromSourceId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (t:Target {id: row.targetId})
           MERGE (m:MousePhenotype {id: row.mpId})
           MERGE (t)-[:HAS_MODEL_PHENOTYPE]->(m)""",
        "REL Target-HAS_MODEL_PHENOTYPE->MousePhenotype", ["targetId", "mpId"])

    # Baseline expression -------------------------------------------------
    add("expression",
        f"""SELECT id AS targetId, ti.efo_code AS biosampleId, ti.label AS biosampleName,
                   ti.rna.value AS rnaValue, ti.rna.level AS rnaLevel
            FROM (SELECT id, unnest(tissues) AS ti FROM {pq('expression')})
            WHERE ti.efo_code IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (t:Target {id: row.targetId})
           MERGE (b:Biosample {id: row.biosampleId})
           SET b.name=coalesce(b.name, row.biosampleName)
           MERGE (t)-[r:EXPRESSED_IN]->(b)
           SET r.rnaValue=row.rnaValue, r.rnaLevel=row.rnaLevel""",
        "REL Target-EXPRESSED_IN->Biosample", ["targetId", "biosampleId"])

    # Pharmacogenomics ----------------------------------------------------
    add("pharmacogenomics",
        f"""SELECT targetFromSourceId AS targetId, variantId, drug.drugId AS drugId,
                   pgxCategory, evidenceLevel, phenotypeText
            FROM {pq('pharmacogenomics')} p, UNNEST(p.drugs) AS u(drug)
            WHERE targetFromSourceId IS NOT NULL AND drug.drugId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (t:Target {id: row.targetId})
           MERGE (d:Drug {id: row.drugId})
           MERGE (t)-[r:HAS_PGX]->(d)
           SET r.category=row.pgxCategory, r.evidenceLevel=row.evidenceLevel,
               r.phenotype=row.phenotypeText""",
        "REL Target-HAS_PGX->Drug", ["targetId", "drugId"])

    # Association backbone (kept: overall_direct; others via ENABLED) ------
    for assoc in ["association_overall_direct", "association_overall_indirect",
                  "association_by_datatype_direct", "association_by_datatype_indirect",
                  "association_by_datasource_direct", "association_by_datasource_indirect"]:
        add(assoc,
            f"""SELECT targetId, diseaseId, associationScore, evidenceCount,
                       aggregationType, aggregationValue
                FROM {pq(assoc)}""",
            """UNWIND $rows AS row
               MERGE (t:Target {id: row.targetId})
               MERGE (s:Disease {id: row.diseaseId})
               MERGE (t)-[r:ASSOCIATED_WITH {aggregation: coalesce(row.aggregationValue,'overall')}]->(s)
               SET r.score=row.associationScore, r.evidenceCount=row.evidenceCount,
                   r.aggregationType=row.aggregationType""",
            f"REL Target-ASSOCIATED_WITH->Disease [{assoc}]", ["targetId", "diseaseId"])

    # ===================== BIG / OPT-IN =====================
    add("colocalisation",
        f"""SELECT leftStudyLocusId, rightStudyLocusId, h4, clpp, colocalisationMethod
            FROM {pq('colocalisation')}""",
        """UNWIND $rows AS row
           MERGE (l:CredibleSet {id: row.leftStudyLocusId})
           MERGE (rt:CredibleSet {id: row.rightStudyLocusId})
           MERGE (l)-[r:COLOCALISES_WITH]->(rt)
           SET r.h4=row.h4, r.clpp=row.clpp, r.method=row.colocalisationMethod""",
        "REL CredibleSet-COLOCALISES_WITH->CredibleSet", ["leftStudyLocusId", "rightStudyLocusId"],
        count=False)

    add("enhancer_to_gene",
        f"""SELECT intervalId AS enhancerId, geneId, chromosome, start, "end" AS endpos,
                   score, distanceToTss, biosampleId, datasourceId
            FROM {pq('enhancer_to_gene')} WHERE geneId IS NOT NULL AND intervalId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (e:Enhancer {id: row.enhancerId})
           SET e.chromosome=row.chromosome, e.start=row.start, e.end=row.endpos,
               e.biosampleId=row.biosampleId, e.datasource=row.datasourceId
           MERGE (t:Target {id: row.geneId})
           MERGE (e)-[r:REGULATES]->(t)
           SET r.score=row.score, r.distanceToTss=row.distanceToTss""",
        "REL Enhancer-REGULATES->Target", ["enhancerId", "geneId"], count=False)

    add("interaction_evidence",
        f"""SELECT targetA, targetB, interactionScore, interactionTypeShortName,
                   interactionDetectionMethodShortName, pubmedId
            FROM {pq('interaction_evidence')}
            WHERE targetA IS NOT NULL AND targetB IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (a:Target {id: row.targetA})
           MERGE (b:Target {id: row.targetB})
           MERGE (a)-[r:PPI_EVIDENCE {pubmedId: coalesce(row.pubmedId,'')}]->(b)
           SET r.score=row.interactionScore, r.type=row.interactionTypeShortName,
               r.method=row.interactionDetectionMethodShortName""",
        "REL Target-PPI_EVIDENCE->Target", ["targetA", "targetB"], count=False)

    add("literature",
        # No DISTINCT: 164M rows -- let it stream and rely on MERGE to de-dup.
        f"""SELECT pmid, keywordId, keywordType, relevance
            FROM {pq('literature')}
            WHERE pmid IS NOT NULL AND keywordId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (p:Publication {id: row.pmid})
           WITH p, row
           CALL (p, row) {
             WITH p, row WHERE row.keywordId STARTS WITH 'ENSG'
             MERGE (t:Target {id: row.keywordId}) MERGE (p)-[m:MENTIONS]->(t) SET m.relevance=row.relevance
           }
           CALL (p, row) {
             WITH p, row WHERE row.keywordId STARTS WITH 'CHEMBL'
             MERGE (d:Drug {id: row.keywordId}) MERGE (p)-[m:MENTIONS]->(d) SET m.relevance=row.relevance
           }
           CALL (p, row) {
             WITH p, row WHERE NOT (row.keywordId STARTS WITH 'ENSG' OR row.keywordId STARTS WITH 'CHEMBL')
             MERGE (s:Disease {id: row.keywordId}) MERGE (p)-[m:MENTIONS]->(s) SET m.relevance=row.relevance
           }""",
        "REL Publication-MENTIONS->{Target|Drug|Disease}", ["pmid", "keywordId"], count=False)

    add("literature_vector",
        f"""SELECT word, category, vector FROM {pq('literature_vector')}""",
        """UNWIND $rows AS row
           MERGE (w:Term {id: row.word})
           SET w.category=row.category, w.vector=row.vector""",
        "NODE Term (literature embedding)", ["word"], count=False)

    add("target_essentiality",
        f"""SELECT id AS targetId, s.depmapId AS cellLineId, s.cellLineName AS cellLineName,
                   s.geneEffect AS geneEffect
            FROM (
              SELECT id, unnest(geneEssentiality) AS ge FROM {pq('target_essentiality')}
            ) a,
            LATERAL (SELECT unnest(a.ge.depMapEssentiality) AS dm) b,
            LATERAL (SELECT unnest(b.dm.screens) AS s) c
            WHERE s.depmapId IS NOT NULL""",
        """UNWIND $rows AS row
           MERGE (t:Target {id: row.targetId})
           MERGE (cl:CellLine {id: row.cellLineId})
           SET cl.name=row.cellLineName
           MERGE (t)-[r:ESSENTIAL_IN]->(cl)
           SET r.geneEffect=row.geneEffect""",
        "REL Target-ESSENTIAL_IN->CellLine", ["targetId", "cellLineId"], count=False)

    return S


# -----------------------------------------------------------------------------
# 6. MAIN
# -----------------------------------------------------------------------------
def main():
    print("=" * 66)
    print("  OPEN TARGETS 26.03  ->  NEO4J   (non-evidence datasets)")
    print("=" * 66)
    t_all = time.time()

    create_constraints()

    specs = build_specs()
    ran, skipped = 0, 0
    for key, sql, cypher, label, required, count in specs:
        if not ENABLED.get(key, True):
            print(f"\n[skip] {label}   (dataset '{key}' disabled in ENABLED)")
            skipped += 1
            continue
        try:
            ingest(key, sql, cypher, label, required=required, count=count)
            ran += 1
        except Exception as e:
            print(f"  !! ERROR on {label}: {e}")

    driver.close()
    duck.close()
    dt = time.time() - t_all
    print("\n" + "=" * 66)
    print(f"  KNOWLEDGE GRAPH BUILD COMPLETE")
    print(f"  loaders run: {ran}   skipped: {skipped}")
    print(f"  total time : {dt/60:,.1f} min")
    print("=" * 66)


if __name__ == "__main__":
    main()