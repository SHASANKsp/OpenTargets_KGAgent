// ============================================================================
//  OPEN TARGETS KG — APPLICATION QUERY PACK
//  Built against the post-QC graph (~85.9M relationships, 25 rel types).
//
//  HOW TO USE
//   - Node labels, relationship directions and property names below are
//     INFERRED from relationship names. Verify against your model first
//     (Section 0) and adjust labels/props to match before running.
//   - Queries are parameterised with $params. In cypher-shell set them with
//     :param efo => 'EFO_0000685';   (etc.)   or pass via driver params.
//   - Confidence thresholds (L2G >= 0.5, PPI score >= 0.5, PIP, p-value) are
//     starting points — tune to your use case.
// ============================================================================


// ----------------------------------------------------------------------------
// 0. SCHEMA VERIFICATION  (run these first, fix labels/props below to match)
// ----------------------------------------------------------------------------
CALL db.schema.visualization();
CALL db.schema.relTypeProperties();
CALL db.schema.nodeTypeProperties();
// Peek at one edge of a type to learn its property names/values:
MATCH ()-[r:L2G]->() RETURN r LIMIT 1;


// ============================================================================
// 01 · GENETIC TARGET DISCOVERY
// ============================================================================

// 1.1  Disease -> causal genes (fine-mapped GWAS + L2G)  [core]
MATCH (d:Disease {id:$efo})<-[:STUDIES]-(s:Study)
      <-[:FROM_STUDY]-(cs:CredibleSet)-[l:L2G]->(t:Target)
WHERE l.score >= 0.5
RETURN t.approvedSymbol AS target, max(l.score) AS l2g,
       count(DISTINCT s) AS studies
ORDER BY l2g DESC;

// 1.2  Variant-to-gene assignment
MATCH (v:Variant {id:$vid})-[:IN_CREDIBLE_SET]->(:CredibleSet)-[l:L2G]->(t:Target)
RETURN t.approvedSymbol AS target, l.score AS l2g
ORDER BY l2g DESC;

// 1.3  Coding-variant target evidence (direct protein impact)
MATCH (v:Variant)-[:ASSOCIATED_WITH]->(d:Disease {id:$efo})
MATCH (v)-[c:MOST_SEVERE_CONSEQUENCE]->(t:Target)
WHERE c.impact IN ['HIGH','MODERATE']
RETURN t.approvedSymbol AS target,
       collect(DISTINCT c.label) AS consequences,
       count(DISTINCT v) AS variants
ORDER BY variants DESC;

// 1.4  Pleiotropy scan (targets linked to many diseases)
MATCH (t:Target)<-[l:L2G]-(:CredibleSet)-[:FROM_STUDY]->(:Study)-[:STUDIES]->(d:Disease)
WHERE l.score >= 0.5
WITH t, count(DISTINCT d) AS diseases
WHERE diseases >= 5
RETURN t.approvedSymbol AS target, diseases
ORDER BY diseases DESC;


// ============================================================================
// 02 · TARGET VALIDATION & PRIORITISATION
// ============================================================================

// 2.1  Composite multi-evidence score (sketch — set your own weights)
MATCH (t:Target {id:$ens})
OPTIONAL MATCH (t)<-[l:L2G]-(:CredibleSet)-[:FROM_STUDY]->(:Study)-[:STUDIES]->(:Disease {id:$efo})
OPTIONAL MATCH (t)-[e:EXPRESSED_IN]->(:Tissue {id:$tissue})
OPTIONAL MATCH (t)-[:HAS_MODEL_PHENOTYPE]->(mp)
OPTIONAL MATCH (t)-[:PARTICIPATES_IN]->(pw)
RETURN t.approvedSymbol AS target,
       max(l.score)        AS genetic,
       max(e.rna)          AS expression,
       count(DISTINCT mp)  AS model_phenos,
       count(DISTINCT pw)  AS pathways;

// 2.2  Tissue-selective target filter
MATCH (t:Target {id:$ens})-[e:EXPRESSED_IN]->(tis:Tissue)
RETURN tis.name AS tissue, e.rna AS rna, e.protein AS protein
ORDER BY rna DESC;

// 2.3  Mouse–human phenotype concordance (needs MP<->HPO mapping)
MATCH (t:Target {id:$ens})-[:HAS_MODEL_PHENOTYPE]->(mp)
MATCH (d:Disease {id:$efo})-[:HAS_PHENOTYPE]->(hp)
// join mp<->hp via your ontology cross-reference, then:
RETURN t.approvedSymbol, collect(DISTINCT mp.label) AS mouse,
       collect(DISTINCT hp.label) AS human;

// 2.4  PPI neighbourhood & backup targets
MATCH (t:Target {id:$ens})-[r:INTERACTS_WITH]-(n:Target)
WHERE r.score >= 0.5
RETURN n.approvedSymbol AS partner, r.score AS score, r.source AS source
ORDER BY score DESC;


// ============================================================================
// 03 · SAFETY & DE-RISKING
// ============================================================================

// 3.1  Target adverse-event profile
MATCH (dr:Drug)-[:TARGETS|CLINICALLY_TARGETS]->(t:Target {id:$ens})
MATCH (dr)-[:HAS_ADVERSE_EVENT]->(ae)
RETURN ae.name AS adverse_event, count(DISTINCT dr) AS drugs
ORDER BY drugs DESC;

// 3.2  Broad-expression toxicity risk
MATCH (t:Target {id:$ens})-[e:EXPRESSED_IN]->(tis:Tissue)
WHERE e.rna >= $cutoff
RETURN count(DISTINCT tis) AS tissues_expressed;

// 3.3  Pharmacogenomics & black-box warnings
MATCH (dr:Drug)-[:TARGETS]->(t:Target {id:$ens})
OPTIONAL MATCH (dr)-[:HAS_PGX]->(p)
OPTIONAL MATCH (dr)-[:HAS_WARNING]->(w)
RETURN dr.name AS drug,
       collect(DISTINCT p.label)       AS pgx,
       collect(DISTINCT w.description)  AS warnings;


// ============================================================================
// 04 · REPURPOSING & INDICATION EXPANSION
// ============================================================================

// 4.1  Genetically-supported repurposing (with whitespace filter)  [core]
MATCH (dr:Drug)-[:TARGETS]->(t:Target)<-[l:L2G]-(:CredibleSet)
      -[:FROM_STUDY]->(:Study)-[:STUDIES]->(d2:Disease)
WHERE l.score >= 0.5 AND NOT (dr)-[:INDICATED_FOR]->(d2)
RETURN dr.name AS drug, t.approvedSymbol AS target, d2.name AS new_indication, l.score AS l2g
ORDER BY l2g DESC;

// 4.2  Indication landscape & whitespace
MATCH (dr:Drug)-[:TARGETS]->(:Target {id:$ens})
OPTIONAL MATCH (dr)-[:INDICATED_FOR]->(app:Disease)
OPTIONAL MATCH (dr)-[:INVESTIGATES|TESTED_IN]->(trial)
RETURN dr.name AS drug, collect(DISTINCT app.name) AS approved,
       count(DISTINCT trial) AS trials;

// 4.3  Clinical-precedence scoring
MATCH (t:Target {id:$ens})<-[:CLINICALLY_TARGETS]-(dr:Drug)
OPTIONAL MATCH (dr)-[:TESTED_IN]->(ct)
RETURN t.approvedSymbol, count(DISTINCT dr) AS clinical_drugs,
       count(DISTINCT ct) AS trials;

// 4.4  Mechanism-shared repurposing (pathway bridge)
MATCH (dr:Drug)-[:TARGETS]->(dt:Target)-[:PARTICIPATES_IN]->(pw:Pathway)
MATCH (dgt:Target)-[:PARTICIPATES_IN]->(pw)
WHERE dgt.id IN $diseaseTargets AND NOT (dr)-[:INDICATED_FOR]->(:Disease {id:$efo})
RETURN dr.name AS drug, pw.name AS shared_pathway, count(DISTINCT dgt) AS disease_targets
ORDER BY disease_targets DESC;


// ============================================================================
// 05 · VARIANT INTERPRETATION & FUNCTIONAL GENOMICS
// ============================================================================

// 5.1  Fine-mapped locus browser
MATCH (s:Study {id:$study})-[:HAS_LEAD_VARIANT]->(lead:Variant)
MATCH (cs:CredibleSet)-[:FROM_STUDY]->(s)
MATCH (v:Variant)-[m:IN_CREDIBLE_SET]->(cs)
RETURN lead.id AS lead, v.id AS member, m.posteriorProbability AS pip
ORDER BY pip DESC;

// 5.2  Molecular QTL linkage (confirm MEASURES semantics)
MATCH (s:Study)-[:MEASURES]->(mt)
WHERE s.type = 'QTL'
RETURN s.id AS study, mt.id AS molecular_trait LIMIT 100;

// 5.3  Variant functional dossier
MATCH (v:Variant {id:$vid})
OPTIONAL MATCH (v)-[:ASSOCIATED_WITH]->(d:Disease)
OPTIONAL MATCH (v)-[c:MOST_SEVERE_CONSEQUENCE]->(t:Target)
OPTIONAL MATCH (v)-[:IN_CREDIBLE_SET]->(cs:CredibleSet)
RETURN collect(DISTINCT d.name) AS traits,
       collect(DISTINCT [t.approvedSymbol, c.impact]) AS consequence,
       count(DISTINCT cs) AS credible_sets;


// ============================================================================
// 06 · DISEASE & PHENOTYPE ANALYTICS
// ============================================================================

// 6.1  Disease–disease similarity (shared phenotypes)
MATCH (d1:Disease)-[:HAS_PHENOTYPE]->(p)<-[:HAS_PHENOTYPE]-(d2:Disease)
WHERE id(d1) < id(d2)
RETURN d1.name AS disease_a, d2.name AS disease_b, count(p) AS shared
ORDER BY shared DESC LIMIT 100;

// 6.2  Ontology roll-up / evidence propagation  [core to your semantic layer]
MATCH (child:Disease)-[:SUBCLASS_OF*1..4]->(d:Disease {id:$efo})
MATCH (v:Variant)-[:ASSOCIATED_WITH]->(child)
RETURN d.name AS parent, count(DISTINCT v) AS rolled_up_variants;

// 6.3  Phenotype-driven target search
MATCH (t:Target)-[:HAS_MODEL_PHENOTYPE]->(mp {id:$mp})
RETURN t.approvedSymbol AS target, count(*) AS evidence
ORDER BY evidence DESC;


// ============================================================================
// 07 · PATHWAY & NETWORK BIOLOGY   (GDS — check licence)
// ============================================================================

// 7.1  Pathway enrichment of disease targets (observed counts)
MATCH (t:Target)-[:PARTICIPATES_IN]->(pw:Pathway)
WHERE t.id IN $diseaseTargets
RETURN pw.name AS pathway, count(*) AS hits
ORDER BY hits DESC;
// -> feed hits + pathway size + background into a hypergeometric / Fisher test

// 7.2  PPI module detection (Louvain)
CALL gds.graph.project('ppi', 'Target',
  {INTERACTS_WITH: {orientation:'UNDIRECTED', properties:'score'}});
CALL gds.louvain.stream('ppi', {relationshipWeightProperty:'score'})
YIELD nodeId, communityId
RETURN communityId, count(*) AS members
ORDER BY members DESC;

// 7.3  Network propagation (personalised PageRank from genetic seeds)
CALL gds.pageRank.stream('ppi', {
  sourceNodes: $seedTargetIds,
  relationshipWeightProperty: 'score'
})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).approvedSymbol AS target, score
ORDER BY score DESC LIMIT 100;


// ============================================================================
// 08 · BIOLOGICS & ANTIBODY TARGET SELECTION
// ============================================================================

// 8.1  Surface / secreted target selection (needs localisation property/join)
MATCH (t:Target {id:$ens})-[e:EXPRESSED_IN]->(:Tissue)
WHERE t.subcellularLocation IN ['Cell membrane','Secreted']   // adjust to your schema
RETURN DISTINCT t.approvedSymbol;

// 8.2  Tumour-vs-normal expression window
MATCH (t:Target {id:$ens})-[e:EXPRESSED_IN]->(tis:Tissue)
RETURN tis.name AS tissue, e.rna AS level
ORDER BY level DESC;   // compare disease-tissue vs normal-tissue rows

// 8.3  Bispecific / BiTE pair nomination (interacting surface pair)
MATCH (a:Target {id:$ens})-[r:INTERACTS_WITH]-(b:Target)
WHERE r.score >= 0.5
  AND a.subcellularLocation = 'Cell membrane'
  AND b.subcellularLocation = 'Cell membrane'
RETURN a.approvedSymbol AS armA, b.approvedSymbol AS armB, r.score
ORDER BY r.score DESC;


// ============================================================================
// 09 · DATA PRODUCTS  (traversal layer under a dossier / portal)
// ============================================================================

// 9.1  Auto target dossier (single parameterised assembly)
MATCH (t:Target {id:$ens})
OPTIONAL MATCH (t)<-[l:L2G]-(:CredibleSet)-[:FROM_STUDY]->(:Study)-[:STUDIES]->(gd:Disease)
OPTIONAL MATCH (t)-[e:EXPRESSED_IN]->(tis:Tissue)
OPTIONAL MATCH (dr:Drug)-[:TARGETS]->(t)
OPTIONAL MATCH (dr)-[:INDICATED_FOR]->(ind:Disease)
OPTIONAL MATCH (dr)-[:HAS_ADVERSE_EVENT]->(ae)
OPTIONAL MATCH (t)-[pp:INTERACTS_WITH]-(n:Target) WHERE pp.score >= 0.5
RETURN t.approvedSymbol AS target,
       collect(DISTINCT gd.name)[0..10]  AS genetic_diseases,
       collect(DISTINCT tis.name)[0..10] AS tissues,
       collect(DISTINCT dr.name)[0..10]  AS drugs,
       collect(DISTINCT ind.name)[0..10] AS indications,
       collect(DISTINCT ae.name)[0..10]  AS adverse_events,
       count(DISTINCT n)                 AS ppi_partners;

// 9.2  Embeddings / link prediction and GNNs run OUTSIDE Cypher — export the
//      graph (gds.graph.export / APOC export) and train with PyKEEN, GDS
//      FastRP + link prediction pipeline, or PyG. Check component licences.
