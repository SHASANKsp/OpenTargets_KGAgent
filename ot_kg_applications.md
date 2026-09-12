# Open Targets KG — Application Atlas

What the filtered graph can power, grounded in the current edge inventory (~85.9M relationships across 25 types, down ~82% from the 483M raw load). Node labels, directions and property names are **inferred from relationship names** — verify with `CALL db.schema.visualization()` and `CALL db.schema.relTypeProperties()` before building.

## The graph at a glance

| Relationship | Count | Layer |
|---|---:|---|
| IN_CREDIBLE_SET | 40,771,978 | genetics |
| ASSOCIATED_WITH | 17,246,560 | genetics |
| MOST_SEVERE_CONSEQUENCE | 7,432,548 | genetics |
| EXPRESSED_IN | 5,614,446 | expression |
| FROM_STUDY | 3,491,182 | genetics |
| HAS_LEAD_VARIANT | 3,491,182 | genetics |
| L2G | 2,794,835 | genetics |
| MEASURES | 1,865,582 | genetics |
| INTERACTS_WITH | 1,222,953 | network |
| TESTED_IN | 347,197 | clinical |
| INVESTIGATES | 325,609 | clinical |
| ANNOTATED_WITH | 290,549 | functional |
| HAS_MODEL_PHENOTYPE | 210,091 | phenotype |
| HAS_PHENOTYPE | 180,981 | phenotype |
| SUBCLASS_OF | 171,460 | ontology |
| STUDIES | 145,883 | genetics |
| HAS_ADVERSE_EVENT | 115,215 | safety |
| IS_A | 58,799 | ontology |
| INDICATED_FOR | 53,950 | clinical |
| PARTICIPATES_IN | 48,613 | pathway |
| TARGETS | 14,559 | drug |
| CLINICALLY_TARGETS | 13,407 | drug |
| HAS_PGX | 3,957 | safety |
| HAS_PARENT | 1,965 | ontology |
| HAS_WARNING | 849 | safety |

Two genetics edge types hold ~two-thirds of the graph; the drug/phenotype/ontology layers are small but carry most interpretive value per edge.

## Inferred schema spine

```
Variant ──IN_CREDIBLE_SET──▶ CredibleSet ──FROM_STUDY──▶ Study ──STUDIES──▶ Disease
   │                              │
   │ MOST_SEVERE_CONSEQUENCE      │ L2G
   └──────────────┬───────────────┘
                  ▼
               Target ──EXPRESSED_IN──▶ Tissue
                  ▲  ▲
   TARGETS /      │  │  INTERACTS_WITH (Target↔Target)
   CLINICALLY_TARGETS│
                  Drug ──INDICATED_FOR──▶ Disease
                       ──HAS_ADVERSE_EVENT / HAS_PGX / HAS_WARNING
```
Node classes (inferred): Variant, CredibleSet, Study, Disease, Target, Tissue, Drug, Pathway, Phenotype, MousePhenotype, AdverseEvent, ontology terms.

---

## 01 · Genetic target discovery

- **Disease → causal genes** — `Disease ←STUDIES Study ←FROM_STUDY CredibleSet →L2G Target`. Fine-mapped GWAS signals resolved to their likely gene; your primary genetically-supported target list per indication. *(core)*
- **Variant-to-gene assignment** — `Variant →IN_CREDIBLE_SET CredibleSet →L2G Target`. Rank the genes a variant acts through.
- **Coding-variant target evidence** — `Variant →MOST_SEVERE_CONSEQUENCE Target` intersected with disease association; HIGH/MODERATE impact is the strongest interpretable class. *(needs consequence impact field)*
- **Pleiotropy scan** — targets linked to many diseases via L2G; reads as repurposing hub or on-target safety flag.

## 02 · Target validation & prioritisation

- **Composite multi-evidence score** — aggregate L2G + EXPRESSED_IN + PARTICIPATES_IN + HAS_MODEL_PHENOTYPE + INTERACTS_WITH per target–disease pair into one tunable score.
- **Tissue-selective target filter** — `Target →EXPRESSED_IN Tissue`; expressed in disease tissue, low elsewhere → therapeutic window. *(needs expression level field)*
- **Mouse–human phenotype concordance** — `Target →HAS_MODEL_PHENOTYPE` vs `Disease →HAS_PHENOTYPE`; orthogonal validation. *(needs MP↔HPO mapping)*
- **PPI neighbourhood & backups** — `Target ↔INTERACTS_WITH Target`; backup targets in the same complex/pathway.

## 03 · Safety & de-risking

- **Target adverse-event profile** — aggregate AEs across every drug that hits a target → empirical on-target safety signature.
- **Broad-expression toxicity risk** — count tissues where a target is meaningfully expressed; ubiquity flags off-tissue tox. *(needs expression level field)*
- **Pharmacogenomics & warnings** — surface HAS_PGX liabilities and HAS_WARNING black-box flags on drugs against a target.
- **Genetic-constraint context** — pleiotropy + phenotype breadth as a proxy for tolerability.

## 04 · Repurposing & indication expansion

- **Genetically-supported repurposing** — `Drug →TARGETS Target ←L2G … → Disease′` with a `NOT INDICATED_FOR` whitespace filter; the highest-value repurposing signal in the graph. *(core)*
- **Indication landscape & whitespace** — approved (INDICATED_FOR) vs in-trial (INVESTIGATES/TESTED_IN) vs untouched, per mechanism.
- **Clinical-precedence scoring** — rank targets by depth of existing clinical validation.
- **Mechanism-shared repurposing** — pathway bridge (PARTICIPATES_IN) between a drug's target and a disease's genetic targets. *(needs pathway coverage)*

## 05 · Variant interpretation & functional genomics

- **Fine-mapped locus browser** — `Study →HAS_LEAD_VARIANT Variant →IN_CREDIBLE_SET CredibleSet`; data behind a locus-zoom view.
- **Molecular QTL linkage** — `Study →MEASURES MolecularTrait`; links regulatory variants to targets. *(confirm MEASURES semantics)*
- **Variant functional dossier** — one call assembling a variant's traits, coding consequence, gene assignment and fine-mapping context.

## 06 · Disease & phenotype analytics

- **Disease–disease similarity** — shared phenotypes or shared genetic targets → mechanistic overlap, comorbidity, repurposing adjacency.
- **Ontology roll-up / expansion** — `SUBCLASS_OF / IS_A / HAS_PARENT`; propagate child-disease evidence to parents. Backbone of your semantic-search layer. *(core)*
- **Phenotype-driven target search** — start from a phenotype, find targets whose perturbation produces it.

## 07 · Pathway & network biology  *(GDS — check licence)*

- **Pathway enrichment of targets** — do disease targets concentrate in a pathway (PARTICIPATES_IN)?
- **PPI module detection** — Louvain/Leiden over INTERACTS_WITH → functional modules / complexes.
- **Network propagation** — personalised PageRank from genetic seeds to recover network-implicated targets missed by GWAS.

## 08 · Biologics & antibody target selection

- **Surface / secreted target selection** — restrict validated targets to antibody-amenable proteins. *(needs subcellular localisation)*
- **Tumour-vs-normal expression window** — expression differential gating an ADC/bispecific. *(needs expression level field)*
- **Bispecific / BiTE pair nomination** — interacting/co-expressed surface pairs as candidate arms; ties into your docking pipeline. *(needs surface + co-expression)*

## 09 · Graph ML & data products

- **KG embeddings & link prediction** — TransE/ComplEx/RGCN to predict novel target–disease or drug–target links. *(PyKEEN/GDS — check licence)*
- **GNN association prediction** — supervised GraphSAGE/R-GCN over target–disease neighbourhoods.
- **Auto target/disease dossier** — one parameterised traversal → formatted one-pager, ready for a Streamlit/REST front end.
- **Semantic search portal** — your ontology-layer resolve→expand→bridge→retrieve algorithm as the natural-language front door to every traversal here.

---

## Caveats

- **Schema is inferred** — verify labels, directions and property names before building.
- **Licensing** — most core Neo4j GDS algorithms (Louvain, PageRank, FastRP) are free, but some algorithms, Bloom and Neo4j Enterprise are commercial; PyKEEN is MIT but individual pretrained models / GNN stacks may not be. Confirm any network-analytics or embedding component before it ships in a product.
- **Filtering trade-off** — this runs on the confidence-thresholded graph. Applications needing recall at the margins (fine-mapping tails, allelic series, novel-signal discovery) should run against a fuller build.

*See `ot_kg_applications.cypher` for the runnable, parameterised version of every query above.*
