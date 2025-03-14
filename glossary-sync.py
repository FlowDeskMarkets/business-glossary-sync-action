import logging
import os
import pathlib
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple, TypeVar, Union, Any

import click
import progressbar
import requests
import ruamel.yaml.util
from datahub.ingestion.graph.client import (DatahubClientConfig, DataHubGraph,
                                            get_default_graph)
from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossaryConfig, BusinessGlossaryFileSource, DefaultConfig,
    GlossaryNodeConfig, GlossaryNodeInterface, GlossaryTermConfig,
    KnowledgeCard, Owners, materialize_all_node_urns, populate_path_vs_id)
from datahub.metadata.schema_classes import (AspectBag, DomainsClass,
                                             GlossaryNodeInfoClass,
                                             GlossaryRelatedTermsClass,
                                             GlossaryTermInfoClass,
                                             InstitutionalMemoryClass,
                                             OwnershipClass)
from datahub.utilities.urns.urn import guess_entity_type
from ruamel.yaml import YAML

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

ParentUrn = str
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

_DELETED_NODE_TOMBSTONE = dict()

_GlossaryElement = TypeVar("_GlossaryElement", GlossaryNodeConfig, GlossaryTermConfig)


def get_graph() -> DataHubGraph:
    if TEST_MODE:
        return get_default_graph()
    else:
        DATAHUB_SERVER = os.environ["DATAHUB_GMS_HOST"]
        DATAHUB_TOKEN: Optional[str] = os.getenv("DATAHUB_GMS_TOKEN")

        return DataHubGraph(
            DatahubClientConfig(server=DATAHUB_SERVER, token=DATAHUB_TOKEN)
        )


def _get_id_from_urn(urn: str) -> str:
    return urn.split(":")[-1]


def _datahub_ownership_to_owners(
    ownership: Optional[OwnershipClass],
) -> Optional[Union[Owners, List[Owners]]]:
    if ownership is None:
        return None

    # Group owners by type/typeUrn
    owners_by_type = defaultdict(list)
    for owner in ownership.owners:
        owners_by_type[(owner.type, owner.typeUrn)].append(owner.owner)

    # Filter URNs by type
    def filter_urns(urns: List[str], entity_type: str) -> Optional[List[str]]:
        filtered = [
            _get_id_from_urn(urn) 
            for urn in urns 
            if guess_entity_type(urn) == entity_type
        ]
        return filtered or None

    # Create owners object(s)
    owners_list = [
        Owners(
            users=filter_urns(urns, "corpuser"),
            groups=filter_urns(urns, "corpGroup"),
            type=owner_type,
            typeUrn=owner_type_urn,
        )
        for (owner_type, owner_type_urn), urns in owners_by_type.items()
    ]

    # Return single object if only one type, otherwise return list
    return owners_list[0] if len(owners_list) == 1 else owners_list


def _datahub_domain_to_str(domain: Optional[DomainsClass]) -> Optional[str]:
    if domain is None or not domain.domains:
        return None

    if len(domain.domains) > 1:
        logger.warning(f"Found multiple domains for {domain} - using first one")
        return None

    # TODO: Need to look up the domain name from the urn.
    return domain.domains[0]


def _datahub_institutional_memory_to_knowledge_links(
    institutionalMemory: Optional[InstitutionalMemoryClass],
) -> Optional[List[KnowledgeCard]]:
    if institutionalMemory is None:
        return None

    return [
        KnowledgeCard(
            url=element.url,
            label=element.description,
        )
        for element in institutionalMemory.elements
    ]


def _extract_audit_info(aspects: AspectBag) -> Dict[str, Any]:
    """Extract audit information from aspects for changelog tracking."""
    audit_info = {}
    
    # Extract ownership last modified info if available
    if "ownership" in aspects:
        ownership = aspects["ownership"]
        if hasattr(ownership, "lastModified"):
            audit_info["last_modified_time"] = ownership.lastModified.time
            audit_info["last_modified_actor"] = ownership.lastModified.actor
            if ownership.lastModified.impersonator:
                audit_info["last_modified_impersonator"] = ownership.lastModified.impersonator
    
    # Extract status info if available
    if "status" in aspects:
        status = aspects["status"]
        if hasattr(status, "removed"):
            audit_info["removed"] = status.removed
    
    return audit_info


def _glossary_node_from_datahub(
    urn: str, aspects: AspectBag
) -> Tuple[Optional[GlossaryNodeConfig], Optional[ParentUrn]]:
    try:
        info_aspect: GlossaryNodeInfoClass = aspects["glossaryNodeInfo"]
    except KeyError:
        logger.error(f"Skipping URN {urn} due to missing 'glossaryNodeInfo' aspect")
        return None, None

    owners = aspects.get("ownership")
    institutionalMemory = aspects.get("institutionalMemory")
    
    # Extract audit information
    audit_info = _extract_audit_info(aspects)

    node = GlossaryNodeConfig(
        id=urn,
        name=info_aspect.name or _get_id_from_urn(urn),
        description=info_aspect.definition,
        owners=_datahub_ownership_to_owners(owners),
        knowledge_links=_datahub_institutional_memory_to_knowledge_links(
            institutionalMemory
        ),
        # These are populated later.
        terms=[],
        nodes=[],
        # Add audit information as custom properties
        custom_properties=audit_info,
    )
    node._urn = urn
    parent_urn = info_aspect.parentNode

    return node, parent_urn


def _glossary_term_from_datahub(
    urn: str, aspects: AspectBag
) -> Tuple[Optional[GlossaryTermConfig], Optional[ParentUrn]]:
    try:
        info_aspect: GlossaryTermInfoClass = aspects["glossaryTermInfo"]
    except KeyError:
        logger.error(f"Skipping URN {urn} due to missing 'glossaryTermInfo' aspect")
        return None, None

    related_terms: GlossaryRelatedTermsClass = aspects.get(
        "glossaryRelatedTerms", GlossaryRelatedTermsClass()
    )
    owners = aspects.get("ownership")
    domain = aspects.get("domains")
    institutionalMemory = aspects.get("institutionalMemory")
    
    # Extract audit information
    audit_info = _extract_audit_info(aspects)
    
    # Merge custom properties from the term with our audit info
    custom_properties = info_aspect.customProperties or {}
    custom_properties.update(audit_info)

    term = GlossaryTermConfig(
        id=urn,
        name=info_aspect.name or _get_id_from_urn(urn),
        description=info_aspect.definition,
        term_source=info_aspect.termSource,
        source_ref=info_aspect.sourceRef,
        source_url=info_aspect.sourceUrl,
        owners=_datahub_ownership_to_owners(owners),
        custom_properties=custom_properties,
        knowledge_links=_datahub_institutional_memory_to_knowledge_links(
            institutionalMemory
        ),
        domain=_datahub_domain_to_str(domain),
        # Where possible, these are converted into biz glossary paths later.
        inherits=related_terms.isRelatedTerms,
        contains=related_terms.hasRelatedTerms,
        values=related_terms.values,
        related_terms=related_terms.relatedTerms,
    )

    term._urn = urn
    parent_urn = info_aspect.parentNode

    return term, parent_urn


def fetch_datahub_glossary():
    graph = get_graph()

    logger.info("Get all the urns in the glossary.")
    urns = list(graph.get_urns_by_filter(entity_types=["glossaryTerm", "glossaryNode"]))
    logger.info(f"Got {len(urns)} urns")

    logger.info("Hydrate them into entities.")
    entities = {}
    for urn in progressbar.progressbar(urns):
        try:
            entities[urn] = graph.get_entity_semityped(urn)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error for urn {urn}: {e}")

    # Map these into pydantic models defined in the biz glossary source.
    # 1. Map each AspectBag -> pydantic model.
    # 2. Construct the hierarchy of pydantic models using the lookup table.

    logger.info("Parse glossary nodes.")
    raw_nodes = {}
    for urn in progressbar.progressbar(urns):
        if guess_entity_type(urn) != "glossaryNode" or urn not in entities:
            continue
        raw_nodes[urn] = _glossary_node_from_datahub(urn, entities[urn])

    logger.info("Construct the hierarchy of nodes.")
    top_level_nodes: List[GlossaryNodeConfig] = []
    for node, parent_urn in raw_nodes.values():
        if node is None:
            continue
        if parent_urn is None:
            top_level_nodes.append(node)
        else:
            parent_node, _ = raw_nodes[parent_urn]
            parent_node.nodes = parent_node.nodes or []
            parent_node.nodes.append(node)

    logger.info("Parse glossary terms.")
    raw_terms = {}
    for urn in progressbar.progressbar(urns):
        if guess_entity_type(urn) != "glossaryTerm" or urn not in entities:
            continue
        raw_terms[urn] = _glossary_term_from_datahub(urn, entities[urn])

    logger.info("Construct the hierarchy of terms.")
    top_level_terms: List[GlossaryTermConfig] = []
    for term, parent_urn in raw_terms.values():
        if term is None:
            continue
        if parent_urn is None:
            top_level_terms.append(term)
        elif parent_urn not in raw_nodes:
            logger.error(f"Unable to find parent urn: {parent_urn} for term {term.id}")
            top_level_terms.append(term)
        else:
            parent_node, _ = raw_nodes[parent_urn]
            parent_node.terms = parent_node.terms or []
            parent_node.terms.append(term)

    return top_level_nodes, top_level_terms


def prune_latest_glossary(
    latest_glossary: BusinessGlossaryConfig, existing_glossary: BusinessGlossaryConfig
) -> None:
    # TODO: Update this logic to allow for pruning of nodes within the hierarchy as well.

    allowed_node_urns = set(node._urn for node in (existing_glossary.nodes or []))
    allowed_term_urns = set(term._urn for term in (existing_glossary.terms or []))

    latest_glossary.nodes = [
        node for node in (latest_glossary.nodes or []) if node._urn in allowed_node_urns
    ]
    latest_glossary.terms = [
        term for term in (latest_glossary.terms or []) if term._urn in allowed_term_urns
    ]


def replace_urn_refs_with_paths(
    glossary: BusinessGlossaryConfig, path_to_id_map: Dict[str, str]
) -> None:
    urn_to_path_map = {urn: path for (path, urn) in path_to_id_map.items()}

    def _simplify_urn_list(urns: Optional[List[str]]) -> Optional[List[str]]:
        if urns is None:
            return None

        return [urn_to_path_map.get(urn, urn) for urn in urns]

    def _process_child_terms(parent_node: GlossaryNodeInterface) -> None:
        for term in parent_node.terms or []:
            term.inherits = _simplify_urn_list(term.inherits)
            term.contains = _simplify_urn_list(term.contains)
            term.values = _simplify_urn_list(term.values)
            term.related_terms = _simplify_urn_list(term.related_terms)

        for node in parent_node.nodes or []:
            _process_child_terms(node)

    _process_child_terms(glossary)


def _align_glossary_elements(
    latest: List[_GlossaryElement], existing: List[_GlossaryElement]
) -> Iterable[Tuple[Optional[_GlossaryElement], Optional[_GlossaryElement]]]:
    latest_by_id = {element._urn: element for element in latest}
    existing_by_id = {element._urn: element for element in existing}

    # Get all unique IDs and sort them alphabetically
    all_ids = sorted(set(latest_by_id.keys()) | set(existing_by_id.keys()))
    
    # Process each ID in alphabetical order
    for id in all_ids:
        if id in latest_by_id and id in existing_by_id:
            yield latest_by_id[id], existing_by_id[id]
        elif id in latest_by_id:
            yield latest_by_id[id], None
        else:
            yield None, existing_by_id[id]


def glossary_to_dict_minimize_diffs(
    latest_glossary: BusinessGlossaryConfig, existing_glossary: BusinessGlossaryConfig
) -> dict:
    def _simple_elem_to_dict(
        latest_elem: Union[BusinessGlossaryConfig, _GlossaryElement],
        existing_elem: Union[None, BusinessGlossaryConfig, _GlossaryElement],
        defaults: DefaultConfig,
        exclude: Optional[Set[str]] = None,
    ) -> dict:
        if isinstance(latest_elem, BusinessGlossaryConfig):
            return latest_elem.dict(
                exclude=exclude, exclude_defaults=True, exclude_none=True
            )
        assert not isinstance(existing_elem, BusinessGlossaryConfig)

        # Exclude fields that are default values here AND are not set in the existing glossary.
        #
        # In other words, a field will be included if:
        # 1. is set in the existing glossary or
        # 2. the value in the latest glossary is not a default value (and this isn't the top-level config)
        exclude = (exclude or set()).copy()

        # Always exclude audit properties from the YAML output
        exclude.add("custom_properties")

        # Exclude any fields that match the defaults.
        if defaults.owners is not None and defaults.owners == latest_elem.owners:
            exclude.add("owners")
        if isinstance(latest_elem, GlossaryTermConfig):
            if (
                defaults.source is not None
                and defaults.source == latest_elem.source_ref
            ):
                exclude.add("source_ref")
            if defaults.url == latest_elem.source_url:
                exclude.add("source_url")
            if defaults.source_type == latest_elem.term_source:
                exclude.add("term_source")

        if existing_elem is not None:
            # SUBTLE: We can drop the ID here because we know that existing_elem._urn
            # matches latest_elem._urn. If existing_elem is not None, then
            # we know the id field is redundant if it is not set in the existing glossary.
            exclude.add("id")

            # Make sure to include any fields that are explicitly set in the existing glossary.
            existing_set_keys = existing_elem.__fields_set__
            exclude -= existing_set_keys
            
            # Re-add custom_properties to exclude list to ensure they don't appear in YAML
            exclude.add("custom_properties")
            
            # If custom_properties was explicitly set in the existing glossary, 
            # preserve only non-audit properties
            if "custom_properties" in existing_set_keys and existing_elem.custom_properties:
                # Get existing custom properties
                existing_custom_props = existing_elem.custom_properties.copy()
                
                # Filter out audit properties
                audit_props = {"last_modified_time", "last_modified_actor", 
                              "last_modified_impersonator", "removed"}
                filtered_props = {k: v for k, v in existing_custom_props.items() 
                                 if k not in audit_props}
                
                # Only include custom_properties if there are non-audit properties
                if filtered_props:
                    exclude.remove("custom_properties")
                    # Replace with filtered properties
                    fields = latest_elem.dict(
                        exclude=exclude,
                        exclude_defaults=True,
                        exclude_none=True,
                    )
                    fields["custom_properties"] = filtered_props
                    return fields

        fields = latest_elem.dict(
            exclude=exclude,
            exclude_defaults=True,
            exclude_none=True,
        )

        return fields

    def _to_dict(
        latest_node: GlossaryNodeInterface,
        existing_node: GlossaryNodeInterface,
        defaults: DefaultConfig,
    ) -> dict:
        # Process terms.
        terms = []
        term_pairs = list(_align_glossary_elements(
            latest_node.terms or [], existing_node.terms or []
        ))
        
        # Sort term pairs by the ID of whichever element exists
        term_pairs.sort(key=lambda x: (x[0] or x[1])._urn)
        
        for latest_term_elem, existing_term_elem in term_pairs:
            if latest_term_elem is None:
                terms.append(_DELETED_NODE_TOMBSTONE)
            else:
                terms.append(
                    _simple_elem_to_dict(latest_term_elem, existing_term_elem, defaults)
                )

        # Process nodes.
        nodes = []
        node_pairs = list(_align_glossary_elements(
            latest_node.nodes or [], existing_node.nodes or []
        ))
        
        # Sort node pairs by the ID of whichever element exists
        node_pairs.sort(key=lambda x: (x[0] or x[1])._urn)
        
        for latest_node_elem, existing_node_elem in node_pairs:
            if latest_node_elem is None:
                nodes.append(_DELETED_NODE_TOMBSTONE)
            elif existing_node_elem is None:
                nodes.append(
                    _to_dict(
                        latest_node_elem,
                        existing_node=GlossaryNodeConfig(
                            id=latest_node_elem._urn,
                            name=latest_node_elem.name,
                            description="",
                        ),
                        defaults=defaults,
                    )
                )
            else:
                nodes.append(
                    _to_dict(
                        latest_node_elem,
                        existing_node_elem,
                        # Update defaults with the current node owner, if any.
                        # This way, the "owners" field cascades down to all child nodes.
                        defaults=defaults.copy(
                            update=(
                                dict(owners=existing_node_elem.owners)
                                if existing_node_elem.owners
                                else dict()
                            )
                        ),
                    )
                )

        # Retain other fields as-is.
        other_fields = _simple_elem_to_dict(
            latest_node, existing_node, defaults, exclude={"terms", "nodes"}
        )

        return {
            **other_fields,
            **({"terms": terms} if terms else {}),
            **({"nodes": nodes} if nodes else {}),
        }

    return _to_dict(latest_glossary, existing_glossary, defaults=existing_glossary)


def update_yml_to_match(
    infile: pathlib.Path,
    outfile: pathlib.Path,
    target: dict,
) -> None:
    yaml = YAML()
    yaml.preserve_quotes = True  # type: ignore[assignment]

    doc = yaml.load(infile)

    def _update_doc(doc, target, update_full):
        if isinstance(target, dict):
            if not isinstance(doc, dict):
                update_full(target)
            else:
                for key, value in target.items():
                    if key not in doc:
                        doc[key] = value
                    else:

                        def _update_value(v):
                            doc[key] = v

                        _update_doc(doc[key], value, _update_value)

        elif isinstance(target, list):
            if not isinstance(doc, list):
                update_full(doc, target)
            else:
                # We assume that the two lists are perfectly aligned, which the exception of deletions.
                elems_to_delete = []
                for i, value in enumerate(target):
                    if i >= len(doc):
                        doc.append(value)
                    elif doc[i] == _DELETED_NODE_TOMBSTONE:
                        elems_to_delete.append(i)
                    else:

                        def _update_value(v):
                            doc[i] = v

                        _update_doc(doc[i], value, _update_value)

                for i in reversed(elems_to_delete):
                    del doc[i]
        else:
            update_full(target)

    _update_doc(doc, target, None)

    # Guess existing indentation in the file so that we can preserve it.
    _, ind, bsi = ruamel.yaml.util.load_yaml_guess_indent(infile.read_text())
    yaml.width = 2**20  # type: ignore[assignment]
    yaml.indent(mapping=bsi, sequence=ind, offset=bsi)

    yaml.dump(doc, outfile)


def _format_timestamp(timestamp: int) -> str:
    """Convert millisecond timestamp to human-readable format."""
    from datetime import datetime
    
    # Handle non-integer timestamps
    if not isinstance(timestamp, int):
        try:
            timestamp = int(timestamp)
        except (ValueError, TypeError):
            logger.warning(f"Invalid timestamp format: {timestamp}")
            return "invalid timestamp"
    
    if timestamp == 0:
        return "unknown"
    
    try:
        # Convert milliseconds to seconds for datetime.fromtimestamp
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError) as e:
        logger.warning(f"Error parsing timestamp {timestamp}: {e}")
        return "invalid timestamp"


def _generate_changelog(
    latest_glossary: BusinessGlossaryConfig, 
    existing_glossary: BusinessGlossaryConfig,
    output_file: str,
    hours_lookback: float = 8.25  # 8 hours and 15 minutes
) -> None:
    """Generate a changelog of differences between latest and existing glossary."""
    from datetime import datetime, timedelta
    
    # Calculate the cutoff time (now - lookback period)
    now = datetime.now()
    cutoff_time = now - timedelta(hours=hours_lookback)
    cutoff_timestamp = int(cutoff_time.timestamp() * 1000)  # Convert to milliseconds
    
    logger.info(f"Filtering changes since: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} ({cutoff_timestamp})")
    
    changes = []
    
    # Helper function to extract entities from glossary
    def extract_entities(glossary):
        entities = {}
        
        def process_node(node, path=""):
            current_path = f"{path}/{node.name}" if path else node.name
            entities[node._urn] = {
                "type": "node",
                "name": node.name,
                "path": current_path,
                "custom_properties": getattr(node, "custom_properties", {}) or {},
                "description": getattr(node, "description", ""),
                "owners": getattr(node, "owners", None),
            }
            
            for term in node.terms or []:
                term_path = f"{current_path}/{term.name}"
                entities[term._urn] = {
                    "type": "term",
                    "name": term.name,
                    "path": term_path,
                    "custom_properties": getattr(term, "custom_properties", {}) or {},
                    "description": getattr(term, "description", ""),
                    "owners": getattr(term, "owners", None),
                    "term_source": getattr(term, "term_source", None),
                    "source_ref": getattr(term, "source_ref", None),
                    "source_url": getattr(term, "source_url", None),
                }
                
            for child_node in node.nodes or []:
                process_node(child_node, current_path)
        
        # Process top-level nodes
        for node in glossary.nodes or []:
            process_node(node)
            
        # Process top-level terms
        for term in glossary.terms or []:
            entities[term._urn] = {
                "type": "term",
                "name": term.name,
                "path": term.name,
                "custom_properties": getattr(term, "custom_properties", {}) or {},
                "description": getattr(term, "description", ""),
                "owners": getattr(term, "owners", None),
                "term_source": getattr(term, "term_source", None),
                "source_ref": getattr(term, "source_ref", None),
                "source_url": getattr(term, "source_url", None),
            }
            
        return entities
    
    # Extract entities from both glossaries
    latest_entities = extract_entities(latest_glossary)
    existing_entities = extract_entities(existing_glossary)
    
    # Log counts for debugging
    logger.info(f"Found {len(latest_entities)} entities in latest glossary")
    logger.info(f"Found {len(existing_entities)} entities in existing glossary")
    
    # Find new entities
    for urn, entity in latest_entities.items():
        if urn not in existing_entities:
            actor = entity.get("custom_properties", {}).get("last_modified_actor", "unknown")
            timestamp = entity.get("custom_properties", {}).get("last_modified_time", 0)
            
            # Skip if the change is older than the cutoff time
            if timestamp and int(timestamp) < cutoff_timestamp:
                logger.debug(f"Skipping old entity: {entity['path']} (timestamp: {timestamp})")
                continue
                
            formatted_time = _format_timestamp(timestamp)
            
            logger.info(f"New entity detected: {entity['path']} by {actor} at {formatted_time}")
            
            changes.append({
                "type": "added",
                "entity_type": entity["type"],
                "name": entity["name"],
                "path": entity["path"],
                "actor": _get_id_from_urn(actor) if isinstance(actor, str) and actor.startswith("urn:") else actor,
                "timestamp": formatted_time,
                "raw_timestamp": timestamp,
            })
    
    # Find removed entities - these are always considered recent since they were just detected
    for urn, entity in existing_entities.items():
        if urn not in latest_entities:
            logger.info(f"Removed entity detected: {entity['path']}")
            
            changes.append({
                "type": "removed",
                "entity_type": entity["type"],
                "name": entity["name"],
                "path": entity["path"],
                "actor": "unknown",  # We don't know who removed it
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "raw_timestamp": int(now.timestamp() * 1000),
            })
    
    # Find modified entities
    for urn, latest_entity in latest_entities.items():
        if urn in existing_entities:
            existing_entity = existing_entities[urn]
            change_details = []
            
            # Check for name changes
            if latest_entity["name"] != existing_entity["name"]:
                change_details.append(f"name changed from '{existing_entity['name']}' to '{latest_entity['name']}'")
            
            # Check for description changes
            if latest_entity["description"] != existing_entity["description"]:
                change_details.append("description updated")
            
            # Check for term source changes
            if latest_entity.get("term_source") != existing_entity.get("term_source"):
                change_details.append(f"term source changed from '{existing_entity.get('term_source')}' to '{latest_entity.get('term_source')}'")
            
            # Check for source ref changes
            if latest_entity.get("source_ref") != existing_entity.get("source_ref"):
                change_details.append(f"source reference changed from '{existing_entity.get('source_ref')}' to '{latest_entity.get('source_ref')}'")
            
            # Check for source URL changes
            if latest_entity.get("source_url") != existing_entity.get("source_url"):
                change_details.append(f"source URL changed from '{existing_entity.get('source_url')}' to '{latest_entity.get('source_url')}'")
            
            # If we found changes
            if change_details:
                actor = latest_entity.get("custom_properties", {}).get("last_modified_actor", "unknown")
                timestamp = latest_entity.get("custom_properties", {}).get("last_modified_time", 0)
                
                # Skip if the change is older than the cutoff time
                if timestamp and int(timestamp) < cutoff_timestamp:
                    logger.debug(f"Skipping old update: {latest_entity['path']} (timestamp: {timestamp})")
                    continue
                    
                formatted_time = _format_timestamp(timestamp)
                
                logger.info(f"Updated entity detected: {latest_entity['path']} - {', '.join(change_details)}")
                
                changes.append({
                    "type": "updated",
                    "entity_type": latest_entity["type"],
                    "name": latest_entity["name"],
                    "path": latest_entity["path"],
                    "details": change_details,
                    "actor": _get_id_from_urn(actor) if isinstance(actor, str) and actor.startswith("urn:") else actor,
                    "timestamp": formatted_time,
                    "raw_timestamp": timestamp,
                })
    
    # Sort changes by timestamp
    changes.sort(key=lambda x: x.get("raw_timestamp", 0), reverse=True)
    
    # Write changelog to file
    with open(output_file, "w") as f:
        f.write("# Glossary Changelog\n\n")
        f.write(f"Generated on: {now.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Showing changes since: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for change in changes:
            if change["type"] == "added":
                f.write(f"* **Added {change['entity_type']}**: {change['path']} by {change['actor']} on {change['timestamp']}\n")
            elif change["type"] == "removed":
                f.write(f"* **Removed {change['entity_type']}**: {change['path']} on {change['timestamp']}\n")
            elif change["type"] == "renamed":
                f.write(f"* **Renamed {change['entity_type']}**: {change['old_name']} → {change['new_name']} by {change['actor']} on {change['timestamp']}\n")
            elif change["type"] == "updated":
                f.write(f"* **Updated {change['entity_type']}**: {change['path']} by {change['actor']} on {change['timestamp']}\n")
                f.write(f"  * Changes: {', '.join(change['details'])}\n")
        
        if not changes:
            f.write("No changes detected in the past 8 hours and 15 minutes.\n")


def _update_glossary_file(
    file: str, enable_auto_id: bool, output: Optional[str], prune: bool = True, 
    changelog_file: Optional[str] = None
) -> None:
    if not output:
        output = file

    logger.info("Read the existing biz glossary file")
    existing_glossary = BusinessGlossaryFileSource.load_glossary_config(file)
    materialize_all_node_urns(existing_glossary, enable_auto_id=enable_auto_id)

    logger.info("Fetch the latest glossary from DataHub")
    top_level_nodes, top_level_terms = fetch_datahub_glossary()
    latest_glossary = existing_glossary.copy(
        update=dict(
            nodes=top_level_nodes,
            terms=top_level_terms,
        ),
        deep=True,
    )

    # Generate changelog if requested
    if changelog_file:
        logger.info(f"Generating changelog to {changelog_file}")
        _generate_changelog(latest_glossary, existing_glossary, changelog_file)

    logger.info("Prune the latest glossary to only include file-managed nodes/terms")
    if prune:
        prune_latest_glossary(
            latest_glossary=latest_glossary,
            existing_glossary=existing_glossary,
        )

    logger.info("Recursively simplify urn references where possible.")
    path_to_id_map = populate_path_vs_id(latest_glossary)
    replace_urn_refs_with_paths(latest_glossary, path_to_id_map=path_to_id_map)

    logger.info(
        "Minimize diffs between the two glossaries using the field defaults and default config."
    )
    obj = glossary_to_dict_minimize_diffs(
        latest_glossary=latest_glossary,
        existing_glossary=existing_glossary,
    )

    logger.info("Serialize back into yaml.")
    update_yml_to_match(pathlib.Path(file), pathlib.Path(output), obj)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--enable-auto-id", type=bool, required=True)
@click.option("--file", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--prune", type=bool, default=False, required=False)
@click.option("--output", type=click.Path())
@click.option("--changelog", type=click.Path(), help="Path to output changelog file")
def update_glossary_file(
    file: str, enable_auto_id: bool, prune: bool, output: Optional[str], changelog: Optional[str]
) -> None:
    if not output:
        output = file

    _update_glossary_file(
        file, enable_auto_id=enable_auto_id, output=output, prune=prune, changelog_file=changelog
    )


@cli.command()
@click.option("--output", type=click.Path(), required=True)
@click.option("--default-source-ref")
@click.option("--default-source-url")
@click.option("--default-source-type")
@click.option("--changelog", type=click.Path(), help="Path to output changelog file")
def bootstrap_glossary_yml(
    output: str,
    default_source_ref: Optional[str],
    default_source_url: Optional[str],
    default_source_type: Optional[str],
    changelog: Optional[str],
) -> None:
    default_yml = """
# This file was auto-generated by the biz glossary CLI.
version: 1
owners: {}
"""
    if default_source_ref:
        default_yml += f"source: {default_source_ref}\n"
    if default_source_url:
        default_yml += f"url: {default_source_url}\n"
    if default_source_type:
        default_yml += f"source_type: {default_source_type}\n"

    pathlib.Path(output).write_text(default_yml)

    _update_glossary_file(output, enable_auto_id=True, output=output, prune=False, changelog_file=changelog)


if __name__ == "__main__":
    cli()
