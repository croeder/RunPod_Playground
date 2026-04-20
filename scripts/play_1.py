
import inspect
import requests
import json


if  False:
    from monarch_py.implementations.solr.solr_implementation import SolrImplementation
    #si = SolrImplementation()
    #si = SolrImplementation(solr_url="https://solr.monarchinitiative.org/solr/monarch_kg")
    #print(inspect.signature(SolrImplementation.__init__))
    si = SolrImplementation(base_url="https://solr.monarchinitiative.org/solr/monarch_kg")

    #print(inspect.signature(si.get_entity))
    result = si.get_entity("MONDO:0007522", extra=False)  # Ehlers-Danlos syndrome
    print(result)


#r = requests.get("https://api.monarchinitiative.org/v3/entity/MONDO:0007522")
#r = requests.get("https://api.monarchinitiative.org/v3/entities/MONDO:0007522")
#print(r.json())

#r = requests.get("https://api.monarchinitiative.org/v3/openapi.json")
#r = requests.get("https://api.monarchinitiative.org")
#print(r.status_code)
#print(r.text[:500])
#print(json.dumps(r.json()["paths"], indent=2))
#print(json.dumps(r.json(), indent=2))

if False:
    r = requests.get("https://api.monarchinitiative.org/openapi.json")
    paths = r.json()["paths"]
    for path in paths:
        print(path)

if True:
    r = requests.get("https://api.monarchinitiative.org/v3/api/entity/MONDO:0007522")
    print(json.dumps(r.json(), indent=2))
    #print(list(r.json().keys()))

    #print(json.dumps(r.json()["association_counts"], indent=2))
    #print(json.dumps(r.json()["causal_gene"], indent=2))
