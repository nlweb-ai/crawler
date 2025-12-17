#!/bin/bash

# API configuration
API_URL="http://localhost:5001/api/sites"
API_KEY="3Ci2GfAVUH0zQ_LPvRwoeNF89YPnpskfn_G9zyLuhs23JiN3QDhzFFhMP-TxUMw4"
BASE_SCHEMA_URL="https://guha.com/data"

# List of domains
# Already processed "boingo.com"
domains=(
    "techcrunch.com"
    "9to5mac.com"
    "boingboing.net"
    "crackmagazine.net"
    "valuewalk.com"
    "thewaltdisneycompany.com"
    "capgemini.com"
    "plesk.com"
    "siteminder.com"
    "adespresso.com"
    "loggly.com"
    "modpizza.com"
    "cpanel.net"
    "harvard.edu"
    "news.harvard.edu"
    "skillcrush.com"
    "polk.edu"
    "creativecommons.org"
    "rollingstones.com"
    "katyperry.com"
    "usainbolt.com"
    "rafaelnadal.com"
    "snoopdogg.com"
    "riverdance.com"
    "news.microsoft.com"
    "blog.mozilla.org"
    "news.spotify.com"
    "nationalarchives.gov.uk"
    "blog.cpanel.com"
    "news.sap.com"
    "finland.fi"
    "blogs.cisco.com"
    "blog.turbotax.intuit.com"
    "blog.alaskaair.com"
    "airstream.com"
    "wolverineworldwide.com"
    "kff.org"
    "invisiblechildren.com"
    "platformlondon.org"
    "travelportland.com"
    "tim.blog"
    "garyvaynerchuk.com"
    "athemes.com"
    "generatepress.com"
    "wpexplorer.com"
    "studiopress.com"
    "yoast.com"
    "portent.com"
    "tri.be"
    "hmn.md"
    "renweb.com"
    "yelpblog.com"
    "sprott.carleton.ca"
    "pacificrimcollege.online"
    "bytes.co"
    "talentodigital.madrid.es"
    "soapstones.com"
    "codefryx.de"
    "centremarceau.com"
    "riponcathedral.org.uk"
    "engineering.fb.com"
    "blog.pagely.com"
    "daybreaker.com"
    "taylorswift.com"
    "hodgebank.co.uk"
    "newsroom.spotify.com"
    "books.disney.com"
    "vanyaland.com"
    "gizmodo.com"
    "kotaku.com"
    "jezebel.com"
    "theonion.com"
    "avclub.com"
    "clickhole.com"
    "usmagazine.com"
    "hongkiat.com"
    "speckyboy.com"
    "arianagrande.com"
    "postmalone.com"
    "rihanna.com"
    "foofighters.com"
    "vice.com"
    "pinchofyum.com"
    "minimalistbaker.com"
    "cookieandkate.com"
    "skinnytaste.com"
    "budgetbytes.com"
    "sallysbakingaddiction.com"
    "halfbakedharvest.com"
    "theeverygirl.com"
    "entrepreneur.com"
    "thefashionspot.com"
    "outsideonline.com"
    "backpacker.com"
    "trailrunnermag.com"
    "climbing.com"
    "cafemom.com"
    "greenweddingshoes.com"
    "recipetineats.com"
    "onceuponachef.com"
    "ambitiouskitchen.com"
)

# Helper function to wait for queue to be empty
wait_for_queue_empty() {
    echo "Waiting for queue to empty..."
    while true; do
        pending=$(curl -s "http://localhost:5001/api/queue/status" | grep -o '"pending_jobs":[0-9]*' | grep -o '[0-9]*')
        if [ "$pending" = "0" ]; then
            echo "Queue is empty"
            break
        fi
        echo "  Queue has $pending pending jobs, waiting..."
        sleep 5
    done
}

# Process each domain one at a time
for domain in "${domains[@]}"; do
    domain_underscore=$(echo "$domain" | tr '.' '_')
    schema_map_url="${BASE_SCHEMA_URL}/${domain_underscore}/schema_map.xml"
    
    echo "========================================"
    echo "Processing: $domain"
    echo "========================================"
    
    # Delete existing schema files
    echo "Step 1: Deleting existing schema files..."
    curl -s -X DELETE "${API_URL}/${domain}/schema-files" \
        -H "Content-Type: application/json" \
        -H "X-API-Key: ${API_KEY}" \
        -d "{\"schema_map_url\": \"${schema_map_url}\"}"
    echo ""
    
    # Wait for deletions to complete
    wait_for_queue_empty
    
    # Add schema files
    echo "Step 2: Adding schema files..."
    curl -s -X POST "${API_URL}/${domain}/schema-files" \
        -H "Content-Type: application/json" \
        -H "X-API-Key: ${API_KEY}" \
        -d "{\"schema_map_url\": \"${schema_map_url}\"}"
    echo ""
    
    # Wait for additions to complete
    wait_for_queue_empty
    
    echo "✓ Completed $domain"
    echo ""
done

echo "========================================"
echo "All domains processed!"
echo "========================================"
