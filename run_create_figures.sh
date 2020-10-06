#! /bin/bash

if [ -z "$1" ]
  then
    echo "ERROR: Please provide the name of the location for the figures or 'ALL' to compute all."
else
    if [ "$1" = "ALL" ] || [ "$1" = "cucuta" ]; then
        echo 'Cucuta'
        # Cucuta
        # Transits Housing
        python figure_generation/scripts/transits_housing_plots.py reporte_norte_de_santander 30 colombia_cucuta

        # Movement Plots
        python figure_generation/scripts/movement_plots.py reporte_norte_de_santander 30 colombia_cucuta_comuna_*

        # Centrality
        python figure_generation/scripts/centrality_housing_plots.py reporte_norte_de_santander colombia_cucuta pagerank_centrality 30 5000 2
        
        # Edgelist
        python figure_generation/scripts/edgelist_plots.py reporte_norte_de_santander colombia_cucuta 30 3        
    
    elif [ "$1" = "ALL" ] || [ "$1" = "palmira" ] 
        then
        echo 'Palmira'

        # Plamira
        # Transits Housing
        python figure_generation/scripts/transits_housing_plots.py reporte_palmira 30 colombia_palmira

        # Movement Plots
        python figure_generation/scripts/movement_plots.py reporte_palmira 30 colombia_palmira_comuna_*

        # Centrality
        python figure_generation/scripts/centrality_housing_plots.py reporte_palmira colombia_palmira personalized_pagerank_centrality 30 1000 2.5

        # Edgelist
        python figure_generation/scripts/edgelist_plots.py reporte_palmira colombia_palmira 30 2.5

    elif [ "$1" = "ALL" ] || [ "$1" = "ibague" ] 
        then
        echo 'Ibague'
        # Transits Housing
        python figure_generation/scripts/transits_housing_plots.py reporte_tolima 30 colombia_ibague

        # Centrality
        python figure_generation/scripts/centrality_housing_plots.py reporte_tolima colombia_ibague pagerank_centrality 30 5000 2

        # Edgelist
        python figure_generation/scripts/edgelist_plots.py reporte_tolima colombia_ibague 30 2

    elif [ "$1" = "ALL" ] || [ "$1" = "armenia" ] 
        then
        echo 'Armenia'
        # Transits Housing
        python figure_generation/scripts/transits_housing_plots.py reporte_quindio 30 colombia_armenia

        # Centrality
        python figure_generation/scripts/centrality_housing_plots.py reporte_quindio colombia_armenia pagerank_centrality 30 5000 2

        # Edgelist
        python figure_generation/scripts/edgelist_plots.py reporte_quindio colombia_armenia 30 5
    
    else
        echo "Parameter not foud"
        
    fi
fi
