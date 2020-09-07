#! /bin/bash
echo 'Cucuta'
# Cucuta
# Transits Housing
python figure_generation/scripts/transits_housing_plots.py reporte_norte_de_santander 30 colombia_cucuta
# Movement Plots
python figure_generation/scripts/movement_plots.py reporte_norte_de_santander 30 colombia_cucuta_comuna_*
# Centrality
python figure_generation/scripts/centrality_housing_plots.py reporte_norte_de_santander colombia_cucuta pagerank_centrality 30 5000 2

echo 'Palmira'

# Cucuta
# Transits Housing
python figure_generation/scripts/transits_housing_plots.py reporte_palmira 30 colombia_palmira
# Movement Plots
python figure_generation/scripts/movement_plots.py reporte_palmira 30 colombia_palmira_comuna_*
# Centrality
python figure_generation/scripts/centrality_housing_plots.py reporte_palmira colombia_palmira personalized_pagerank_centrality 60 1000 2.5