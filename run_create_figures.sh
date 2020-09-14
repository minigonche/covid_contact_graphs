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

# Plamira
# Transits Housing
python figure_generation/scripts/transits_housing_plots.py reporte_palmira 30 colombia_palmira
# Movement Plots
python figure_generation/scripts/movement_plots.py reporte_palmira 30 colombia_palmira_comuna_*
# Centrality
python figure_generation/scripts/centrality_housing_plots.py reporte_palmira colombia_palmira pagerank_centrality 30 1000 2.5

echo 'Ibague'
# Transits Housing
python figure_generation/scripts/transits_housing_plots.py reporte_tolima 30 colombia_ibague

# Centrality
python figure_generation/scripts/centrality_housing_plots.py reporte_tolima colombia_ibague pagerank_centrality 30 5000 2

echo 'Armenia'
# Transits Housing
python figure_generation/scripts/transits_housing_plots.py reporte_quindio 30 colombia_armenia

# Centrality
python figure_generation/scripts/centrality_housing_plots.py reporte_quindio colombia_armenia pagerank_centrality 30 5000 2