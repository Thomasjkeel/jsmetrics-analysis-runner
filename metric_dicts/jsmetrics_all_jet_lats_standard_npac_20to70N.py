from jsmetrics.metrics import jet_statistics

METRIC_DICT = {
    "Woollings2010_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.woollings_et_al_2010,
        "name": "Woollings et al. 2010 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "BarnesPolvani2013_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.barnes_polvani_2013,
        "name": "Barnes & Polvani 2013 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "BarnesPolvani2015_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [0, 90], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.barnes_polvani_2015,
        "name": "Barnes & Polvani 2015 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "BarnesSimpson2017_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.barnes_simpson_2017,
        "name": "Barnes & Simpson 2017 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "GrisePolvani2017_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.grise_polvani_2017,
        "name": "Grise & Polvani 2017 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "Bracegirdle2018_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.bracegirdle_et_al_2018,
        "name": "Bracegirdle et al. 2018 North Pacific",
        "variable_name": "annual_JPOS",
        "description": "",
    },
    "Ceppi2018_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.ceppi_et_al_2018,
        "name": "Ceppi et al. 2018 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "Zappa2018_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.zappa_et_al_2018,
        "name": "Zappa et al. 2018 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
    "Kerr2020_NorthPacific": {
        "variables": ["ua"],
        "coords": {"plev": [70000, 85000],  "lat": [20, 70], "lon": [120, 240]},
        "plev_units": "Pa",
        "metric": jet_statistics.kerr_et_al_2020,
        "name": "Kerr et al. 2020 North Pacific",
        "variable_name": "jet_lat",
        "description": "",
    },
}
