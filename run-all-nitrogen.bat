rem python run-multivariable-experiments.py server=cluster1 crop=winter-wheat
rem move 1 1_WW
rem python run-multivariable-experiments.py server=cluster1 crop=winter-barley
rem move 1 1_WB
rem python run-multivariable-experiments.py server=cluster1 crop=winter-rye
rem move 1 1_WR
python run-multivariable-experiments.py server=cluster1 crop=spring-barley
move 1 1_SB
python run-multivariable-experiments.py server=cluster1 crop=potato
move 1 1_PO
rem python run-multivariable-experiments.py server=cluster1 crop=sugar-beet
rem move 1 1_SBee
rem python run-multivariable-experiments.py server=cluster1 crop=winter-rape
rem move 1 1_WRa
rem python run-multivariable-experiments.py server=cluster1 crop=silage-maize
rem move 1 1_SM
rem python run-multivariable-experiments.py server=cluster1 crop=grain-maize
rem move 1 1_GM
