echo off

for %%i in (10,17,18) do ( rem ,5,6,7,11,12,13,14,15,16,2,3,4) do (
	rem echo i=%%i
	python run-work-producer.py shared_id=mib-vocs-%%i setups-file=sim_setups_voce.csv run-setups=[%%i] server=cluster1 no-data-port=555%%i
)