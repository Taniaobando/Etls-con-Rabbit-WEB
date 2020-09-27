# makefile for medicallocator

venv: venv/bin/activate
venv/bin/activate: requirements.txt
	py -m venv env 
	.\env/Scripts/pip3 install -Ur requirements.txt

clean:
	del env


