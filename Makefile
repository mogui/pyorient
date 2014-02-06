
# all: clean install

# install:
# 	python setup.py install

.builddev:
	# scarica una build di orient db e falla partire per i test
	python setup.py develop
	touch .builddev

dev: .builddev
	DEBUG=1 nosetests --nocapture test/devtest.py

clean:
	rm -rf ./build ./dist ./*.pyc ./test/*.pyc

test:
	nosetests test/test_*