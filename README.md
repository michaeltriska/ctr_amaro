CRT calculator
=============================

Backend for the CRT calculator

Dependencies
------------

* Python >= 3.5, virtualenv
* Make

Building
--------

To **build** the project run:

make

Start API
--------

To **start** the API run:

make start
    
Example curl
--------

To get a response use the example curl in another terminal while API is running:

curl -X POST localhost/crt/v2 -d '{"startTimestamp" : "2016-01-03 13:55:00","endTimestamp" : "2019-01-04 13:55:00","aggregation" : 60}'


Testing
-------

The **unit tests** live in the main directory.
To run the unit tests of the project run:

make test

