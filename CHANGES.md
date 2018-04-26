## Changes

## 0.5.0

* Now works with both python2 and python3! 
* Unicode strings stored in a lazy table in python2 are converted to utf8 encoded bytes at read time. Unicode strings stored in a lazy table in python3 go in and out as unicode `str` types.

## 0.4.0

* BREAKING CHANGE swap arguments for update() and upsert(), matching dict is now first argument
* getone() renamed get_one(), legacy getone() still in library
* None values now map to NULL in sqlite values for matching
