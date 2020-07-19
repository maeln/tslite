# TSLite : A small embeddable time-serie database

[![Build Status](https://travis-ci.com/maeln/tslite.svg?branch=master)](tslite)
[![Crates.io](https://img.shields.io/crates/v/tslite)](https://crates.io/crates/tslite)


TSLite is a small and embeddable time-serie database that operate directly on a file.
It has no concept of type, right now you can store anything that fit into 1 octet.

For more information look at the documentation : 
```
git clone https://github.com/maeln/tslite.git
cd tslite
cargo doc --open
```