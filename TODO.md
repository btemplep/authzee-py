# TODO

- [ ] How to return errors?
    - Not really a unified way to do this... 
    - Could just be at least standard for different errors. 
    - **SOLUTION** - make it standard for each error type. 

- [ ] Run a full audit in the background and store results?
    - Probably should just leave this to the client to manage, unless storage is going to need to facilitate this as well
    - Either way it will have to be paginated since they can't really retrieve all of the results at once

- [ ] compute should clean up latches? 
    - storage should just paginate latches it's up to compute to clean up failed latches. 

- [x] store grants with UUID as str in InMemoryDB
    - probably should be it's all stored as string and whatnot
    - or I could index by UUID and then just store grant as a string
    - **SOLUTION** - grants get a string but the lookup is UUID.  Should be a list because that only speeds up repeal and get by UUID. 