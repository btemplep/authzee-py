# TODO


- [ ] add default None for methods after generating rust version. 
- [ ] should break up the core authorize steps into separate functions to make it easier for the compute to reuse it.
    - Authorize_deny to evaluate deny grants
    - authorize_allow to evaluate allow grants and 
    - should need to separate allow or deny as the function is unique to that, and should assume that the action matches as well. Besides that the authorization function is nicely reusable. 
- [ ] optionally check output schemas
- [ ] Add all exceptions at the authzee level


- [x] default for parallel paging should check storage if compatible
- [x] How to return errors?
    - Not really a unified way to do this... 
    - Could just be at least standard for different errors. 
    - how to handle for different workflows
    - Maybe this should only include the errors for the workflow exceptions? 
    - The other part of the workflow was just so it returned a schema response.
    - **SOLUTION** - make it standard the all spec errors return the errors fields, and exceptions are raised at the authzee level.
- [x] next_page_ref in audit page
- [x] Run a full audit in the background and store results?
    - Probably should just leave this to the client to manage, unless storage is going to need to facilitate this as well
    - Either way it will have to be paginated since they can't really retrieve all of the results at once
    - **Solution** - not now
- [x] compute should clean up latches? 
    - storage should just paginate latches it's up to compute to clean up failed latches. 
    - **solution** - no just do it with storage.  should paginate behind the scenes as needed.
- [x] store grants with UUID as str in InMemoryDB
    - probably should be it's all stored as string and whatnot
    - or I could index by UUID and then just store grant as a string
    - **SOLUTION** - grants get a string but the lookup is UUID.  Should be a list because that only speeds up repeal and get by UUID. 