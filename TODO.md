# TODO

- [ ] fill out authzeeasync
    - How to handle errors

- [ ] add has_failed to the authorizeresult

- [ ] internal json serializer just take string?

- [ ] dataclasses vs dicts
    - It is easier to read dicts to me
    - dataclasses are easier to program
    - docs are the dicts make it really easy though
    - except either and return dataclasses? 
    - I think using the dictionaries internally is the lowest level, but can be turned into anything
        - Creating a DC or pydantic wrapper is trivial
        - Could be included in the package for dataclasses and a simple pydantic wrapper authzee-pydantic


- [ ] Def schemas need to check for base object

- [ ] errors for putting and deleted and getting defs/grants
    - put and delete should not care if it exists either way
    - get should return error since it expects to be there??

- [ ] define where authzee class and modules do the validation
    - Requests are handled by compute, 
    - defs are done at the authzee level
    - Need to document this in the SDK docs or just this packages at least

- [ ] switch reference to use jsonschema-rs and switch in the spec too

- [ ] paginators 
    - def want something in the backend
        - Done
    - Should these be at the Authzee level? 
        - Authzee.paginate_grants()
        - I think this would be nice to have, just return an async page generator

- [ ] core.py functionality
    - validate the request
    - run audit on a page
    - run authorize

- [ ] make a minimum product with the SDK. 

- [ ] regenerate __all__s
    - [x] exceptions

- [ ] for iterators may need to manage own event loop instead of just using asyncio.run

- [ ] jmespath rust bindings for python

- [ ] thing about caching for defs

- [x] Authzee Config break out page size of each type of def into it's own size

- [x] validate request, could have page size for identity defs
    - When you run an op like Audit, when you pass it down should you have to add in the grant page size and def page size? 
    - Maybe we should just continue with the AuthzeeConfig and pass that to both the Authzee class and the compute/storage modules???
        - Upside is it's extremely flexible with the configuration of items
            - things like audit should only need the request, page_ref, and the authzee config
            - That get's passed as far down as it needs to and everything can just pluck the configs from there
        - downsides
            - passing yet another value everywhere, upside is it replaces the extra configs
            - pretty small now but may get bigger, thus performance issues passing this thing everywhere??
    - **SOLUTION** use authzee_config for all

- [x] validation
    - validate defs doesn't need to be split since it's just validating the schema and schema base type is object
        - Since they are always puts, we are just going to update it if it exists
        - Can just do this completely in process right??
    - Validate request and batch request will need to list and cache all defs
        - This should be able to be off loaded

- [x] opaque struct for optional parameters
    - Should this be a single config object to pass to the create struct, and to all methods? 
    - should I do this in python and java too? 
    - AuthzeeConfig
        - grants_page_size
            - get_grants_page
        - grant_refs_size
            - get_grant_refs_page
        - authorize_parallel_paging
            - authorize
        - batch_authorize_parallel_paging
            - batch_authorize
        - raise_crits
            - all methods if a critical error occurs
    - may just be easier to do this with all of them. 
        - override the authzee configs
    - this is really only needed to use slightly different configs and not spinning up a bunch of compute/storage resources
    - Is there a better way to handle that
        - If the compute resources are outside of this??
        - fixed thread pool or process pool and send the tasks to a queue? 
        - That's fine but do we really need to do that for all versions of this or just pass in the config when you need it???
        - Even then there may still be a way that the configs should be overridden.  Even with like storage threads, it may just be easier to manage a couple of configs for the same pool of compute and storage workers, then send that instead. 
    - This makes it easy to manage if they are all wrapped up into one.
    - **SOLUTION** - Let's do the config as a dataclass and this will align more with other languages as well.

- [x] when to use pydantic or data classes over raw. this is up to me but should be done from the beginning. 
    - Need to weigh this because it would be a slim wrapper to put around Authzee if you want data classes and pydantic
    - data classes is built it, so if I was going to do it, it would be better. 
    - honestly just need to create  test and try it out both ways. 
        - Dicts are fast to create, but data classes are more IDE friendly in general and very low performance implications
        - for either it is fairly easy to add wrappers for pydantic or anything
    - **Solution** - let's try dataclass

- [x] What to do with the SDK
    - Multi-types
        - Java can make this work
        - C 
            - 0 for int null
            - null for str null
            - use opaque structs
    - default values - only use where you can or else they will have to pass the nulls or the defaults
        - pass opaque 
    - config object for authzee defaults? 
        - This makes it easier to add to for other languages
        - or else yo have to do var args or something like that
        - can add defaults to C in the starter method 
            - or if just the struct is opaque for C, then you can add to it. 
    - The future of adding new values to functions?
        - would be easiest to encapsulate new functionality in a struct or config
        - That config is passed when creating Authzee and will be the defaults
        - That config can also be passed to methods to override the defaults. 
        - python is nice cause you can just add it to the 
    

- [x] - Include page and parallel settings in authorize()?
    - yes, these need to be included for overrides where needed.  
    - should set defaults at class level, and method should use default values where they can


- Better Fan Out MP Compute
    - Audit - parallel off
        - Send request to worker to process
    - Audit - Parallel on
        - get the list of things and send one out to each process then collect results
    - authorize - parallel off
        - send request to process for deny with deny latch and aio tx pipe
            - Worker gets first page and sends next ref back
            - then continues 
- [x] for fanout mp you need threads for each what?
    - There is a process pool that is limited but the technically you could keep sending requests and they would need more threads
    - If you don't and say you send a request 10 requests to the process pool and only 5 workers are in the pool. 
        - The first 5 are picked up by the pool and the other 5 are waiting
        - The waiting 5 have the connection recv functions dealt out to the thread pool first though. 
        - IDK if the sending ones will have have to wait for recv or not
        - Probably better just to use a package for this something like https://github.com/kchmck/aiopipe/tree/master
- [x] authorize needs to do many at once
    - It would be a bad use of resources for if I list 100 or 1000 things to authorize each one separately
- [x] add default None for methods after generating rust version. 
- [x] Add all exceptions at the authzee level
    - optional
- [x] multiprocess compute - multiple modes
    - simple 
        - Every request just gets sent to a process in a pool and all compute is done in that single process
    - legacy
        - All work is offloaded to a process pool but the main process controls all of it
        - A complex request process would be expected to do this work
    - complex
        - 2 pools of processes.  One for handling requests and one for workers. If you only have one pool you may get into deadlock where you have requests taking all of the processes
        - For single page eval it only uses a request handling process
        - For multi-page eval it will use worker processes as well
        - For authorization it will use a request process and many worker processes.
            - serial paging should be able to chain worker processes, either by returning in the next page ref to the request process or by a worker submitting another task to the pool
                - This can basically be a dupe of the legacy setup because that works really well
            - Parallel paging should send out a page of pages at a time until all pages are sent then wait for a response 
    - 

- [x] Storage for SQL
    - Parallel paging the uses limit paging
        - faster because it's parallel but for a lot of pages may actually be slower
    - single paging that uses id pages

- [x] Distributed compute with redis

- [x] For storage that is in the process, we have to copy it from authzee to the compute module
    - In authzee start do we just set `self._compute._storage = self._storage` 
    - or do we pass it in the the start method as an option to the compute engine?
        - Thi is probably the smarter way, since compute can not pass it around as usual if it has any start sections that pass around the storage module?
    - **SOLUTION** - Or for storage we require that you pass in the dictionary where you want to store it as an arg, then that is just passed along like a pointer to all other storage modules that are created.
- [x] should break up the core authorize steps into separate functions to make it easier for the compute to reuse it.
    - Authorize_deny to evaluate deny grants
    - authorize_allow to evaluate allow grants and 
    - should need to separate allow or deny as the function is unique to that, and should assume that the action matches as well. Besides that the authorization function is nicely reusable. 
    - **SOLUTION** - maybe later it's not that bad now. 
- [x] Should authorize return errors as well? 
    - It's not really scalable to do that. 
    - With audit it's done a page or few at a time because they could all be errors. 
    - Authorize should only return whether it is authorized or not and the reason.  
    - Should be scalable and optimized around the decision, not the errors. 
    - It should only return whatever the critical error it was that caused it to fail. 
    - Can still return the same response but it should only be the list of critical errors. S
- [x] optionally check output schemas
    - **SOLUTION** - this should just be in tests. 
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
    - **SOLUTION** - grants get a string but the lookup is UUID.  Should be a list because that only speeds up repeal and get by UUID. c