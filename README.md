oSPARC Meta Parallel Runner
===========================

This service is part of the Metamodelling framework in 
**o<sup>2</sup>S<sup>2</sup>PARC**. It allows the user to run studies in 
parallel on the platform. 

Test
----

To run a local test:

```
make build
make run-local
```
Deploy to local **o<sup>2</sup>S<sup>2</sup>PARC** registry
-------------------------------------------------------

To run publish to the local registry:

```
make publish-local
```

Deploy to main **o<sup>2</sup>S<sup>2</sup>PARC** registry
------------------------------------------------------

Create a PR request in this repository, and make sure to run 
make version-patch to increase the version number. 

Once the PR is merged into master, go to the internal gitlab repository: 
sparc-external. Add the new version number to the relevant block in the 
sync-workflow.yml file in that repository.

These steps should make the system deploy the new version automatically.

