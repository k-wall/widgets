

Create the following users:

```
scripts/add-crc-user.sh example
scripts/add-crc-user.sh widgets
```
  

Create the namespace and apply the bundle:
```
oc login -u widgets -p widgets
oc create namespace wiggets
oc apply -f yaml/
oc login -u developer -p developer

```