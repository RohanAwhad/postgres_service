so i have a postgres-instance, and because i am a student i have to be very careful with my aws credits (which i haven't been)

and i recently created a controller to scale up to 15 instances and then back down to zero for a uni proj. so i wanted to create something like that for this instance.

my current solution is a bit coupled. its like a sidecar pattern. i have a postgres client, that has all the configuration to start, stop and connect to the instance.

my app can do a normal db operation at port 5432, but the host is the pg-client. the pg-client starts the instance on the first request, and sends a retry later message, while the instance is starting up. Post starting the instance, the just acts as a proxy in between postgres and my app.

and in the bg it also starts a thread which listens for an event with a wait timeout  of 15 mins. If there is no event, it shuts down the instance, else timeout is reset.

the best thing is, i did all of this at socket level, and it is refreshing to use concepts i had learned like 5 years back.

the current implementation is in python, but will be moving to golang soon.
