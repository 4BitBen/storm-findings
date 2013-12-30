# storm-findings
The purpose of this repo is to better understand the storm framework. It is meant to answer questions I had that were
either not covered in the [storm-starter](https://github.com/nathanmarz/storm-starter) repo or unclear in the
[documentation](http://storm-project.net/documentation.html).

The first questions I had were the following:

1. How to put in a backflow pressure to stop a spout? What affect does this have on the topology and the cluster?
2. How do acks and failures work?

# Overview
## How to Run
    >gradle run

## What to Expect
There will be two log files created: correct-topology.log and misbehaving-topology.log. Each represent a topology. The
misbehaving topology has a spout and two bolts. One bolt misbehaves and it affects the other bolt in the topology. The
correct topology runs and is not affected by the misbehaving topology.

Also, ACKs are set in the bolts. You should see all ACKs in the correct topology. You should see some ACKs in the
misbehaving topology once the longer defined PrintAndSleepBolt processes the tuple.

Currently, I do not see any failures in the misbehaving topology. I do not know why.

#TODOs
1. Better understand how failures work. I am not able to see them currently in the misbehaving topology. According to
the [documentation](https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing), the default message
timeout is 30 seconds, so the one minute sleep in the bolt should be longer and cause the fail.