the code on node.py is an implementation of the standard bully algorithm.
OOP was used as it offers better code organization.
What is the Bully algorithm?
        This is a standard synchronization algorithm that avoids loops by electing a leader node P to be the coordinator of this election.
        The network periodically sends AYA messages to the coordinator and any nearby nodes. The coordinator must send a reply with an IAA message 