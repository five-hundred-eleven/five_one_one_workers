# priorities
# for use in the `five_one_one_workers.message.Message` priority slot
CRITICAL = 10
HIGH = 20
LOW = 30
TRIVIAL = 40

# following should only be internal use
FIRST = 1
LAST = 100

# following constants are special cases for the "to" slot
# hr worker assigns ids to new workers
HR = "HUMAN RESOURCES"
# postmaster delivers messages
POSTMASTER = "POSTMASTER"
# manager is a special case in which the "manager" of name given by "sender"
# will
# receive the message
MANAGER = "MANAGER"
# team is a special case in which the "manager" of the name given by "sender"
# and all subordinates of "manager" will receive the message
TEAM = "TEAM"
# all is a special case in which all workers receive the message
ALL = "ALL"

# post worker operations
REGISTER_WORKER = "REGISTER WORKER"
DELETE_WORKER = "DELETE WORKER"

# worker operations
NOTIFY_RESIGN = "RESIGN"
NOTIFY_ADD_INTEREST = "NOTIFY ADD INTEREST"
NOTIFY_REMOVE_INTEREST = "NOTIFY REMOVE INTEREST"
