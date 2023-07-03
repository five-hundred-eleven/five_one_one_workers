import collections

Message = collections.namedtuple(
    "Message",
    (
        # If messages become backed up, messages with a low value in the
        # "priority" slot are handled first.
        "priority",
        # the message id is assigned by the `send_message` method or the
        # postal worker and is for internal use.
        "id",
        # The "to" slot is the name (str) or the id (int) of the recipient.
        # If the recipient is a manager, they may call `delegate` and forward
        # to a subordinate.
        # Otherwise, the recipient's `handle_message` will be called on the
        # message when they have room for it in their task pool.
        # Use this for requests for jobs or changes to the worker.
        "to",
        # The "cc" slot can be `None`, a single name (str)/id (int), or an
        # iterable of names and/or ids. Also supports macros such as `ALL`,
        # `TEAM`, or `MANAGER`.
        # The "cc" recipients' `check_message` will be called on the message
        # when they have room for it in their task pool.
        # Use this for requests for information.
        # The postmaster will ensure a message is delivered at most once to
        # duplicated "cc" recipients.
        "cc",
        # The "sender" slot is automatically filled in with the name of the
        # sender, or the id if the name is None.
        "sender",
        # if more than one worker could handle the message based on the "to"
        # slot, or if the "to" slot is None, workers whose `interests` include
        # the "subject" will be taken into consideration
        "subject",
        # the "body" slot is for use by the programmer
        "body",
    ),
)
