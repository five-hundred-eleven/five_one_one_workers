import asyncio
from collections.abc import Iterable
import logging
import traceback
from typing import List, Union

from .base import AbstractWorker
from .constants import (
    ALL,
    MANAGER,
    POSTMASTER,
    TEAM,
    REGISTER_WORKER,
    DELETE_WORKER,
)
from .message import Message
from .priority_bucket_queue import PriorityBucketQueue

_POSTAL_WORKER = None


class PostalWorker(AbstractWorker):

    def __init__(self, max_tasks=4, **kwargs):
        """
        Postal worker is a special case, where the postal worker's inbox_q is
        the outbox_q of all other workers, and the postal worker has access to
        all other worker's inbox_q so that messages may be delivered.

        Programmers should not use custom postal workers unless they are
        familiar with the code.
        """

        super().__init__(name=POSTMASTER, max_tasks=max_tasks, **kwargs)

        # since `message.id` will not be populated in the `_inbox_q`, we cannot
        # use the standard PriorityQueue.
        self._inbox_q = PriorityBucketQueue()
        # for standard `send_message`
        self._outbox_q = self._inbox_q

        # map of worker ids to the worker's inbox q
        self._worker_id_to_q = {}
        # map of worker ids to the worker's got mail notifier
        self._worker_id_to_cond = {}
        # map of worker name to id
        self._worker_name_to_id = {}
        # map of worker id to name
        self._worker_id_to_name = {}

        # map of worker ids to the worker id of the worker's manager
        self._worker_id_to_manager_id = {}
        # map of manger id to a collection of the worker ids of subordinates
        self._manager_id_to_worker_ids = {}

        # for assigning message ids
        self._message_counter = 0

        # for modifying the postal worker
        self._post_semaphore = asyncio.Semaphore(value=1)

        # set our handled subjects
        self.s_add_handled_callback(
            REGISTER_WORKER,
            self._handle_register_worker_callback,
        )
        self.s_add_handled_callback(
            DELETE_WORKER,
            self._handle_delete_worker_callback,
        )

    @staticmethod
    def get_postal_worker(logger: logging.Logger = None):
        """
        This method should always be used to get a reference to the postal
        worker.
        """
        global _POSTAL_WORKER
        if _POSTAL_WORKER is None:
            _POSTAL_WORKER = PostalWorker(logger=logger)
        return _POSTAL_WORKER

    async def register_worker(self, worker) -> bool:
        """
        Registers the worker.

        Args:
            worker: the worker to be registered.

        Returns:
            bool: whether the oporation was successful.
        """
        await self._post_semaphore.acquire()
        res = self.s_register_worker(worker)
        self._post_semaphore.release()
        return res

    def s_register_worker(self, worker) -> bool:
        """
        Registers the worker.

        This is the non-asyncio version of the method and it should not be
        called directly after the worker has been started.

        Args:
            worker: the worker to be registered.

        Returns:
            bool: whether the oporation was successful.
        """
        if worker.name and worker.name in self._worker_name_to_id:
            return True
        try:
            if worker.manager is not None:
                manager_id = self._lookup(worker.manager)[0]
                self._worker_id_to_manager_id[worker.id] = manager_id
                if manager_id not in self._manager_id_to_worker_ids:
                    self._manager_id_to_worker_ids[manager_id] = []
                self._manager_id_to_worker_ids[manager_id].append(worker.id)
            self._worker_id_to_name[worker.id] = worker.name
            self._worker_name_to_id[worker.name] = worker.id
            self._worker_id_to_q[worker.id] = worker.inbox
            self._worker_id_to_cond[worker.id] = worker.got_mail
            worker.set_outbox(self._inbox_q)
            worker.set_pickup_mail_flag(self._got_mail_cond)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered exception registering worker: %s",
                    self,
                    traceback.format_exc(),
                )
            return False
        return True

    async def handle_message(self, message: Message) -> None:
        """
        Sorts the message.

        Programmers should not use custom postal workers unless they are
        familiar with the code.
        """

        # first part is validating the message
        if message.sender is None:
            if self._logger is not None:
                self._logger.debug(
                    "%s got message with empty sender and it will not be "
                    "sent.",
                    self,
                )
            return

        message_subject = message.subject
        if message_subject is None:
            message_subject = ""

        if not isinstance(message.subject, str):
            if self._logger is not None:
                self._logger.debug(
                    "%s got message with invalid subject and it will "
                    "not be sent.",
                    self,
                )
            return

        # de-duplicate recipients
        dedup = set()
        # keep recipient ids in a rough order
        recipient_ids = []

        # process the message.to slot
        to_id = None
        to_ids = self._lookup(message.to, sender=message.sender)
        if len(to_ids) == 1:
            to_id = to_ids[0]
            recipient_ids.append(to_id)
        elif self._logger is not None:
            self._logger.debug(
                "%s got invalid \"to\" slot in message: %s",
                self,
                message.to,
            )

        # for convenience to the recipient, we populate the "sender" slot
        # with the name
        message_to = self._worker_id_to_name.get(to_id, None)

        # process the message.cc slot
        cc_names = []
        if isinstance(message.cc, (int, str)):
            for cc_id in self._lookup(message.cc, sender=message.sender):
                if cc_id not in dedup:
                    dedup.add(cc_id)
                    cc_names.append(self._worker_id_to_name[cc_id])
                    recipient_ids.append(cc_id)
        elif isinstance(message.cc, Iterable):
            for item in message.cc:
                for cc_id in self._lookup(item, sender=message.sender):
                    if cc_id not in dedup:
                        dedup.add(cc_id)
                        cc_names.append(self._worker_id_to_name[cc_id])
                        recipient_ids.append(cc_id)

        # for conveniences to the recipients, we normalize the "cc" slot
        message_cc = tuple(cc_names)

        for recipient_id in recipient_ids:
            q = self._worker_id_to_q.get(recipient_id, None)
            cond = self._worker_id_to_cond.get(recipient_id, None)
            if q is None or cond is None:
                if self._logger is not None:
                    self._logger.debug(
                        "%s got a worker id that does not have a q/cond",
                        self,
                    )
                continue
            await self._post_semaphore.acquire()
            message_id = self._message_counter
            self._message_counter += 1
            self._post_semaphore.release()
            message_copy = Message(
                message.priority,
                message_id,
                message_to,
                message_cc,
                message.sender,
                message_subject,
                message.body,
            )
            await q.put(message_copy)
            await cond.acquire()
            cond.notify()
            cond.release()

    def _lookup(self, name: Union[int, str], sender: str = None) -> List[int]:
        """
        Convenience method for converting a message's "to" slot to a numeric
        id or list of ids.

        Args:
            name (str): recipient.
            sender (str, optional): id of the sender, for use in special cases.

        Returns:
            List[int]: results of the lookup.
        """
        if isinstance(name, int):
            if name in self._worker_id_to_name:
                return [name]
            return []
        if not isinstance(name, str):
            return []
        res_id = self._worker_name_to_id.get(name, None)
        if res_id is not None:
            return [res_id,]
        # special macros
        if name == ALL:
            return list(self._worker_id_to_q.keys())
        if name == MANAGER:
            sender_ids = self._lookup(sender)
            if len(sender_ids) != 1:
                if self._logger is not None:
                    self._logger.debug(
                        "%s got a message with an unknown sender",
                        self,
                    )
                return []
            res_id = self._worker_id_to_manager_id.get(sender_ids[0], None)
            if res_id is None:
                if self._logger is not None:
                    self._logger.debug(
                        "%s got a message addressed to an unknown manager.",
                        self,
                    )
                return []
            return [res_id,]
        if name == TEAM:
            sender_ids = self._lookup(sender)
            if len(sender_ids) != 1:
                if self._logger is not None:
                    self._logger.debug(
                        "%s got a message with an unknown sender",
                        self,
                    )
                return []
            man_id = self._worker_id_to_manager_id.get(sender_ids[0], None)
            if man_id is None:
                if self._logger is not None:
                    self._logger.debug(
                        "%s got a message addressed to an unknown team.",
                        self,
                    )
                return []
            res_ids = self._manager_id_to_worker_ids.get(man_id, None)
            if res_ids is None:
                if self._logger is not None:
                    self._logger.debug(
                        "%s got a message addressed to an unknown team.",
                        self,
                    )
                return []
            return list(res_ids)
        if self._logger is not None:
            self._logger.debug(
                "%s got a message with an unrecognized name in the "
                "\"to\" slot: %s",
                self,
                name,
            )
        return []

    async def _handle_register_worker_callback(self, message: Message) -> None:
        """
        Convenience method for handling messages addressed to the postmaster.

        Args:
            message: message to be handled
        """
        await self._post_semaphore.acquire()
        try:
            worker = message.body["worker"]
            manager_id = None
            if worker.manager is not None:
                manager_id = self._lookup(
                    worker.manager,
                    sender=message.sender,
                )[0]
            self._worker_id_to_name[worker.id] = worker.name
            self._worker_name_to_id[worker.name] = worker.id
            self._worker_id_to_manager_id[worker.id] = manager_id
            self._worker_id_to_q[worker.id] = worker.inbox
            self._worker_id_to_cond[worker.id] = worker.got_mail

        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered an error interpreting a postmaster "
                    "request: %s",
                    self,
                    message,
                )
            return

        finally:
            self._post_semaphore.release()

    async def _handle_delete_worker_callback(self, message: Message) -> None:
        """
        Convenience method for handling messages addressed to the postmaster.

        Args:
            message: message to be handled
        """
        await self._post_semaphore.acquire()
        try:
            name = message.body["name"]
            worker_id = self._lookup(name, sender=message.sender)[0]
            del self._worker_id_to_name[worker_id]
            del self._worker_name_to_id[name]
            del self._worker_id_to_manager_id[worker_id]
            del self._worker_id_to_q[worker_id]
            del self._worker_id_to_cond[worker_id]

            return

        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered an error interpreting a postmaster "
                    "request: %s",
                    self,
                    message,
                )
            return

        finally:
            self._post_semaphore.release()
