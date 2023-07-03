import asyncio
from collections.abc import Iterable
import logging
import traceback
from typing import Callable, List, Union

from .message import Message
from .constants import (
    FIRST,
    LOW,
    MANAGER,
    NOTIFY_RESIGN,
    NOTIFY_ADD_INTEREST,
    NOTIFY_REMOVE_INTEREST,
)


class AbstractWorker(object):

    def __init__(
        self,
        name: str = None,
        interests: Union[None, Iterable[str]] = None,
        empty_inbox_sleep: Union[int, float] = 5,
        max_tasks: int = 1,
        logger: logging.Logger = None,
    ):
        """
        Abstract worker class.

        Worker classes should inherit from this class and define the
        async `handle_message` method.

        Internally, it should be assumed that a manager may call a
        subordinate's methods, while other tasks should be handled with
        messages.

        Args:
            name (str, optional): Messages with the `to` slot matching `name`
                will be handled by this worker. Is filled automatically if not
                given.
            interests (Iterable[str], optional): If given, the worker's manager
                may delegate messages whose `subject` slot is contained in
                `interests`.
            max_tasks (int, optional): The maximum number of `handle_message`
                taks that this worker is allowed to spawn at a time.
            logger (logging.Logger, optional): If given, internal methods may
                log debug messages with this logger. The manager may assign a
                logger if not given.
        """
        assert name is None or isinstance(name, str)
        assert isinstance(max_tasks, int)

        self._name = name
        self._interests = list(interests) if interests else []
        self._max_tasks = max_tasks
        self._logger = logger

        # for internal use
        self._id = None
        self._manager = None
        self._interests_semaphore = asyncio.Semaphore(value=1)
        # postal worker has access to inbox and will populate it
        self._inbox_q = asyncio.PriorityQueue()
        # postal worker will notify this after populating inbox
        self._got_mail_cond = asyncio.Condition()
        # for sending mail
        self._outbox_q = None
        self._pickup_mail_cond = None
        # special handle callbacks
        self._handled_subjects = {}
        self._handled_subjects_semaphore = asyncio.Semaphore(value=1)
        # asyncio fun features
        self._loop = asyncio.get_event_loop()
        self._tasks_semaphore = asyncio.Semaphore(value=self._max_tasks)
        # future to task
        self._active_tasks = {}
        self._waiting_tasks = {}
        # main task
        self._main_task = None

        # self.s_add_handled_callback(subject, self._method)

    async def handle_message(self, message: Message) -> None:
        """
        Worker classes should define this method.

        A task for this callback is created when the worker receives a message
        with the "to" slot matching this worker.
        The task will be started when there is room in the pool of tasks.

        Args:
            message (five_one_one_workers.Message): the message to be handled.
        """
        raise NotImplementedError("Worker must define `handle_message`!")

    async def check_message(self, message: Message) -> bool:
        """
        Worker classes can optionally define this method.

        This method will be called on messages that include the worker in the
        `cc` field in opposed to the `to` field.

        The returned bool indicates whether the message needs to go to
        `handle_message`.

        This method is awaited directly in the main loop, and to avoid blocking
        execution, the programmer is encouraged to determine the result and
        then return as quickly as possible. If additional work is needed,
        additional tasks may be spawned with `s_spawn`.

        Args:
            message (five_one_one_workers.Message): the message to be handled.

        Returns:
            bool: If True, the message will be enqueued to be passed to
                `handle_message`.
                If False, no further action will be taken.
        """
        return False

    async def add_handled_callback(
        self,
        subject: str,
        method: Callable[[Message], None],
    ) -> None:
        """
        Messages with the "subject" slot matching `subject` will be
        passed to `method` instead of `check_message` or `handle_message`.

        Args:
            subject: subject associated with the callback.
            method: the callback.
        """
        await self._handled_subjects_semaphore.acquire()
        try:
            self._handled_subjects[subject] = method
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s got exception adding handled callback: %s",
                    self,
                    traceback.format_exc(),
                )
        finally:
            self._handled_subjects_semaphore.release()

    def s_add_handled_callback(
        self,
        subject: str,
        method: Callable[[Message], None],
    ) -> None:
        """
        Messages with the "subject" slot matching `subject` will be
        passed to `method` instead of `check_message` or `handle_message`.

        This is the non-asyncio version of the method and it should not be
        called after the worker has been started.

        Args:
            subject: subject associated with the callback.
            method: the callback.
        """
        try:
            self._handled_subjects[subject] = method
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s got exception adding handled callback: %s",
                    self,
                    traceback.format_exc(),
                )

    async def send_message(
        self,
        priority: int = None,
        to: Union[int, str] = None,
        cc: Union[int, str, Iterable[Union[int, str]]] = None,
        subject: str = None,
        body=None,
    ) -> None:
        """
        Callback for sending a message to another worker(s) with the name given
        by the "to" slot, a manager with the name given by the "to" slot who
        will delegate the message to a subordinate worker with an interest in
        the "subject" slot, or recipient(s) given by the "cc" slot.

        The postal worker reserves the right to modify the message (assign an
        id and normalize the "to"/"cc" slots). However the recipients will
        receive copies of the message, not a reference to the original. Note
        that if the body is mutable, references to it may still be tampered
        with on the other end.

        If one overrides this method, keep in mind the the postal worker will
        not deliver the message if the "sender" slot is not populated.

        Args:
            message (five_one_one_workers.Message): the message to be handled.
        """
        if self._outbox_q is None:
            if self._logger is not None:
                self._logger.debug(
                    "Worker %s tried to send message before outbox was set",
                    self,
                )
            return
        if not isinstance(priority, int):
            priority = LOW
        message = Message(
            priority,
            0,  # id, the post master will populate this on the other end
            to,
            cc,
            self._name if self._name is not None else self._id,
            subject,
            body,
        )
        await self._outbox_q.put(message)
        await self._pickup_mail_cond.acquire()
        self._pickup_mail_cond.notify()
        self._pickup_mail_cond.release()

    async def add_interest(self, interest: str):
        """
        [experimental after `start` has been called] add `interest` to the
        worker's interests and notify manager.
        """
        await self._interests_semaphore.acquire()
        try:
            if interest in self._interests:
                if self._logger is not None:
                    self._logger.debug(
                        "Worker %s tried to add interest %s that they are "
                        "already interested in.",
                        self,
                        interest,
                    )
                return
            self._interests.append(interest)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s got exception adding interest: %s",
                    self,
                    traceback.format_exc(),
                )
        finally:
            self._interests_semaphore.release()
        if self._manager is not None:
            await self.send_message(
                priority=FIRST,
                to=self._manager,
                subject=NOTIFY_ADD_INTEREST,
                body={"interest": interest},
            )

    async def remove_interest(self, interest: str):
        """
        [experimental after `start` has been called] remove `interest` from the
        worker's interests and notify manager.
        """
        await self._interests_semaphore.acquire()
        try:
            if interest not in self._interests:
                if self._logger is not None:
                    self._logger.debug(
                        "Worker %s tried to remove interest %s that they are "
                        "not interested in.",
                        self,
                        interest,
                    )
                return
            self._interests.remove(interest)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s got exception removing interest: %s",
                    self,
                    traceback.format_exc(),
                )
        finally:
            self._interests_semaphore.release()
        if self._manager is not None:
            await self.send_message(
                priority=FIRST,
                to=self._manager,
                subject=NOTIFY_REMOVE_INTEREST,
                body={"interest": interest},
            )

    async def resign(self) -> None:
        """
        Cancels the worker's main task and sends a message to the manager
        notifying of the retirement.
        """
        await self.send_message(
            priority=FIRST,
            to=self._manager if self._manager is not None else MANAGER,
            subject=NOTIFY_RESIGN,
        )

        if (self._main_task is not None
                and not self._main_task.done()
                and not self._main_task.cancelled()):
            self._main_task.cancel()
        self._inbox_q = None
        self._got_mail_cond = None

    def is_to_self(self, message: Message) -> bool:
        """
        Convenience method that determines if the message's "to" slot matches
        the worker's name or id.

        Args:
            message: "to" slot of this message will be examined

        Returns:
            bool: indicates if there is a match
        """
        if isinstance(message.to, str) and message.to == self._name:
            return True
        if isinstance(message.to, int) and message.to == self._id:
            return True
        return False

    def is_cc_self(self, message: Message) -> bool:
        """
        Convenience method that determines if the message's "cc" slot matches
        the worker's name or id.

        Args:
            message: "cc" slot of this message will be examined

        Returns:
            bool: indicates if there is a match
        """
        if message.cc is None:
            return False
        if self._name in message.cc:
            return True
        return False

    def s_spawn(self, coro) -> asyncio.Future:
        """
        Convenience method for spawning a task which will wait for room in the
        tasks pool and then await `coro`.

        Args:
            coro: awaitable to be spawned when there is room in the tasks pool

        Returns:
            asyncio.Future: will be set to `done` with either a result or an
                exception when `coro` is complete.
        """
        fut = self._loop.create_future()
        self._waiting_tasks[fut] = self._loop.create_task(
            self._spawn(fut, coro)
        )
        return fut

    def start(self) -> None:
        """
        Convenience method for starting the worker.
        """
        if self._main_task is None:
            self._main_task = self._loop.create_task(self._main_loop())
        return self._main_task

    async def _main_loop(self) -> None:
        """
        Executes the main loop of the worker, for internal use.
        """
        while True:

            try:
                # wait for mail
                if self._inbox_q.empty():
                    await self._got_mail_cond.acquire()
                    await self._got_mail_cond.wait()
                    self._got_mail_cond.release()

                # sanity checks
                message = await self._inbox_q.get()
                if not isinstance(message, Message):
                    if self._logger is not None:
                        self._logger.debug(
                            "%s got a message that is not a Message: %s",
                            self,
                            message,
                        )
                    continue

                # determine if there is a special handler for the subject
                special_handler = self._handled_subjects.get(
                    message.subject,
                    None,
                )
                if special_handler is not None:
                    await special_handler(message)
                    continue

                # determine if the message should be checked and/or handled
                if self.is_cc_self(message):
                    is_handled = await self.check_message(message)
                else:
                    is_handled = True

                # handle the message
                if not is_handled:
                    continue

                self.s_spawn(self.handle_message(message))

            except Exception:
                self._logger.debug(
                    "%s encountered exception in main loop: %s",
                    self,
                    traceback.format_exc(),
                )

    async def _spawn(
        self,
        fut: asyncio.Future,
        coro,
    ) -> bool:
        """
        Waits for room in the worker's tasks pool and then creates a task for
        `coro`. Expects that `fut` is already in `self._waiting_tasks`.

        Args:
            fut: result will be set when `coro` completes.
            coro: this coro will be awaited.

        Returns:
            bool: whether the operation was successful.
        """
        await self._tasks_semaphore.acquire()
        try:
            this_task = self._waiting_tasks[fut]
            del self._waiting_tasks[fut]
            self._active_tasks[fut] = this_task
            res = await coro
            fut.set_result(res)
        except Exception as e:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered an error in spawned coro: %s",
                    self,
                    traceback.format_exc(),
                )
            fut.set_exception(e)
        finally:
            self._tasks_semaphore.release()
            try:
                del self._active_tasks[fut]
            except KeyError:
                self._logger.debug(
                    "%s was not able process current tasks.",
                    self,
                )

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def interests(self):
        return list(self._interests)

    @property
    def max_tasks(self):
        return self._max_tasks

    @property
    def inbox(self):
        return self._inbox_q

    @property
    def got_mail(self):
        return self._got_mail_cond

    @property
    def outbox(self):
        return self._outbox_q

    @property
    def pickup_mail_flag(self):
        return self._pickup_mail_cond

    @property
    def manager(self):
        return self._manager

    @property
    def logger(self):
        return self._logger

    async def get_interests(self) -> List[str]:
        """
        Asyncio alternative to the `interests` property.

        This method acquires the interests semaphore which will wait if the
        interests are in the process of being updated.

        Returns:
            List[str]: interests
        """
        await self._interests_semaphore.acquire()
        try:
            res = list(self._interests)
        except Exception:
            if self._logger is not None:
                self._logger.debug(
                    "%s encountered exception in `get_interests`: %s",
                    self,
                    traceback.format_exc(),
                )
            return []
        finally:
            self._interests_semaphore.release()
        return res

    def get_backlog_size(self):
        """
        Returns the approximate number of backlog messages, plus the number
        of tasks in progress.
        """
        return self._inbox_q.qsize() + len(self._active_tasks)

    def set_id(self, id: int) -> None:
        """
        Sets the worker id, for internal use.
        """
        if self._id is None:
            self._id = id

    def set_name(self, name: str) -> None:
        """
        Sets the worker name, for internal use.
        """
        if self._name is None:
            self._name = name

    def set_outbox(self, outbox_q: asyncio.Queue) -> None:
        """
        Sets the outbox, for internal use.
        """
        if self._outbox_q is None:
            self._outbox_q = outbox_q

    def set_pickup_mail_flag(
        self,
        pickup_mail_cond: asyncio.Condition,
    ) -> None:
        """
        Sets the pickup mail flag condition, for internal use.
        """
        if self._pickup_mail_cond is None:
            self._pickup_mail_cond = pickup_mail_cond

    def set_manager(self, manager_name: str) -> None:
        """
        Sets the manager, for internal use.
        """
        if self._manager is None:
            self._manager = manager_name

    def set_logger(self, logger) -> None:
        """
        Sets the logger, for internal use.
        """
        self._logger = logger

    def asdict(self) -> dict:
        """
        Converts the important object attributes to a dict.
        """
        return {
            "id": self._id,
            "class": self.__class__.__name__,
            "name": self._name,
            "interests": tuple(self._interests),
            "manager": self._manager,
            "inbox": self._inbox_q,
            "got_mail": self._got_mail_cond,
        }

    def __str__(self):
        return f"<{self.__class__.__name__} id={self._id} name={self._name}>"

    def __repr__(self):
        return str(self)
