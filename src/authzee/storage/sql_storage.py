""""""

__all__ = [
    "SQLStorage"
]

import datetime
import json
from typing import Any, Literal
from uuid import UUID

from sqlalchemy.types import JSON
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sqlalchemy import delete, event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine

from authzee.storage.storage_module import StorageModule
from authzee.exceptions import StorageError
from authzee.module_locality import ModuleLocality
from authzee.types.authzee import *
from authzee.types.config import (
    CleanupLatchesConfig,
    CreateLatchConfig,
    DeleteContextDefConfig,
    DeleteIdentityDefConfig,
    DeleteLatchConfig,
    DeleteResourceDefConfig,
    EnactConfig,
    GetContextDefConfig,
    GetGrantConfig,
    GetIdentityDefConfig,
    GetLatchConfig,
    GetResourceDefConfig,
    ListContextDefsConfig,
    ListGrantRefsConfig,
    ListGrantsConfig,
    ListIdentityDefsConfig,
    ListResourceDefsConfig,
    PutContextDefConfig,
    PutIdentityDefConfig,
    PutResourceDefConfig,
    RepealConfig,
    SetLatchConfig,
    StorageConstructConfig,
    StorageDestroyConfig,
    StorageShutdownConfig,
    StorageStartConfig
)



class Base(AsyncAttrs, DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSON,
        dict[str, str]: JSON,
        Any: JSON
    }


class ContextDefDB(Base):
    __tablename__ = "context_defs"

    context_type: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    schema: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class IdentityDefDB(Base):
    __tablename__ = "identity_defs"

    identity_type: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    schema: Mapped[dict[str, Any]]


class ResourceDefDB(Base):
    __tablename__ = "resource_defs"

    resource_type: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    actions: Mapped[list[str]] = mapped_column(nullable=False)
    schema: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class GrantDB(Base):
    __tablename__ = "grants"

    grant_uuid: Mapped[UUID] = mapped_column(primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str] = mapped_column(nullable=False)
    tags: Mapped[dict[str, str]] = mapped_column(nullable=False)
    effect: Mapped[Literal["allow", "deny"]] = mapped_column(nullable=False)
    actions: Mapped[list[str]] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    equality: Mapped[Any] = mapped_column(nullable=True)
    applicable_on_failure: Mapped[bool] = mapped_column(nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class StorageLatchDB(Base):
    __tablename__ = "storage_latches"

    storage_latch_uuid: Mapped[UUID] = mapped_column(primary_key=True, nullable=False)
    is_set: Mapped[bool] = mapped_column(nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(nullable=False)



class SQLStorage(StorageModule):
    """Storage Module using SQL.

    For best performance, use UUID7 for all UUID fields.

    Parameters
    ----------
    sqlalchemy_async_engine_kwargs : dict[str, Any]
        SQLAlchemy Async Engine keyword args. 
        https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#sqlalchemy.ext.asyncio.create_async_engine
    """


    def __init__(
        self,
        *,
        sqlalchemy_async_engine_kwargs: dict[str, Any]
    ):
        self._sqlalchemy_async_engine_kwargs = sqlalchemy_async_engine_kwargs
        self.has_parallel_paging = True
        self.locality = ModuleLocality.NETWORK
        url = sqlalchemy_async_engine_kwargs['url']
        if url.endswith("://:memory:") is True:
            self.locality = ModuleLocality.PROCESS
        
        if (
            url.startswith("sqlite") is True
            or "://localhost" in url
            or "://127.0.0.1" in url
        ):
            self.locality = ModuleLocality.SYSTEM
    

    async def start(self, config: StorageStartConfig) -> GenericResult:
        self._engine = create_async_engine(**self._sqlalchemy_async_engine_kwargs)
        self._async_sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self._engine, 
            expire_on_commit=False
        )

        return {
            "error": None
        }

    async def shutdown(self, config: StorageShutdownConfig) -> GenericResult:
        await self._engine.dispose()

        return {
            "error": None
        }
    

    async def construct(self, config: StorageConstructConfig) -> GenericResult:
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)   

        return {
            "error": None
        }


    async def destroy(self, config: StorageDestroyConfig) -> GenericResult:
        return {
            "error": None
        }


    async def list_context_defs(
        self,
        page_ref: str | None,
        config: ListContextDefsConfig
    ) -> ContextDefsPage:
        


    async def add_grant(self, effect: GrantEffect, grant: Grant) -> Grant:
        """Add a grant. 

        Parameters
        ----------
        effect : GrantEffect
            The effect of the grant.
        grant : Grant
            The grant.

        Returns
        -------
        Grant
            The grant that has been added with additional information for the specific backend.
        """
        grant = self._check_uuid(grant=grant, generate_uuid=True)
        async with self._async_sessionmaker() as session:
            resource_action_strs = {str(action) for action in grant.actions}
            result = await session.execute(
                select(ResourceActionDB).where(
                    ResourceActionDB.action.in_(resource_action_strs)
                )
            )
            re_actions = set(result.scalars().fetchall())
            grant_kwargs = {
                "uuid": grant.uuid,
                "name": grant.name,
                "description": grant.description,
                "resource_type": grant.resource_type.__name__,
                "actions": re_actions,
                "expression": grant.expression,
                "context": grant.context,
                "equality": grant.equality
            }
            if effect is GrantEffect.ALLOW:
                db_grant = AllowGrantDB(**grant_kwargs)
            else:
                db_grant = DenyGrantDB(**grant_kwargs)

            session.add(db_grant)
            await session.commit()
            grant.storage_id = db_grant.storage_id
        
        return grant


    async def delete_grant(self, effect: GrantEffect, uuid: str) -> None:
        """Delete a grant.

        Parameters
        ----------
        effect : GrantEffect
            The effect of the grant.
        uuid : str
            UUID of grant to delete.
        """
        async with self._async_sessionmaker() as session:
            if effect is GrantEffect.ALLOW:
                grant_table = AllowGrantDB
            else:
                grant_table = DenyGrantDB
            
            result = await session.execute(
                select(grant_table).where(grant_table.uuid == uuid)
            )
            db_grant = result.scalars().unique().one_or_none()
            if db_grant is None:
                raise exceptions.GrantDoesNotExistError(
                    f"{effect.value} Grant with UUID: '{uuid}' does not exist."
                )

            await session.delete(db_grant)
            await session.commit()
    

    async def get_raw_grants_page(
        self,
        effect: GrantEffect,
        resource_type: Optional[Type[BaseModel]] = None,
        action: Optional[ResourceAction] = None,
        page_size: Optional[int] = None,
        page_ref: Optional[str] = None
    ) -> RawGrantsPage:
        """Retrieve a page of raw grants matching the filters.

        If ``RawGrantsPage.next_page_ref`` is not ``None`` , there are more grants to retrieve.
        To get the next page, pass ``page_ref=RawGrantsPage.next_page_ref`` .

        Use ``normalize_raw_grants_page`` to convert the ``RawGrantsPage`` to a ``GrantsPage`` model.

        **NOTE** - There is no guarantee of how many grants will be returned if any.

        Parameters
        ----------
        effect : GrantEffect
            The effect of the grant.
        resource_type : Optional[Type[BaseModel]], optional
            Filter by resource type.
            By default no filter is applied.
        action : Optional[ResourceAction], optional
            Filter by `ResourceAction``. 
            By default no filter is applied.
        page_size : Optional[int], optional
            The suggested page size to return. 
            There is no guarantee of how much data will be returned if any.
            The default is set on the storage backend. 
        page_ref : Optional[str], optional
            The reference to the next page that is returned in ``RawGrantsPage``, 
            or one of the page references from ``StorageBackend.get_page_ref_page()`` (if parallel pagination is supported.) .
            By default this will return the first page.

        Returns
        -------
        RawGrantsPage
            The page of raw grants.
        """
        page_size = self._real_page_size(page_size=page_size)
        async with self._async_sessionmaker() as session:
            if effect is GrantEffect.ALLOW:
                grant_table = AllowGrantDB
            else:
                grant_table = DenyGrantDB

            query = select(grant_table)
            filters = []
            if resource_type is not None:
                filters.append(
                    grant_table.resource_type == resource_type.__name__
                )
            
            if action is not None:
                filters.append(
                    grant_table.actions.any(
                        ResourceActionDB.action == str(action)
                    )
                )

            if page_ref is not None:
                sql_next_page = SQLNextPageRef(**json.loads(page_ref))
                filters.append(
                    grant_table.storage_id > sql_next_page.next_token
                )
            
            query = query.where(*filters)
            query = query.limit(page_size)

            result = await session.execute(query)
            db_grants = result.scalars().unique().all()
            next_page_ref = None
            if len(db_grants) >= page_size:
                next_page_ref = SQLNextPageRef(next_token=db_grants[-1].storage_id).model_dump_json()

        return RawGrantsPage(
            raw_grants=db_grants,
            next_page_ref=next_page_ref
        )


    async def normalize_raw_grants_page(
        self,
        raw_grants_page: RawGrantsPage
    ) -> GrantsPage:
        """Convert a ``RawGrantsPage`` to a ``GrantsPage``.

        Parameters
        ----------
        raw_grants_page : RawGrantsPage
            Raw grants page to convert.

        Returns
        -------
        GrantsPage
            Normalized grants page.
        """
        grants = []
        db_grants: list[Union[AllowGrantDB, DenyGrantDB]] = raw_grants_page.raw_grants
        for db_grant in db_grants:
            grants.append(
                Grant(
                    name=db_grant.name,
                    description=db_grant.description,
                    resource_type=self._resource_type_lookup[db_grant.resource_type],
                    actions={
                        self._resource_action_lookup[action.action] for action in db_grant.actions
                    },
                    expression=db_grant.expression,
                    context=db_grant.context,
                    equality=db_grant.equality,
                    storage_id=str(db_grant.storage_id),
                    uuid=db_grant.uuid
                )
            )

        return GrantsPage(
            grants=grants,
            next_page_ref=raw_grants_page.next_page_ref
        )


    async def create_flag(self) -> StorageFlag:
        """Create a new shared flag in the storage backend.

        Returns
        -------
        StorageFlag
            New storage flag. 
        """
        new_flag = StorageFlag()
        async with self._async_sessionmaker() as session:
            db_flag = StorageFlagDB(**new_flag.model_dump())
            session.add(db_flag)
            await session.commit()

        return new_flag


    async def get_flag(self, uuid: str) -> StorageFlag:
        """Retrieve flag by UUID.

        Parameters
        ----------
        uuid : str
            Storage flag UUID.

        Returns
        -------
        StorageFlag
            The storage flag with the given UUID.
        
        Raises
        ------
        authzee.exceptions.StorageFlagNotFoundError
            The storage flag with the given UUID was not found.
        """
        async with self._async_sessionmaker() as session:
            query = select(StorageFlagDB).where(StorageFlagDB.uuid == uuid)
            result = await session.execute(query)
            db_flag = result.scalars().unique().one_or_none()
            if  db_flag is None:
                raise exceptions.StorageFlagNotFoundError(
                    f"The storage flag with UUID '{uuid}' was not found!"
                )
        
            await session.commit()
    
        return StorageFlag.model_validate(db_flag, from_attributes=True)


    async def set_flag(self, uuid: str) -> StorageFlag:
        """set a flag for a given UUID. 

        Parameters
        ----------
        uuid : str
            Storage flag UUID.

        Returns
        -------
        StorageFlag
            The storage flag with the given UUID and the flag set.
        
        Raises
        ------
        authzee.exceptions.StorageFlagNotFoundError
            The storage flag with the given UUID was not found.
        """
        async with self._async_sessionmaker() as session:
            query = select(StorageFlagDB).where(StorageFlagDB.uuid == uuid)
            result = await session.execute(query)
            db_flag = result.scalars().unique().one_or_none()
            if  db_flag is None:
                raise exceptions.StorageFlagNotFoundError(
                    f"The storage flag with UUID '{uuid}' was not found!"
                )
        
            db_flag.is_set = True
            await session.commit()
    
        return StorageFlag.model_validate(db_flag, from_attributes=True)


    async def delete_flag(self, uuid: str) -> None:
        """Delete a storage flag by UUID.

        Parameters
        ----------
        uuid : str
            Storage flag UUID.
        """
        async with self._async_sessionmaker() as session:
            await session.execute(
                delete(StorageFlagDB).where(StorageFlagDB.uuid == uuid)
            )
            await session.commit()


    async def cleanup_flags(self, earlier_than: datetime.datetime) -> None:
        """Delete zombie storage flags from before a certain point in time.

        Parameters
        ----------
        earlier_than : datetime.datetime
            Delete flags created earlier than this date. 
            Naive datetimes are assumed to be UTC. 
        """
        async with self._async_sessionmaker() as session:
            await session.execute(
                delete(StorageFlagDB).where(StorageFlagDB.created_at < earlier_than)
            )
            await session.commit()