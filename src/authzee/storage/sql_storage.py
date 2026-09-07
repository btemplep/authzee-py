""""""

__all__ = [
    "SQLStorage"
]

import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import DateTime, delete, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

from authzee.exceptions import StorageError
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule
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
        list[str]: JSON,
        Any: JSON
    }


class ContextDefDB(Base):
    __tablename__ = "context_defs"
    internal_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    context_type: Mapped[str] = mapped_column(unique=True, nullable=False)
    schema: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class IdentityDefDB(Base):
    __tablename__ = "identity_defs"
    internal_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    identity_type: Mapped[str] = mapped_column(unique=True, nullable=False)
    schema: Mapped[dict[str, Any]]


class ResourceDefDB(Base):
    __tablename__ = "resource_defs"
    internal_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    resource_type: Mapped[str] = mapped_column(unique=True, nullable=False)
    actions: Mapped[list[str]] = mapped_column(nullable=False)
    schema: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class GrantDB(Base):
    __tablename__ = "grants"
    internal_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    grant_uuid: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str] = mapped_column(nullable=False)
    tags: Mapped[dict[str, str]] = mapped_column(nullable=False)
    effect: Mapped[Literal["allow", "deny"]] = mapped_column(nullable=False)
    actions: Mapped[list[str]] = mapped_column(nullable=False)
    query: Mapped[str] = mapped_column(nullable=False)
    equality: Mapped[Any] = mapped_column(nullable=True)
    applicable_on_failure: Mapped[bool] = mapped_column(nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class StorageLatchDB(Base):
    __tablename__ = "storage_latches"
    internal_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    storage_latch_uuid: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    is_set: Mapped[bool] = mapped_column(nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SQLStorage(StorageModule):
    """Storage Module using SQL.

    `get_*` calls do not honor configs to use list or cache.


    Parameters
    ----------
    sqlalchemy_async_engine_kwargs : dict[str, Any]
        SQLAlchemy Async Engine keyword args.
        https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#sqlalchemy.ext.asyncio.create_async_engine
    """


    def __init__(self, *, sqlalchemy_async_engine_kwargs: dict[str, Any]):
        self._sqlalchemy_async_engine_kwargs = sqlalchemy_async_engine_kwargs
        self.has_parallel_paging = True
        self.locality = ModuleLocality.NETWORK
        url: str = sqlalchemy_async_engine_kwargs['url']
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
        self.locality = ModuleLocality.NETWORK
        self.has_parallel_paging = False

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
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.reflect)
            await conn.run_sync(Base.metadata.drop_all)

        return {
            "error": None
        }


    async def list_context_defs(
        self,
        page_ref: str | None,
        config: ListContextDefsConfig
    ) -> ContextDefsPage:
        async with self._async_sessionmaker() as db_sess:
            query = select(
                ContextDefDB
            ).limit(
                config['page_size']
            ).order_by(
                ContextDefDB.internal_id
            )
            if page_ref is not None:
                query = query.where(ContextDefDB.internal_id > int(page_ref))

            db_cds: list[ContextDefDB] = (await db_sess.execute(query)).scalars().all()

        next_page_ref = None
        if len(db_cds) == config['page_size']:
            next_page_ref = str(db_cds[-1].internal_id)

        result: ContextDefsPage = {
            "context_defs": [],
            "next_page_ref": next_page_ref,
            "error": None
        }
        for cd in db_cds:
            result['context_defs'].append(
                {
                    "context_type": cd.context_type,
                    "schema": cd.schema
                }
            )

        return result


    async def get_context_def(
        self,
        context_type: str,
        config: GetContextDefConfig
    ) -> ContextDefResult:
        async with self._async_sessionmaker() as db_sess:
            db_cd: ContextDefDB | None = (
                await db_sess.execute(
                    select(ContextDefDB).where(ContextDefDB.context_type == context_type)
                )
            ).scalar_one_or_none()

        if db_cd is not None:
            return {
                "context_def": {
                    "context_type": db_cd.context_type,
                    "schema": db_cd.schema
                },
                "error": None
            }

        return {
            "context_def": None,
            "error": {
                "error_type": "resource_not_found",
                "message": f"Context type '{context_type}' was not found."
            }
        }


    async def put_context_def(
        self,
        context_def: ContextDef,
        config: PutContextDefConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            db_cd: ContextDefDB | None = (
                await db_sess.execute(
                    select(
                        ContextDefDB
                    ).where(
                        ContextDefDB.context_type == context_def['context_type']
                    )
                )
            ).scalar_one_or_none()
            if db_cd is None:
                db_sess.add(
                    ContextDefDB(
                        context_type=context_def['context_type'],
                        schema=context_def['schema']
                    )
                )
            else:
                db_cd.schema = context_def['schema']

            await db_sess.commit()

        return {
            "error": None
        }


    async def delete_context_def(
        self,
        context_type: str,
        config: DeleteContextDefConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            await db_sess.execute(
                delete(ContextDefDB).where(ContextDefDB.context_type == context_type)
            )
            await db_sess.commit()

        return {
            "error": None
        }


    async def list_identity_defs(
        self,
        page_ref: str | None,
        config: ListIdentityDefsConfig
    ) -> IdentityDefsPage:
        async with self._async_sessionmaker() as db_sess:
            query = select(
                IdentityDefDB
            ).limit(
                config['page_size']
            ).order_by(
                IdentityDefDB.internal_id
            )
            if page_ref is not None:
                query = query.where(IdentityDefDB.internal_id > int(page_ref))

            db_ids: list[IdentityDefDB] = (await db_sess.execute(query)).scalars().all()

        next_page_ref = None
        if len(db_ids) == config['page_size']:
            next_page_ref = str(db_ids[-1].internal_id)

        result: IdentityDefsPage = {
            "identity_defs": [],
            "next_page_ref": next_page_ref,
            "error": None
        }
        for id_def in db_ids:
            result['identity_defs'].append(
                {
                    "identity_type": id_def.identity_type,
                    "schema": id_def.schema
                }
            )

        return result


    async def get_identity_def(
        self,
        identity_type: str,
        config: GetIdentityDefConfig
    ) -> IdentityDefResult:
        async with self._async_sessionmaker() as db_sess:
            db_id: IdentityDefDB | None = (
                await db_sess.execute(
                    select(IdentityDefDB).where(IdentityDefDB.identity_type == identity_type)
                )
            ).scalar_one_or_none()

        if db_id is not None:
            return {
                "identity_def": {
                    "identity_type": db_id.identity_type,
                    "schema": db_id.schema
                },
                "error": None
            }

        return {
            "identity_def": None,
            "error": {
                "error_type": "resource_not_found",
                "message": f"Identity type '{identity_type}' was not found."
            }
        }


    async def put_identity_def(
        self,
        identity_def: IdentityDef,
        config: PutIdentityDefConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            db_id: IdentityDefDB | None = (
                await db_sess.execute(
                    select(
                        IdentityDefDB
                    ).where(
                        IdentityDefDB.identity_type == identity_def['identity_type']
                    )
                )
            ).scalar_one_or_none()
            if db_id is None:
                db_sess.add(
                    IdentityDefDB(
                        identity_type=identity_def['identity_type'],
                        schema=identity_def['schema']
                    )
                )
            else:
                db_id.schema = identity_def['schema']

            await db_sess.commit()

        return {
            "error": None
        }


    async def delete_identity_def(
        self,
        identity_type: str,
        config: DeleteIdentityDefConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            await db_sess.execute(
                delete(IdentityDefDB).where(IdentityDefDB.identity_type == identity_type)
            )
            await db_sess.commit()

        return {
            "error": None
        }


    async def list_resource_defs(
        self,
        page_ref: str | None,
        config: ListResourceDefsConfig
    ) -> ResourceDefsPage:
        async with self._async_sessionmaker() as db_sess:
            query = select(
                ResourceDefDB
            ).limit(
                config['page_size']
            ).order_by(
                ResourceDefDB.internal_id
            )
            if page_ref is not None:
                query = query.where(ResourceDefDB.internal_id > int(page_ref))

            db_rds: list[ResourceDefDB] = (await db_sess.execute(query)).scalars().all()

        next_page_ref = None
        if len(db_rds) == config['page_size']:
            next_page_ref = str(db_rds[-1].internal_id)

        result: ResourceDefsPage = {
            "resource_defs": [],
            "next_page_ref": next_page_ref,
            "error": None
        }
        for rd in db_rds:
            result['resource_defs'].append(
                {
                    "resource_type": rd.resource_type,
                    "actions": rd.actions,
                    "schema": rd.schema
                }
            )

        return result


    async def get_resource_def(
        self,
        resource_type: str,
        config: GetResourceDefConfig
    ) -> ResourceDefResult:
        async with self._async_sessionmaker() as db_sess:
            db_rd: ResourceDefDB | None = (
                await db_sess.execute(
                    select(ResourceDefDB).where(ResourceDefDB.resource_type == resource_type)
                )
            ).scalar_one_or_none()

        if db_rd is not None:
            return {
                "resource_def": {
                    "resource_type": db_rd.resource_type,
                    "actions": db_rd.actions,
                    "schema": db_rd.schema
                },
                "error": None
            }

        return {
            "resource_def": None,
            "error": {
                "error_type": "resource_not_found",
                "message": f"Resource type '{resource_type}' was not found."
            }
        }


    async def put_resource_def(
        self,
        resource_def: ResourceDef,
        config: PutResourceDefConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            db_rd: ResourceDefDB | None = (
                await db_sess.execute(
                    select(
                        ResourceDefDB
                    ).where(
                        ResourceDefDB.resource_type == resource_def['resource_type']
                    )
                )
            ).scalar_one_or_none()
            if db_rd is None:
                db_sess.add(
                    ResourceDefDB(
                        resource_type=resource_def['resource_type'],
                        actions=resource_def['actions'],
                        schema=resource_def['schema']
                    )
                )
            else:
                db_rd.actions = resource_def['actions']
                db_rd.schema = resource_def['schema']

            await db_sess.commit()

        return {
            "error": None
        }


    async def delete_resource_def(
        self,
        resource_type: str,
        config: DeleteResourceDefConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            await db_sess.execute(
                delete(ResourceDefDB).where(ResourceDefDB.resource_type == resource_type)
            )
            await db_sess.commit()

        return {
            "error": None
        }


    async def enact(self, grant: Grant, config: EnactConfig) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            db_sess.add(
                GrantDB(
                    grant_uuid=UUID(grant['grant_uuid']),
                    name=grant['name'],
                    description=grant['description'],
                    tags=grant['tags'],
                    effect=grant['effect'],
                    actions=grant['actions'],
                    query=grant['query'],
                    equality=grant['equality'],
                    applicable_on_failure=grant['applicable_on_failure'],
                    data=grant['data']
                )
            )
            await db_sess.commit()

        return {
            "error": None
        }


    async def repeal(
        self,
        grant_uuid: str,
        purge: bool,
        config: RepealConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            await db_sess.execute(
                delete(GrantDB).where(GrantDB.grant_uuid == UUID(grant_uuid))
            )
            await db_sess.commit()

        return {
            "error": None
        }


    async def get_grant(
        self,
        grant_uuid: str,
        config: GetGrantConfig
    ) -> GrantResult:
        async with self._async_sessionmaker() as db_sess:
            db_grant: GrantDB | None = (
                await db_sess.execute(
                    select(GrantDB).where(GrantDB.grant_uuid == UUID(grant_uuid))
                )
            ).scalar_one_or_none()

        if db_grant is not None:
            return {
                "grant": self._grant_from_db(db_grant),
                "error": None
            }

        return {
            "grant": None,
            "error": {
                "error_type": "resource_not_found",
                "message": f"Grant with UUID '{grant_uuid}' was not found."
            }
        }


    async def list_grants(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantsConfig
    ) -> GrantsPage:
        async with self._async_sessionmaker() as db_sess:
            query = select(GrantDB).limit(config['page_size']).order_by(GrantDB.internal_id)
            if effect is not None:
                query = query.where(GrantDB.effect == effect)

            if action is not None:
                query = query.where(GrantDB.actions.contains(action))

            if page_ref is not None:
                query = query.where(GrantDB.internal_id > int(page_ref))

            db_grants: list[GrantDB] = (await db_sess.execute(query)).scalars().all()

        next_page_ref = None
        if len(db_grants) == config['page_size']:
            next_page_ref = str(db_grants[-1].internal_id)

        result: GrantsPage = {
            "grants": [],
            "next_page_ref": next_page_ref,
            "error": None
        }
        for db_grant in db_grants:
            result['grants'].append(self._grant_from_db(db_grant))

        return result


    async def list_grant_refs(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantRefsConfig
    ) -> PageRefsPage:
        return {
            "page_refs": [],
            "next_page_ref": None,
            "error": {
                "error_type": "parallel_pagination_not_supported",
                "message": "SQLStorage does not support parallel pagination."
            }
        }


    def _grant_from_db(self, db_grant: GrantDB) -> Grant:
        return {
            "grant_uuid": str(db_grant.grant_uuid),
            "name": db_grant.name,
            "description": db_grant.description,
            "tags": db_grant.tags,
            "effect": db_grant.effect,
            "actions": db_grant.actions,
            "query": db_grant.query,
            "equality": db_grant.equality,
            "applicable_on_failure": db_grant.applicable_on_failure,
            "data": db_grant.data
        }


    async def create_latch(self, config: CreateLatchConfig) -> StorageLatchResult:
        latch_uuid = uuid4()
        created_at = datetime.datetime.now(tz=datetime.timezone.utc)
        async with self._async_sessionmaker() as db_sess:
            db_sess.add(
                StorageLatchDB(
                    storage_latch_uuid=latch_uuid,
                    is_set=False,
                    created_at=created_at
                )
            )
            await db_sess.commit()

        return {
            "storage_latch": {
                "storage_latch_uuid": str(latch_uuid),
                "is_set": False,
                "created_at": created_at.isoformat()
            },
            "error": None
        }


    async def get_latch(
        self,
        storage_latch_uuid: str,
        config: GetLatchConfig
    ) -> StorageLatchResult:
        async with self._async_sessionmaker() as db_sess:
            db_latch: StorageLatchDB | None = (
                await db_sess.execute(
                    select(
                        StorageLatchDB
                    ).where(
                        StorageLatchDB.storage_latch_uuid == UUID(storage_latch_uuid)
                    )
                )
            ).scalar_one_or_none()

        if db_latch is not None:
            return {
                "storage_latch": self._latch_from_db(db_latch),
                "error": None
            }

        return {
            "storage_latch": None,
            "error": {
                "error_type": "resource_not_found",
                "message": f"Storage latch with UUID '{storage_latch_uuid}' was not found."
            }
        }


    async def set_latch(
        self,
        storage_latch_uuid: str,
        config: SetLatchConfig
    ) -> StorageLatchResult:
        async with self._async_sessionmaker() as db_sess:
            db_latch: StorageLatchDB | None = (
                await db_sess.execute(
                    select(
                        StorageLatchDB
                    ).where(
                        StorageLatchDB.storage_latch_uuid == UUID(storage_latch_uuid)
                    )
                )
            ).scalar_one_or_none()
            if db_latch is None:
                return {
                    "storage_latch": None,
                    "error": {
                        "error_type": "resource_not_found",
                        "message": f"Storage latch with UUID '{storage_latch_uuid}' was not found."
                    }
                }

            db_latch.is_set = True
            latch = self._latch_from_db(db_latch)
            await db_sess.commit()

        return {
            "storage_latch": latch,
            "error": None
        }


    async def delete_latch(
        self,
        storage_latch_uuid: str,
        config: DeleteLatchConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            await db_sess.execute(
                delete(
                    StorageLatchDB
                ).where(
                    StorageLatchDB.storage_latch_uuid == UUID(storage_latch_uuid)
                )
            )
            await db_sess.commit()

        return {
            "error": None
        }


    async def cleanup_latches(
        self,
        before: datetime.datetime,
        config: CleanupLatchesConfig
    ) -> GenericResult:
        async with self._async_sessionmaker() as db_sess:
            await db_sess.execute(
                delete(StorageLatchDB).where(StorageLatchDB.created_at < before)
            )
            await db_sess.commit()

        return {
            "error": None
        }


    def _latch_from_db(self, db_latch: StorageLatchDB) -> StorageLatch:
        created_at = db_latch.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=datetime.timezone.utc)
        else:
            created_at = created_at.astimezone(datetime.timezone.utc)

        return {
            "storage_latch_uuid": str(db_latch.storage_latch_uuid),
            "is_set": db_latch.is_set,
            "created_at": created_at.isoformat()
        }
