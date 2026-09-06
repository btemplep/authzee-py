""""""

__all__ = [
    "SQLStorage"
]

import datetime
import json
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import delete, event, select
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
    name: Mapped[str] = mapped_column(nullable=False)
    equality: Mapped[Any] = mapped_column(nullable=True)
    applicable_on_failure: Mapped[bool] = mapped_column(nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(nullable=False)


class StorageLatchDB(Base):
    __tablename__ = "storage_latches"
    internal_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    storage_latch_uuid: Mapped[UUID] = mapped_column(unique=True, nullable=False)
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
            query = select(ContextDefDB).limit(config["page_size"]).order_by(ContextDefDB.internal_id)
            if page_ref is not None:
                query = query.where(ContextDefDB.internal_id > int(page_ref))
            
            context_defs: list[ContextDefDB] = (await db_sess.execute()).scalars().all()

        next_page_ref = None
        if len(context_defs) > 0:
            next_page_ref = str(context_defs[-1].internal_id)

        result: ContextDefsPage = {
            "context_defs": [],
            "next_page_ref": next_page_ref,
            "error": None
        }
        for cd in context_defs:
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
        """Get a context definition by type.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def put_context_def(
        self,
        context_def: ContextDef,
        config: PutContextDefConfig
    ) -> GenericResult:
        """Add a new Context Definition or update an existing one.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def delete_context_def(
        self,
        context_type: str,
        config: DeleteContextDefConfig
    ) -> GenericResult:
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def list_identity_defs(
        self,
        page_ref: str | None,
        config: ListIdentityDefsConfig
    ) -> IdentityDefsPage:
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def get_identity_def(
        self,
        identity_type: str,
        config: GetIdentityDefConfig
    ) -> IdentityDefResult:
        """Get an identity definition by type.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def put_identity_def(
        self,
        identity_def: IdentityDef,
        config: PutIdentityDefConfig
    ) -> GenericResult:
        """Add a new Identity Definition or update an existing one.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def delete_identity_def(
        self,
        identity_type: str,
        config: DeleteIdentityDefConfig
    ) -> GenericResult:
        """Delete an identity definition by type.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def list_resource_defs(
        self,
        page_ref: str | None,
        config: ListResourceDefsConfig
    ) -> ResourceDefsPage:
        """Get a page of resource definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def get_resource_def(
        self,
        resource_type: str,
        config: GetResourceDefConfig
    ) -> ResourceDefResult:
        """Get a resource definition by type.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def put_resource_def(
        self,
        resource_def: ResourceDef,
        config: PutResourceDefConfig
    ) -> GenericResult:
        """Add a new Resource Definition or update an existing one.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def delete_resource_def(
        self,
        resource_type: str,
        config: DeleteResourceDefConfig
    ) -> GenericResult:
        """Delete a resource definition by type.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def enact(self, grant: Grant, config: EnactConfig) -> GenericResult:
        """Add a new grant.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def repeal(
        self,
        grant_uuid: str,
        purge: bool,
        config: RepealConfig
    ) -> GenericResult:
        """Delete a grant.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def get_grant(
        self,
        grant_uuid: str,
        config: GetGrantConfig
    ) -> GrantResult:
        """Get a grant by UUID.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def list_grants(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantsConfig
    ) -> GrantsPage:
        """Retrieve a page of grants.

        Pass the returned page reference to get the next page until a null page reference is returned.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def list_grant_refs(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantRefsConfig
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Pass the returned page reference to get the next page until a null page reference is returned.

        For some storage modules this may not be possible.
        Check the `parallel_paging` attribute on the storage module after `start()` is complete.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def create_latch(self, config: CreateLatchConfig) -> StorageLatchResult:
        """Create a new [storage latch](#storage-latches).
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def get_latch(
        self,
        storage_latch_uuid: str,
        config: GetLatchConfig
    ) -> StorageLatchResult:
        """Get a [storage latch](#storage-latches) by UUID.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def set_latch(
        self,
        storage_latch_uuid: str,
        config: SetLatchConfig
    ) -> StorageLatchResult:
        """Set a [storage latch](#storage-latches) by UUID.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def delete_latch(
        self,
        storage_latch_uuid: str,
        config: DeleteLatchConfig
    ) -> GenericResult:
        """Delete a [storage latch](#storage-latches) by UUID.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }


    async def cleanup_latches(
        self,
        before: datetime.datetime,
        config: CleanupLatchesConfig
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        - operations should clean up their own latches, but in case of a failure this can be used to clean up zombie latches.
        """
        try:
            pass
        except Exception as exc:
            return {
                "error": {
                    "error_type": "storage",
                    "message": f"[{exc.__class__.__qualname__}]: {exc}"
                }
            }
