
/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/cursor_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/queued_data_stage.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/query/cursor_request.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/uuid.h"

namespace mongo {

using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;
using stdx::make_unique;

namespace {

/**
 * Lists the indexes for a given collection.
 * If the optional 'includeIndexBuilds' field is set to true, returns indexes that are not
 * ready. Defaults to false.
 * These not-ready indexes are identified by a 'buildUUID' field in the index spec.
 *
 * Format:
 * {
 *   listIndexes: <collection name>,
 *   includeIndexBuilds: <boolean>,
 * }
 *
 * Return format:
 * {
 *   indexes: [
 *     ...
 *   ]
 * }
 */
class CmdListIndexes : public BasicCommand {
public:
    CmdListIndexes() : BasicCommand("listIndexes") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kOptIn;
    }

    virtual bool adminOnly() const {
        return false;
    }
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    std::string help() const override {
        return "list indexes for a collection";
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const std::string& dbname,
                                 const BSONObj& cmdObj) const override {
        AuthorizationSession* authzSession = AuthorizationSession::get(opCtx->getClient());

        if (!authzSession->isAuthorizedToParseNamespaceElement(cmdObj.firstElement())) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }

        // Check for the listIndexes ActionType on the database.
        const auto nss = AutoGetCollection::resolveNamespaceStringOrUUID(
            opCtx, CommandHelpers::parseNsOrUUID(dbname, cmdObj));
        if (authzSession->isAuthorizedForActionsOnResource(ResourcePattern::forExactNamespace(nss),
                                                           ActionType::listIndexes)) {
            return Status::OK();
        }

        return Status(ErrorCodes::Unauthorized,
                      str::stream() << "Not authorized to list indexes on collection: "
                                    << nss.ns());
    }

    bool run(OperationContext* opCtx,
             const string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) {
        const long long defaultBatchSize = std::numeric_limits<long long>::max();
        long long batchSize;
        uassertStatusOK(
            CursorRequest::parseCommandCursorOptions(cmdObj, defaultBatchSize, &batchSize));

        auto includeIndexBuilds = cmdObj["includeIndexBuilds"].trueValue();

        NamespaceString nss;
        std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec;
        BSONArrayBuilder firstBatch;
        {
            AutoGetCollectionForReadCommand ctx(opCtx,
                                                CommandHelpers::parseNsOrUUID(dbname, cmdObj));
            Collection* collection = ctx.getCollection();
            uassert(ErrorCodes::NamespaceNotFound,
                    str::stream() << "ns does not exist: " << ctx.getNss().ns(),
                    collection);

            const CollectionCatalogEntry* cce = collection->getCatalogEntry();
            invariant(cce);

            nss = ctx.getNss();

            vector<string> indexNames;
            writeConflictRetry(opCtx, "listIndexes", nss.ns(), [&] {
                indexNames.clear();
                if (includeIndexBuilds) {
                    cce->getAllIndexes(opCtx, &indexNames);
                } else {
                    cce->getReadyIndexes(opCtx, &indexNames);
                }
            });

            auto ws = make_unique<WorkingSet>();
            auto root = make_unique<QueuedDataStage>(opCtx, ws.get());

            for (size_t i = 0; i < indexNames.size(); i++) {
                auto indexSpec = writeConflictRetry(opCtx, "listIndexes", nss.ns(), [&] {
                    if (includeIndexBuilds && !cce->isIndexReady(opCtx, indexNames[i])) {
                        auto spec = cce->getIndexSpec(opCtx, indexNames[i]);
                        BSONObjBuilder builder(spec);
                        // TODO(SERVER-37980): Replace with index build UUID.
                        auto indexBuildUUID = UUID::gen();
                        indexBuildUUID.appendToBuilder(&builder, "buildUUID"_sd);
                        return builder.obj();
                    }
                    return cce->getIndexSpec(opCtx, indexNames[i]);
                });

                WorkingSetID id = ws->allocate();
                WorkingSetMember* member = ws->get(id);
                member->keyData.clear();
                member->recordId = RecordId();
                member->obj = Snapshotted<BSONObj>(SnapshotId(), indexSpec.getOwned());
                member->transitionToOwnedObj();
                root->pushBack(id);
            }

            exec = uassertStatusOK(PlanExecutor::make(
                opCtx, std::move(ws), std::move(root), nss, PlanExecutor::NO_YIELD));

            for (long long objCount = 0; objCount < batchSize; objCount++) {
                BSONObj next;
                PlanExecutor::ExecState state = exec->getNext(&next, NULL);
                if (state == PlanExecutor::IS_EOF) {
                    break;
                }
                invariant(state == PlanExecutor::ADVANCED);

                // If we can't fit this result inside the current batch, then we stash it for later.
                if (!FindCommon::haveSpaceForNext(next, objCount, firstBatch.len())) {
                    exec->enqueue(next);
                    break;
                }

                firstBatch.append(next);
            }

            if (exec->isEOF()) {
                appendCursorResponseObject(0LL, nss.ns(), firstBatch.arr(), &result);
                return true;
            }

            exec->saveState();
            exec->detachFromOperationContext();
        }  // Drop collection lock. Global cursor registration must be done without holding any
           // locks.

        const auto pinnedCursor = CursorManager::getGlobalCursorManager()->registerCursor(
            opCtx,
            {std::move(exec),
             nss,
             AuthorizationSession::get(opCtx->getClient())->getAuthenticatedUserNames(),
             repl::ReadConcernArgs::get(opCtx),
             cmdObj,
             ClientCursorParams::LockPolicy::kLocksInternally});

        appendCursorResponseObject(
            pinnedCursor.getCursor()->cursorid(), nss.ns(), firstBatch.arr(), &result);

        return true;
    }

} cmdListIndexes;

}  // namespace
}  // namespace mongo
