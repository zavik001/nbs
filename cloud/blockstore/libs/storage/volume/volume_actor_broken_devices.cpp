#include "volume_actor.h"

#include "volume_database.h"
#include "volume_tx.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleBrokenDeviceNotification(
    const TEvNonreplPartitionPrivate::TEvBrokenDeviceNotification::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s marked broken at %s",
        LogTitle.GetWithTime().c_str(),
        msg->DeviceUUID.Quote().c_str(),
        msg->BrokenTs.ToString().c_str());

    if (BrokenDevices.contains(msg->DeviceUUID)) {
        return;
    }

    BrokenDevices[msg->DeviceUUID] = msg->BrokenTs;

    ExecuteTx<TAddBrokenDevice>(ctx, msg->DeviceUUID, msg->BrokenTs);
}

void TVolumeActor::HandleDeviceRecoveredNotification(
    const TEvNonreplPartitionPrivate::TEvDeviceRecoveredNotification::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Device %s recovered",
        LogTitle.GetWithTime().c_str(),
        msg->DeviceUUID.Quote().c_str());

    if (!BrokenDevices.contains(msg->DeviceUUID)) {
        return;
    }

    BrokenDevices.erase(msg->DeviceUUID);

    ExecuteTx<TRemoveBrokenDevice>(ctx, msg->DeviceUUID);
}

bool TVolumeActor::PrepareAddBrokenDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TAddBrokenDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteAddBrokenDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TAddBrokenDevice& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    db.WriteBrokenDevice(args.DeviceUUID, args.BrokenTs);
}

void TVolumeActor::CompleteAddBrokenDevice(
    const TActorContext& ctx,
    TTxVolume::TAddBrokenDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

bool TVolumeActor::PrepareRemoveBrokenDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TRemoveBrokenDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TVolumeActor::ExecuteRemoveBrokenDevice(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TRemoveBrokenDevice& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    db.DeleteBrokenDevice(args.DeviceUUID);
}

void TVolumeActor::CompleteRemoveBrokenDevice(
    const TActorContext& ctx,
    TTxVolume::TRemoveBrokenDevice& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
