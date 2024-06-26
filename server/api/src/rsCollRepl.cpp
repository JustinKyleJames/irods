#include "irods/collRepl.h"
#include "irods/dataObjOpr.hpp"
#include "irods/rodsLog.h"
#include "irods/objMetaOpr.hpp"
#include "irods/rsCollRepl.hpp"
#include "irods/openCollection.h"
#include "irods/readCollection.h"
#include "irods/closeCollection.h"
#include "irods/dataObjRepl.h"
#include "irods/rsApiHandler.hpp"
#include "irods/getRemoteZoneResc.h"
#include "irods/rsOpenCollection.hpp"
#include "irods/rsCloseCollection.hpp"
#include "irods/rsReadCollection.hpp"
#include "irods/rsDataObjRepl.hpp"

#include <cstring>

/* rsCollRepl - The Api handler of the rcCollRepl call - Replicate
 * a data object.
 * Input -
 *    rsComm_t *rsComm
 *     dataObjInp_t *collReplInp - The replication input
 *    collOprStat_t **collOprStat - transfer stat output. If it is an
 *     internal server call, collOprStat must be NULL
 */

int
rsCollRepl( rsComm_t *rsComm, collInp_t *collReplInp,
            collOprStat_t **collOprStat ) {
    int status;
    dataObjInp_t dataObjInp;
    int handleInx;
    int totalFileCnt = 0;
    int fileCntPerStatOut;
    int savedStatus = 0;
    int remoteFlag;
    rodsServerHost_t *rodsServerHost;

    /* try to connect to dest resc */
    std::memset(&dataObjInp, 0, sizeof(dataObjInp));
    rstrcpy( dataObjInp.objPath, collReplInp->collName, MAX_NAME_LEN );
    remoteFlag = getAndConnRemoteZone( rsComm, &dataObjInp, &rodsServerHost,
                                       REMOTE_CREATE );

    if ( remoteFlag < 0 ) {
        return remoteFlag;
    }
    else if ( remoteFlag == REMOTE_HOST ) {
        int retval;
        retval = _rcCollRepl( rodsServerHost->conn, collReplInp, collOprStat );
        if ( retval < 0 ) {
            return retval;
        }
        status = svrSendZoneCollOprStat( rsComm, rodsServerHost->conn,
                                         *collOprStat, retval );
        return status;
    }

    fileCntPerStatOut = FILE_CNT_PER_STAT_OUT;
    if ( collOprStat != NULL ) {
        *collOprStat = NULL;
    }
    collReplInp->flags = RECUR_QUERY_FG;
    handleInx = rsOpenCollection( rsComm, collReplInp );
    if ( handleInx < 0 ) {
        rodsLog( LOG_ERROR,
                 "rsCollRepl: rsOpenCollection of %s error. status = %d",
                 collReplInp->collName, handleInx );
        return handleInx;
    }

    if ( collOprStat != NULL ) {
        *collOprStat = ( collOprStat_t* )malloc( sizeof( collOprStat_t ) );
        memset( *collOprStat, 0, sizeof( collOprStat_t ) );
    }

    if ( CollHandle[handleInx].rodsObjStat->specColl != NULL ) {
        rodsLog( LOG_ERROR,
                 "rsCollRepl: unable to replicate mounted collection %s",
                 collReplInp->collName );
        rsCloseCollection( rsComm, &handleInx );
        return 0;
    }

    collEnt_t *collEnt = NULL;
    while ( ( status = rsReadCollection( rsComm, &handleInx, &collEnt ) ) >= 0 ) {
        if ( collEnt->objType == DATA_OBJ_T ) {
            if ( totalFileCnt == 0 ) totalFileCnt =
                    CollHandle[handleInx].dataObjSqlResult.totalRowCount;

            std::memset(&dataObjInp, 0, sizeof(dataObjInp));
            snprintf( dataObjInp.objPath, MAX_NAME_LEN, "%s/%s",
                      collEnt->collName, collEnt->dataName );
            dataObjInp.condInput = collReplInp->condInput;

            transferStat_t myTransStat{};
            transferStat_t* stat{&myTransStat};
            status = rsDataObjRepl(rsComm, &dataObjInp, &stat);

            if ( status == SYS_COPY_ALREADY_IN_RESC ) {
                savedStatus = status;
                status = 0;
            }

            if ( status < 0 ) {
                rodsLogError( LOG_ERROR, status,
                              "rsCollRepl: rsDataObjRepl failed for %s. status = %d",
                              dataObjInp.objPath, status );
                savedStatus = status;
                break;
            }
            else {
                if ( collOprStat != NULL ) {
                    ( *collOprStat )->bytesWritten += myTransStat.bytesWritten;
                    ( *collOprStat )->filesCnt ++;
                }
            }
            if ( collOprStat != NULL &&
                    ( *collOprStat )->filesCnt >= fileCntPerStatOut ) {
                rstrcpy( ( *collOprStat )->lastObjPath, dataObjInp.objPath,
                         MAX_NAME_LEN );
                ( *collOprStat )->totalFileCnt = totalFileCnt;
                status = svrSendCollOprStat( rsComm, *collOprStat );
                if ( status < 0 ) {
                    rodsLogError( LOG_ERROR, status,
                                  "rsCollRepl: svrSendCollOprStat failed for %s. status = %d",
                                  dataObjInp.objPath, status );
                    *collOprStat = NULL;
                    savedStatus = status;
                    break;
                }
                *collOprStat = ( collOprStat_t* )malloc( sizeof( collOprStat_t ) );
                memset( *collOprStat, 0, sizeof( collOprStat_t ) );
            }
        }
        free( collEnt );	   /* just free collEnt but not content */
        collEnt = NULL;
    }
    rsCloseCollection( rsComm, &handleInx );
    free(collEnt);

    return savedStatus;
}
