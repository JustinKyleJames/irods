#include "irods/dataObjCopy.h"
#include "irods/procApiRequest.h"
#include "irods/apiNumber.h"

#include <cstring>

/**
 * \fn rcDataObjCopy (rcComm_t *conn, dataObjCopyInp_t *dataObjCopyInp)
 *
 * \brief Copy a data object from a iRODS path to another a iRODS path.
 *
 * \user client
 *
 * \ingroup data_object
 *
 * \since 1.0
 *
 *
 * \remark none
 *
 * \note none
 *
 * \usage
 * Copy a data object /myZone/home/john/myfileA to /myZone/home/john/myfileB
 *     and store in myRescource:
 * \n dataObjCopyInp_t dataObjCopyInp;
 * \n memset(&dataObjCopyInp, 0, sizeof(dataObjCopyInp));
 * \n rstrcpy (dataObjCopyInp.destDataObjInp.objPath,
 *       "/myZone/home/john/myfileB", MAX_NAME_LEN);
 * \n rstrcpy (dataObjCopyInp.srcDataObjInp.objPath,
 *      "/myZone/home/john/myfileA", MAX_NAME_LEN);
 * \n addKeyVal (&dataObjCopyInp.destDataObjInp.condInput, DEST_RESC_NAME_KW,
 *      "myRescource");
 * \n status = rcDataObjCopy (conn, &dataObjCopyInp);
 * \n if (status < 0) {
 * \n .... handle the error
 * \n }
 *
 * \param[in] conn - A rcComm_t connection handle to the server.
 * \param[in] dataObjCopyInp - Elements of dataObjCopyInp_t used :
 *    \li char \b srcDataObjInp.objPath[MAX_NAME_LEN] - full path of the
 *         source data object.
 *    \li char \b destDataObjInp.objPath[MAX_NAME_LEN] - full path of the
 *         target data object.
 *    \li int \b destDataObjInp.numThreads - the number of threads to use.
 *        Valid values are:
 *      \n NO_THREADING (-1) - no multi-thread
 *      \n 0 - the server will decide the number of threads.
 *        (recommended setting).
 *      \n A positive integer - specifies the number of threads.
 *    \li keyValPair_t \b destDataObjInp.condInput - keyword/value pair input.
 *       Valid keywords:
 *    \n DATA_TYPE_KW - the data type of the data object.
 *    \n FORCE_FLAG_KW - overwrite existing target. This keyWd has no value
 *    \n REG_CHKSUM_KW -  register the target checksum value after the copy.
 *	      This keyWd has no value.
 *    \n VERIFY_CHKSUM_KW - verify and register the target checksum value
 *            after the copy. This keyWd has no value.
 *    \n DEST_RESC_NAME_KW - The resource to store this data object
 *
 * \return integer
 * \retval 0 on success

 * \sideeffect none
 * \pre none
 * \post none
 * \sa none
**/

int
rcDataObjCopy( rcComm_t *conn, dataObjCopyInp_t *dataObjCopyInp ) {
    int status;
    transferStat_t *transferStat = NULL;

    memset( &conn->transStat, 0, sizeof( transferStat_t ) );

    dataObjCopyInp->srcDataObjInp.oprType = COPY_SRC;
    dataObjCopyInp->destDataObjInp.oprType = COPY_DEST;

    status = _rcDataObjCopy( conn, dataObjCopyInp, &transferStat );

    if ( status >= 0 && transferStat != NULL ) {
        conn->transStat = *( transferStat );
    }
    if ( transferStat != NULL ) {
        free( transferStat );
    }

    return status;
}

int
_rcDataObjCopy( rcComm_t *conn, dataObjCopyInp_t *dataObjCopyInp,
                transferStat_t **transferStat ) {
    int status;

    status = procApiRequest( conn, DATA_OBJ_COPY_AN,  dataObjCopyInp, NULL,
                             ( void ** ) transferStat, NULL );

    return status;
}
