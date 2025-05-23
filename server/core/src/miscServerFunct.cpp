#include "irods/miscServerFunct.hpp"

#include "irods/authentication_plugin_framework.hpp"
#include "irods/dataObjOpen.h"
#include "irods/dataObjLseek.h"
#include "irods/irods_configuration_keywords.hpp"
#include "irods/rcMisc.h"
#include "irods/dataObjOpr.hpp"
#include "irods/dataObjClose.h"
#include "irods/dataObjWrite.h"
#include "irods/dataObjRead.h"
#include "irods/modAVUMetadata.h"
#include "irods/genQuery.h"
#include "irods/rcPortalOpr.h"
#include "irods/rcConnect.h"
#include "irods/rodsConnect.h"
#include "irods/sockComm.h"
#include "irods/modAccessControl.h"
#include "irods/rsDataObjOpen.hpp"
#include "irods/rsDataObjClose.hpp"
#include "irods/rsDataObjLseek.hpp"
#include "irods/rsDataObjWrite.hpp"
#include "irods/rsDataObjRead.hpp"
#include "irods/rsFileOpen.hpp"
#include "irods/rsGenQuery.hpp"
#include "irods/rsModAVUMetadata.hpp"
#include "irods/rsModAccessControl.hpp"
#include "irods/rsFileClose.hpp"
#include "irods/rsGlobalExtern.hpp"
#include "irods/rcGlobalExtern.h"
#include "irods/irods_stacktrace.hpp"
#include "irods/irods_network_factory.hpp"
#include "irods/irods_buffer_encryption.hpp"
#include "irods/irods_client_server_negotiation.hpp"
#include "irods/irods_exception.hpp"
#include "irods/irods_serialization.hpp"
#include "irods/irods_server_properties.hpp"
#include "irods/irods_hierarchy_parser.hpp"
#include "irods/irods_threads.hpp"
#include "irods/irods_lexical_cast.hpp"
#include "irods/sockCommNetworkInterface.hpp"
#include "irods/irods_random.hpp"
#include "irods/irods_resource_manager.hpp"
#include "irods/irods_default_paths.hpp"
#include "irods/irods_logger.hpp"

#include <boost/filesystem.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/scoped_thread.hpp>
#include <boost/lexical_cast.hpp>

#include <fmt/format.h>

#include <sys/wait.h>
#include <openssl/md5.h>

#include <cstring>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>

// clang-format off
using log_server    = irods::experimental::log::server;
using leaf_bundle_t = irods::resource_manager::leaf_bundle_t;
// clang-format on

namespace {

int l3OpenByHost( rsComm_t *rsComm, int l3descInx, int flags ) {
    fileOpenInp_t fileOpenInp{};
    rstrcpy( fileOpenInp.resc_hier_, FileDesc[l3descInx].rescHier, MAX_NAME_LEN );
    rstrcpy( fileOpenInp.fileName, FileDesc[l3descInx].fileName, MAX_NAME_LEN );
    rstrcpy( fileOpenInp.objPath, FileDesc[l3descInx].objPath, MAX_NAME_LEN );
    fileOpenInp.mode = FileDesc[l3descInx].mode;
    fileOpenInp.flags = flags;

    return rsFileOpenByHost(rsComm, &fileOpenInp, FileDesc[l3descInx].rodsServerHost);
} // l3OpenByHost

int _l3Close( rsComm_t *rsComm, int l3descInx ) {
    fileCloseInp_t fileCloseInp{};
    fileCloseInp.fileInx = l3descInx;
    return rsFileClose( rsComm, &fileCloseInp );
} // _l3Close

} // anonymous namespace

int
svrToSvrConnectNoLogin( rsComm_t *rsComm, rodsServerHost_t *rodsServerHost ) {
    rErrMsg_t errMsg;
    int reconnFlag;


    if ( rodsServerHost->conn == NULL ) { /* a connection already */
        if ( getenv( RECONNECT_ENV ) != NULL ) {
            reconnFlag = RECONN_TIMEOUT;
        }
        else {
            reconnFlag = NO_RECONN;
        }
        rodsServerHost->conn = _rcConnect( rodsServerHost->hostName->name,
                                           ( ( zoneInfo_t * ) rodsServerHost->zoneInfo )->portNum,
                                           rsComm->myEnv.rodsUserName, rsComm->myEnv.rodsZone,
                                           rsComm->clientUser.userName, rsComm->clientUser.rodsZone, &errMsg,
                                           rsComm->connectCnt, reconnFlag );

        if ( rodsServerHost->conn == NULL ) {
            if ( errMsg.status < 0 ) {
                return errMsg.status;
            }
            else {
                return SYS_SVR_TO_SVR_CONNECT_FAILED - errno;
            }
        }
    }

    return rodsServerHost->localFlag;
}

int
svrToSvrConnect( rsComm_t *rsComm, rodsServerHost_t *rodsServerHost ) {
    int status;

    status = svrToSvrConnectNoLogin( rsComm, rodsServerHost );

    if ( status < 0 ) {
        return status;
    }

    RodsEnvironment env;
    _getRodsEnv(env);

    namespace ia = irods::authentication;

    status = ia::authenticate_client(*rodsServerHost->conn, nlohmann::json{{ia::scheme_name, env.rodsAuthScheme}});
    if ( status < 0 ) {
        // TODO(#8003): Replace rodsLog calls
        rodsLog(LOG_NOTICE, "svrToSvrConnect: authenticate_client to %s failed", rodsServerHost->hostName->name);
        return status;
    }
    else {
        return rodsServerHost->localFlag;
    }
}

/* setupSrvPortalForParaOpr - Setup the portal on this server for
 * parallel transfer. It call createSrvPortal to create
 * the portal socket, malloc the portalOprOut struct, put the
 * server address, portNumber, etc in this struct. Also malloc
 * the rsComm->portalOpr struct and fill in the struct. This struct
 * will be used later by the server after sending reply to the client.
 */

int
setupSrvPortalForParaOpr( rsComm_t *rsComm, dataOprInp_t *dataOprInp,
                          int oprType, portalOprOut_t **portalOprOut ) {
    portalOprOut_t *myDataObjPutOut = NULL;
    int portalSock = 0;
    int proto = SOCK_STREAM;

    myDataObjPutOut = ( portalOprOut_t * ) malloc( sizeof( portalOprOut_t ) );
    memset( myDataObjPutOut, 0, sizeof( portalOprOut_t ) );

    *portalOprOut = myDataObjPutOut;

    if (getValByKey(&dataOprInp->condInput, STREAMING_KW) != nullptr) {
        /* streaming - use only one thread */
        myDataObjPutOut->numThreads = 1;
    }
    else {
        myDataObjPutOut->numThreads = getNumThreads( rsComm,
                                      dataOprInp->dataSize, dataOprInp->numThreads,
                                      &dataOprInp->condInput,
                                      //getValByKey (&dataOprInp->condInput, RESC_NAME_KW), NULL);
                                      getValByKey( &dataOprInp->condInput,  RESC_HIER_STR_KW ), NULL, oprType );
    }

    if ( myDataObjPutOut->numThreads == 0 ) {
        return 0;
    }
    else {
        int port_range_count;
        try {
            const auto svr_port_range_start = irods::get_server_property<const int>(irods::KW_CFG_SERVER_PORT_RANGE_START);
            const auto svr_port_range_end = irods::get_server_property<const int>(irods::KW_CFG_SERVER_PORT_RANGE_END);
            port_range_count = svr_port_range_end - svr_port_range_start + 1;
        } catch ( irods::exception& e ) {
            return e.code();
        }

        /* setup the portal - try port_range_count times in case of bind collision */
        for ( int i = 0; i < port_range_count; ++i ) {
            portalSock = createSrvPortal( rsComm, &myDataObjPutOut->portList,
                                        proto );
            if ( 0 <= portalSock ) {
                break;
            }
        }
        if ( portalSock < 0 ) {
            rodsLog( LOG_NOTICE,
                     "setupSrvPortalForParaOpr: createSrvPortal error, status = %d",
                     portalSock );
            myDataObjPutOut->status = portalSock;
            return portalSock;
        }
        rsComm->portalOpr = ( portalOpr_t * ) malloc( sizeof( portalOpr_t ) );
        rsComm->portalOpr->oprType = oprType;
        rsComm->portalOpr->portList = myDataObjPutOut->portList;
        rsComm->portalOpr->dataOprInp = *dataOprInp;
        memset( &dataOprInp->condInput, 0, sizeof( dataOprInp->condInput ) );
        rsComm->portalOpr->dataOprInp.numThreads = myDataObjPutOut->numThreads;
    }

    return 0;
}

/* createSrvPortal - create a server socket portal.
 * proto is a SOCK_STREAM
 */

int
createSrvPortal( rsComm_t *rsComm, portList_t *thisPortList, int proto ) {
    int lsock = -1;
    int lport = 0;
    char* laddr = nullptr;

    if (proto != SOCK_STREAM) {
        rodsLog(LOG_ERROR, "createSrvPortal: invalid input protocol %d", proto);
        return SYS_INVALID_PROTOCOL_TYPE;
    }

    if ( ( lsock = svrSockOpenForInConn( rsComm, &lport, &laddr,
                                         SOCK_STREAM ) ) < 0 ) {
        rodsLog( LOG_ERROR,
                 "createSrvPortal: svrSockOpenForInConn failed: status=%d",
                 lsock );
        return lsock;
    }

    thisPortList->sock = lsock;
    thisPortList->cookie = ( int )( irods::getRandom<unsigned int>() >> 1 );
    if ( ProcessType == CLIENT_PT ) {
        rstrcpy( thisPortList->hostAddr, laddr, LONG_NAME_LEN );
    }
    else {
        /* server. try to use what is configured */
        if (    LocalServerHost != NULL
             && strcmp( LocalServerHost->hostName->name, "localhost" ) != 0
             && get_canonical_name( LocalServerHost->hostName->name, thisPortList->hostAddr, LONG_NAME_LEN) == 0 ) {
            // empty block b/c get_canonical_name does the copy on success
        } else {
            rstrcpy( thisPortList->hostAddr, laddr, LONG_NAME_LEN );
        }
    }
    free( laddr );
    thisPortList->portNum = lport;
    thisPortList->windowSize = rsComm->windowSize;

    if ( listen( lsock, SOMAXCONN ) < 0 ) {
        rodsLog( LOG_NOTICE,
                 "setupSrvPortal: listen failed, errno: %d",
                 errno );
        return SYS_SOCK_LISTEN_ERR;
    }

    return lsock;
}

int
acceptSrvPortal( rsComm_t *rsComm, portList_t *thisPortList ) {
    const int sockfd = getTcpSockFromPortList( thisPortList );
    const int nfds = sockfd + 1;
    fd_set basemask;
    struct timeval selectTimeout;
    int nSelected = 0;

    selectTimeout.tv_sec = SELECT_TIMEOUT_FOR_CONN;
    selectTimeout.tv_usec = 0;
    while ( true ) {
        FD_ZERO( &basemask );
        FD_SET( sockfd, &basemask );
        nSelected = select( nfds, &basemask, ( fd_set * ) NULL, ( fd_set * ) NULL, &selectTimeout );
        if ( nSelected < 0 ) {
            if ( errno == EINTR || errno == EAGAIN ) {
                rodsLog( LOG_ERROR, "acceptSrvPortal: select interrupted, errno = %d", errno );
            }
            else {
                rodsLog( LOG_ERROR, "acceptSrvPortal: select failed, errno = %d", errno );
                return SYS_SOCK_SELECT_ERR - errno;
            }
        }
        else {
            break;
        }
    }
    if ( nSelected == 0 ) {
        rodsLog( LOG_ERROR, "acceptSrvPortal() -- select timed out" );
        return SYS_SOCK_SELECT_ERR - errno;
    }

    const int saved_socket_flags = fcntl( sockfd, F_GETFL );
    fcntl( sockfd, F_SETFL, saved_socket_flags | O_NONBLOCK );
    const int myFd = accept( sockfd, 0, 0 );
    fcntl( sockfd, F_SETFL, saved_socket_flags );

    if ( myFd < 0 ) {
        rodsLog( LOG_NOTICE,
                 "acceptSrvPortal() -- accept() failed: errno=%d",
                 errno );
        return SYS_SOCK_ACCEPT_ERR - errno;
    }
    else {
        rodsSetSockOpt( myFd, rsComm->windowSize );
    }

    int myCookie;
    int nbytes = read( myFd, &myCookie, sizeof( myCookie ) );
    myCookie = ntohl( myCookie );
    if ( nbytes != sizeof( myCookie ) || myCookie != thisPortList->cookie ) {
        rodsLog( LOG_NOTICE,
                 "acceptSrvPortal: cookie err, bytes read=%d,cookie=%d,inCookie=%d",
                 nbytes, thisPortList->cookie, myCookie );
        CLOSE_SOCK( myFd );
        return SYS_PORT_COOKIE_ERR;
    }
    return myFd;
}

int applyRuleForSvrPortal( int sockFd, int oprType, int preOrPost, int load, rsComm_t *rsComm ) {
    typedef union address {
        struct sockaddr    sa;
        struct sockaddr_in sa_in;
    } address_t;

    address_t local, peer;
    socklen_t local_len;
    memset( &local, 0, sizeof( local ) );
    memset( &peer, 0, sizeof( peer ) );
    local_len = sizeof( struct sockaddr );
    int status = getsockname( sockFd, &local.sa, &local_len );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "applyRuleForSvrPortal: acceptSrvPortal error. errno = %d", errno );
        return SYS_SOCK_READ_ERR - errno;
    }
    local_len = sizeof( struct sockaddr );
    status = getpeername( sockFd, &peer.sa, &local_len );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR, "applyRuleForSvrPortal: acceptSrvPortal error. errno = %d", errno );
        return SYS_SOCK_READ_ERR - errno;
    }
    char lPort[MAX_NAME_LEN];
    char pPort[MAX_NAME_LEN];
    char lLoad[MAX_NAME_LEN];
    char oType[MAX_NAME_LEN];
    snprintf( oType, MAX_NAME_LEN, "%d", oprType );
    snprintf( lLoad, MAX_NAME_LEN, "%d", load );
    char *lAddr = strdup( inet_ntoa( local.sa_in.sin_addr ) );
    int localPort = ntohs( local.sa_in.sin_port );
    snprintf( lPort, MAX_NAME_LEN, "%d", localPort );
    char *pAddr = strdup( inet_ntoa( peer.sa_in.sin_addr ) );
    int peerPort = ntohs( peer.sa_in.sin_port );
    snprintf( pPort, MAX_NAME_LEN, "%d", peerPort );
    const char *args[6] = {oType, lAddr, lPort, pAddr, pPort, lLoad};
    ruleExecInfo_t rei;
    memset( &rei, 0, sizeof( rei ) );
    rei.rsComm = rsComm;
    int ret = applyRuleArg( ( char * )( preOrPost == 0 ? "acPreProcForServerPortal" : "acPostProcForServerPortal" ), args, 6, &rei,
                            0 );
    free( lAddr );
    free( pAddr );
    return ret;
}


int
svrPortalPutGet( rsComm_t *rsComm ) {
    portalOpr_t *myPortalOpr;
    dataOprInp_t *dataOprInp;
    portList_t *thisPortList;
    rodsLong_t size0, size1, offset0;
    int lsock, portalFd;
    int i;
    int numThreads;
    portalTransferInp_t myInput[MAX_NUM_CONFIG_TRAN_THR];
    boost::shared_ptr<boost::thread> tid[MAX_NUM_CONFIG_TRAN_THR];
    int oprType;
    int flags = 0;
    int retVal = 0;

    myPortalOpr = rsComm->portalOpr;

    if ( myPortalOpr == NULL ) {
        rodsLog( LOG_NOTICE, "svrPortalPut: NULL myPortalOpr" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    thisPortList = &myPortalOpr->portList;
    if ( thisPortList == NULL ) {
        rodsLog( LOG_NOTICE, "svrPortalPut: NULL portList" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    oprType = myPortalOpr->oprType;
    dataOprInp = &myPortalOpr->dataOprInp;

    if ( getValByKey( &dataOprInp->condInput, STREAMING_KW ) != NULL ) {
        flags |= STREAMING_FLAG;
    }

    numThreads = dataOprInp->numThreads;

    if ( numThreads <= 0 || numThreads > MAX_NUM_CONFIG_TRAN_THR ) {
        rodsLog( LOG_NOTICE,
                 "svrPortalPut: numThreads %d out of range" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    memset( myInput, 0, sizeof( myInput ) );

    size0 = dataOprInp->dataSize / numThreads;

    size1 = dataOprInp->dataSize - size0 * ( numThreads - 1 );
    offset0 = dataOprInp->offset;

    lsock = getTcpSockFromPortList( thisPortList );

    /* accept the first connection */
    portalFd = acceptSrvPortal( rsComm, thisPortList );
    if ( portalFd < 0 ) {
        rodsLog( LOG_NOTICE,
                 "svrPortalPut: acceptSrvPortal error. errno = %d",
                 errno );

        CLOSE_SOCK( lsock );

        return portalFd;
    }
    applyRuleForSvrPortal( portalFd, oprType, 0, size0, rsComm );

    if ( oprType == PUT_OPR ) {
        fillPortalTransferInp( &myInput[0], rsComm,
                               portalFd, dataOprInp->destL3descInx, 0, dataOprInp->destRescTypeInx,
                               0, size0, offset0, flags );
    }
    else {
        fillPortalTransferInp( &myInput[0], rsComm,
                               dataOprInp->srcL3descInx, portalFd, dataOprInp->srcRescTypeInx, 0,
                               0, size0, offset0, flags );
    }

    if ( numThreads == 1 ) {
        if ( oprType == PUT_OPR ) {
            partialDataPut( &myInput[0] );
        }
        else {
            partialDataGet( &myInput[0] );
        }

        CLOSE_SOCK( lsock );

        return myInput[0].status;
    }
    else {
        rodsLong_t mySize = 0;
        rodsLong_t myOffset = 0;

        for ( i = 1; i < numThreads; i++ ) {
            int l3descInx;

            portalFd = acceptSrvPortal( rsComm, thisPortList );
            if ( portalFd < 0 ) {
                rodsLog( LOG_NOTICE,
                         "svrPortalPut: acceptSrvPortal error. errno = %d",
                         errno );

                CLOSE_SOCK( lsock );

                return portalFd;
            }

            myOffset += size0;

            if ( i < numThreads - 1 ) {
                mySize = size0;
            }
            else {
                mySize = size1;
            }

            applyRuleForSvrPortal( portalFd, oprType, 0, mySize, rsComm );

            if ( oprType == PUT_OPR ) {
                /* open the file */
                l3descInx = l3OpenByHost( rsComm, dataOprInp->destL3descInx, O_WRONLY );
                fillPortalTransferInp( &myInput[i], rsComm,
                                       portalFd, l3descInx, 0,
                                       dataOprInp->destRescTypeInx,
                                       i, mySize, myOffset, flags );
                tid[i].reset( new boost::thread( partialDataPut, &myInput[i] ) );

            }
            else {	/* a get */
                l3descInx = l3OpenByHost( rsComm, dataOprInp->srcL3descInx, O_RDONLY );
                fillPortalTransferInp( &myInput[i], rsComm,
                                       l3descInx, portalFd, dataOprInp->srcRescTypeInx, 0,
                                       i, mySize, myOffset, flags );
                tid[i].reset( new boost::thread( partialDataGet, &myInput[i] ) );
            }
        } // for i

        /* spawn the first thread. do this last so the file will not be
        * closed */
        if ( oprType == PUT_OPR ) {
            tid[0].reset( new boost::thread( partialDataPut, &myInput[0] ) );
        }
        else {
            tid[0].reset( new boost::thread( partialDataGet, &myInput[0] ) );
        }

        for ( i = 0; i < numThreads; i++ ) {
            if ( tid[i] != 0 ) {
                tid[i]->join();
            }
            if ( myInput[i].status < 0 ) {
                retVal = myInput[i].status;
            }
        } // for i
        CLOSE_SOCK( lsock );
        return retVal;

    } // else
}

int fillPortalTransferInp(
    portalTransferInp_t* myInput,
    rsComm_t*            rsComm,
    int                  srcFd,
    int                  destFd,
    int                  srcRescTypeInx,
    int                  destRescTypeInx,
    int                  threadNum,
    rodsLong_t           size,
    rodsLong_t           offset,
    int                  flags ) {

    if ( myInput == NULL ) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    myInput->rsComm          = rsComm;
    myInput->destFd          = destFd;
    myInput->srcFd           = srcFd;
    myInput->destRescTypeInx = destRescTypeInx;
    myInput->srcRescTypeInx  = srcRescTypeInx;
    myInput->threadNum       = threadNum;
    myInput->size            = size;
    myInput->offset          = offset;
    myInput->flags           = flags;

    // =-=-=-=-=-=-=-
    // copy the encryption key over to the
    // portal input
    memcpy(
        myInput->shared_secret,
        rsComm->shared_secret,
        NAME_LEN );

    // =-=-=-=-=-=-=-
    // copy the encryption environment over to the
    // portal input
    myInput->key_size        = rsComm->key_size;
    myInput->salt_size       = rsComm->salt_size;
    myInput->num_hash_rounds = rsComm->num_hash_rounds;
    snprintf( myInput->encryption_algorithm, sizeof( myInput->encryption_algorithm ),
              "%s", rsComm->encryption_algorithm );
    return 0;
}

static void partialDataPut_impl(portalTransferInp_t* myInput)
{
    int destL3descInx = 0, srcFd = 0;
    unsigned char *buf = 0;
    int bytesWritten = 0;
    rodsLong_t bytesToGet = 0;
    rodsLong_t myOffset = 0;

    if ( myInput == NULL ) {
        rodsLog( LOG_SYS_FATAL, "partialDataPut: NULL myInput" );
        return;
    }

    // =-=-=-=-=-=-=-
    // flag to determine if we need to use encryption
    bool use_encryption_flg =
        ( myInput->rsComm->negotiation_results ==
          irods::CS_NEG_USE_SSL );

    myInput->status = 0;
    destL3descInx = myInput->destFd;
    srcFd = myInput->srcFd;

    if ( myInput->offset != 0 ) {
        myOffset = _l3Lseek( myInput->rsComm, destL3descInx,
                             myInput->offset, SEEK_SET );
        if ( myOffset < 0 ) {
            myInput->status = myOffset;
            rodsLog( LOG_NOTICE,
                     "_partialDataPut: _objSeek error, status = %d ",
                     myInput->status );
            if ( myInput->threadNum > 0 ) {
                _l3Close( myInput->rsComm, destL3descInx );
            }
            CLOSE_SOCK( srcFd );
            return;
        }
    }

    bytesToGet = myInput->size;

    // =-=-=-=-=-=-=-
    // create an encryption context, initialization vector
    int iv_size = 0;
    irods::buffer_crypt::array_t iv;
    irods::buffer_crypt::array_t this_iv;
    irods::buffer_crypt::array_t cipher;
    irods::buffer_crypt::array_t plain;
    irods::buffer_crypt::array_t shared_secret;
    irods::buffer_crypt crypt(
        myInput->key_size,
        myInput->salt_size,
        myInput->num_hash_rounds,
        myInput->encryption_algorithm );

    // =-=-=-=-=-=-=-
    // compute an iv to determine how large it
    // is for this implementation
    if ( use_encryption_flg ) {
        iv_size = crypt.key_size();
        shared_secret.assign(
            &myInput->shared_secret[0],
            &myInput->shared_secret[iv_size] );
    }

    int chunk_size;
    try {
        chunk_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_CHUNK_SIZE_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    int trans_buff_size = 0;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    buf = ( unsigned char* )malloc( ( 2 * trans_buff_size ) + sizeof( unsigned char ) );

    while ( bytesToGet > 0 ) {
        int toread0;
        int bytesRead;

        if ( myInput->flags & STREAMING_FLAG ) {
            toread0 = bytesToGet;
        }
        else if ( bytesToGet > chunk_size ) {
            toread0 = chunk_size;
        }
        else {
            toread0 = bytesToGet;
        }

        myInput->status = sendTranHeader( srcFd, PUT_OPR, myInput->flags,
                                          myOffset, toread0 );

        if ( myInput->status < 0 ) {
            rodsLog( LOG_NOTICE,
                     "partialDataPut: sendTranHeader error. status = %d",
                     myInput->status );
            if ( myInput->threadNum > 0 ) {
                _l3Close( myInput->rsComm, destL3descInx );
            }
            CLOSE_SOCK( srcFd );
            free( buf );
            return;
        }

        while ( toread0 > 0 ) {
            int toread1 = 0;

            if ( toread0 > trans_buff_size ) {
                toread1 = trans_buff_size;
            }
            else {
                toread1 = toread0;
            }

            // =-=-=-=-=-=-=-
            // read the incoming size as it might differ due to encryption
            int new_size = toread1;
            if ( use_encryption_flg ) {
                bytesRead = myRead(
                                srcFd,
                                &new_size,
                                sizeof( int ),
                                NULL, NULL );
                if ( bytesRead != sizeof( int ) ) {
                    rodsLog( LOG_ERROR, "_partialDataPut:Bytes Read != %d", sizeof( int ) );
                    break;
                }
            }

            // =-=-=-=-=-=-=-
            // now read the provided number of bytes as suggested by the incoming size
            bytesRead = myRead(
                            srcFd,
                            buf,
                            new_size,
                            NULL, NULL );

            if ( bytesRead == new_size ) {
                // =-=-=-=-=-=-=-
                // if using encryption, strip off the iv
                // and decrypt before writing
                int plain_size = bytesRead;
                if ( use_encryption_flg ) {
                    this_iv.assign(
                        &buf[0],
                        &buf[ iv_size ] );
                    if (iv_size > new_size) {
                        log_server::error("{}: Unable to extract cipher to decrypt data: Start position ({}) comes "
                                          "after end position ({}). This is likely due to mismatched client-server "
                                          "negotiation settings resulting in one-sided encrypted communications.",
                                          __func__,
                                          iv_size,
                                          new_size);
                        myInput->status = SYS_SOCK_READ_ERR;
                        break;
                    }
                    cipher.assign(
                        &buf[ iv_size ],
                        &buf[ new_size ] );
                    irods::error ret = crypt.decrypt(
                                           shared_secret,
                                           this_iv,
                                           cipher,
                                           plain );
                    if ( !ret.ok() ) {
                        irods::log( PASS( ret ) );
                        myInput->status = SYS_COPY_LEN_ERR;
                        break;
                    }

                    std::copy(
                        plain.begin(),
                        plain.end(),
                        &buf[0] );
                    plain_size = plain.size();

                }

                if ( ( bytesWritten = _l3Write(
                                          myInput->rsComm,
                                          destL3descInx,
                                          buf,
                                          plain_size ) ) != ( plain_size ) ) {
                    rodsLog( LOG_NOTICE,
                             "_partialDataPut:Bytes written %d don't match read %d",
                             bytesWritten, bytesRead );

                    if ( bytesWritten < 0 ) {
                        myInput->status = bytesWritten;
                    }
                    else {
                        myInput->status = SYS_COPY_LEN_ERR;
                    }
                    break;
                }
                bytesToGet -= bytesWritten;
                toread0    -= bytesWritten;
                myOffset   += bytesWritten;

            }
            else if ( bytesRead < 0 ) {
                myInput->status = bytesRead;
                break;
            }
            else {          /* toread > 0 */
                rodsLog( LOG_NOTICE,
                         "_partialDataPut: toread %d bytes, %d bytes read, errno = %d",
                         toread1, bytesRead, errno );
                myInput->status = SYS_COPY_LEN_ERR;
                break;
            }

        }	/* while loop toread0 */
        if ( myInput->status < 0 ) {
            break;
        }
    }           /* while loop bytesToGet */

    free( buf );

    applyRuleForSvrPortal( srcFd, PUT_OPR, 1, myOffset - myInput->offset, myInput->rsComm );

    sendTranHeader( srcFd, DONE_OPR, 0, 0, 0 );
    if ( myInput->threadNum > 0 ) {
        _l3Close( myInput->rsComm, destL3descInx );
    }
    mySockClose( srcFd );

    return;
} // partialDataPut_impl

void partialDataPut(portalTransferInp_t* myInput)
{
    // This function acts as a wrapper to ensure that all exceptions are being caught while minimizing changes to this
    // legacy implementation of parallel transfer.
    try {
        partialDataPut_impl(myInput);
    }
    catch (const irods::exception& e) {
        log_server::error("{}: Caught irods::exception: {}", __func__, e.client_display_what());
        myInput->status = static_cast<int>(e.code());
    }
    catch (const std::exception& e) {
        log_server::error("{}: Caught std::exception: {}", __func__, e.what());
        myInput->status = SYS_LIBRARY_ERROR;
    }
} // partialDataPut

static void partialDataGet_impl(portalTransferInp_t* myInput)
{
    // =-=-=-=-=-=-=-
    //
    int srcL3descInx = 0, destFd = 0;
    int bytesWritten = 0;
    rodsLong_t bytesToGet = 0;
    rodsLong_t myOffset = 0;

    if ( myInput == NULL ) {
        rodsLog( LOG_SYS_FATAL, "partialDataGet: NULL myInput" );
        return;
    }

    myInput->status = 0;
    srcL3descInx = myInput->srcFd;
    destFd = myInput->destFd;

    if ( myInput->offset != 0 ) {
        myOffset = _l3Lseek( myInput->rsComm, srcL3descInx,
                             myInput->offset, SEEK_SET );
        if ( myOffset < 0 ) {
            myInput->status = myOffset;
            rodsLog( LOG_NOTICE,
                     "_partialDataGet: _objSeek error, status = %d ",
                     myInput->status );
            if ( myInput->threadNum > 0 ) {
                _l3Close( myInput->rsComm, srcL3descInx );
            }
            CLOSE_SOCK( destFd );
            return;
        }
    }

    // =-=-=-=-=-=-=-
    // flag to determine if we need to use encryption
    bool use_encryption_flg =
        ( myInput->rsComm->negotiation_results ==
          irods::CS_NEG_USE_SSL );

    // =-=-=-=-=-=-=-
    // create an encryption context
    int iv_size = 0;
    irods::buffer_crypt::array_t iv;
    irods::buffer_crypt::array_t cipher;
    irods::buffer_crypt::array_t in_buf;
    irods::buffer_crypt::array_t shared_secret;
    irods::buffer_crypt crypt(
        myInput->key_size,
        myInput->salt_size,
        myInput->num_hash_rounds,
        myInput->encryption_algorithm );

    // =-=-=-=-=-=-=-
    // set iv size
    if ( use_encryption_flg ) {
        iv_size = crypt.key_size();
        shared_secret.assign(
            &myInput->shared_secret[0],
            &myInput->shared_secret[iv_size] );
    }

    int trans_buff_size = 0;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    size_t buf_size = ( 2 * trans_buff_size ) * sizeof( unsigned char ) ;
    unsigned char * buf = ( unsigned char* )malloc( buf_size );

    bytesToGet = myInput->size;

    int chunk_size;
    try {
        chunk_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_CHUNK_SIZE_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    while ( bytesToGet > 0 ) {
        int toread0;
        int bytesRead;

        if ( myInput->flags & STREAMING_FLAG ) {
            toread0 = bytesToGet;
        }
        else if ( bytesToGet > chunk_size ) {
            toread0 = chunk_size;
        }
        else {
            toread0 = bytesToGet;
        }

        myInput->status = sendTranHeader( destFd, GET_OPR, myInput->flags,
                                          myOffset, toread0 );

        if ( myInput->status < 0 ) {
            rodsLog( LOG_NOTICE,
                     "partialDataGet: sendTranHeader error. status = %d",
                     myInput->status );
            if ( myInput->threadNum > 0 ) {
                _l3Close( myInput->rsComm, srcL3descInx );
            }
            CLOSE_SOCK( destFd );
            free( buf );
            return;
        }

        while ( toread0 > 0 ) {
            int toread1;

            if ( toread0 > trans_buff_size ) {
                toread1 = trans_buff_size;
            }
            else {
                toread1 = toread0;
            }

            bytesRead = _l3Read( myInput->rsComm, srcL3descInx, buf, toread1 );


            if ( bytesRead == toread1 ) {
                // =-=-=-=-=-=-=-
                // compute an iv for this particular transmission and use
                // it to encrypt this buffer
                int new_size = bytesRead;
                if ( use_encryption_flg ) {
                    irods::error ret = crypt.initialization_vector( iv );
                    if ( !ret.ok() ) {
                        ret = PASS( ret );
                        printf( "%s", ret.result().c_str() );
                        break;
                    }

                    // =-=-=-=-=-=-=-
                    // encrypt
                    in_buf.assign(
                        &buf[0],
                        &buf[ bytesRead ] );

                    ret = crypt.encrypt(
                              shared_secret,
                              iv,
                              in_buf,
                              cipher );
                    if ( !ret.ok() ) {
                        ret = PASS( ret );
                        printf( "%s", ret.result().c_str() );
                        break;
                    }

                    // =-=-=-=-=-=-=-
                    // capture the iv with the cipher text
                    memset( buf, 0,  buf_size );
                    std::copy(
                        iv.begin(),
                        iv.end(),
                        &buf[0] );
                    std::copy(
                        cipher.begin(),
                        cipher.end(),
                        &buf[iv_size] );

                    new_size = iv_size + cipher.size();

                    // =-=-=-=-=-=-=-
                    // need to send the incoming size as encryption might change
                    // the size of the data from the written values
                    bytesWritten = myWrite(
                                       destFd,
                                       &new_size,
                                       sizeof( int ),
                                       &bytesWritten );
                }

                // =-=-=-=-=-=-=-
                // then write the actual buffer
                bytesWritten = myWrite(
                                   destFd,
                                   buf,
                                   new_size,
                                   &bytesWritten );

                if ( bytesWritten != new_size ) {
                    rodsLog( LOG_NOTICE,
                             "_partialDataGet:Bytes written %d don't match read %d",
                             bytesWritten, bytesRead );

                    if ( bytesWritten < 0 ) {
                        myInput->status = bytesWritten;
                    }
                    else {
                        myInput->status = SYS_COPY_LEN_ERR;
                    }
                    break;
                }

                // =-=-=-=-=-=-=-
                // had to change to bytesRead as bytesWritten
                // may have changed due to encryption
                bytesToGet -= bytesRead;
                toread0    -= bytesRead;
                myOffset   += bytesRead;

            }
            else if ( bytesRead < 0 ) {
                myInput->status = bytesRead;
                break;

            }
            else {          /* toread > 0 */
                rodsLog( LOG_NOTICE,
                         "_partialDataGet: toread %d bytes, %d bytes read",
                         toread1, bytesRead );
                myInput->status = SYS_COPY_LEN_ERR;
                break;
            }
        }       /* while loop toread0 */
        if ( myInput->status < 0 ) {
            break;
        }
    }           /* while loop bytesToGet */

    free( buf );

    applyRuleForSvrPortal( destFd, GET_OPR, 1, myOffset - myInput->offset, myInput->rsComm );

    sendTranHeader( destFd, DONE_OPR, 0, 0, 0 );
    if ( myInput->threadNum > 0 ) {
        _l3Close( myInput->rsComm, srcL3descInx );
    }
    CLOSE_SOCK( destFd );

    return;
} // partialDataGet_impl

void partialDataGet(portalTransferInp_t* myInput)
{
    // This function acts as a wrapper to ensure that all exceptions are being caught while minimizing changes to this
    // legacy implementation of parallel transfer.
    try {
        partialDataGet_impl(myInput);
    }
    catch (const irods::exception& e) {
        log_server::error("{}: Caught irods::exception: {}", __func__, e.client_display_what());
        myInput->status = static_cast<int>(e.code());
    }
    catch (const std::exception& e) {
        log_server::error("{}: Caught std::exception: {}", __func__, e.what());
        myInput->status = SYS_LIBRARY_ERROR;
    }
} // partialDataGet

void
remToLocPartialCopy( portalTransferInp_t *myInput ) {
    transferHeader_t myHeader;
    int destL3descInx = 0, srcFd = 0;
    unsigned char *buf = 0;
    rodsLong_t curOffset = 0;
    rodsLong_t myOffset = 0;
    int toRead, bytesRead = 0, bytesWritten = 0;

    if ( myInput == NULL ) {
        rodsLog( LOG_NOTICE,
                 "remToLocPartialCopy: NULL input" );
        return;
    }

    myInput->status = 0;
    destL3descInx = myInput->destFd;
    srcFd = myInput->srcFd;
    myInput->bytesWritten = 0;

    // =-=-=-=-=-=-=-
    // flag to determine if we need to use encryption
    bool use_encryption_flg =
        ( myInput->rsComm->negotiation_results ==
          irods::CS_NEG_USE_SSL );

    // =-=-=-=-=-=-=-
    // create an encryption context, initialization vector
    int iv_size = 0;
    irods::buffer_crypt::array_t iv;
    irods::buffer_crypt::array_t this_iv;
    irods::buffer_crypt::array_t cipher;
    irods::buffer_crypt::array_t plain;
    irods::buffer_crypt::array_t shared_secret;
    irods::buffer_crypt crypt(
        myInput->key_size,
        myInput->salt_size,
        myInput->num_hash_rounds,
        myInput->encryption_algorithm );

    // =-=-=-=-=-=-=-
    // compute an iv to determine how large it
    // is for this implementation
    if ( use_encryption_flg ) {
        iv_size = crypt.key_size();
        shared_secret.assign(
            &myInput->shared_secret[0],
            &myInput->shared_secret[iv_size] );
    }

    int trans_buff_size;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    buf = ( unsigned char* )malloc( ( 2 * trans_buff_size ) * sizeof( unsigned char ) );

    while ( myInput->status >= 0 ) {
        rodsLong_t toGet;

        myInput->status = rcvTranHeader( srcFd, &myHeader );

        if ( myInput->status < 0 ) {
            break;
        }

        if ( myHeader.oprType == DONE_OPR ) {
            break;
        }
        if ( myHeader.offset != curOffset ) {
            curOffset = myHeader.offset;
            myOffset = _l3Lseek( myInput->rsComm, destL3descInx,
                                 myHeader.offset, SEEK_SET );
            if ( myOffset < 0 ) {
                myInput->status = myOffset;
                rodsLog( LOG_NOTICE,
                         "remToLocPartialCopy: _objSeek error, status = %d ",
                         myInput->status );
                break;
            }
        }

        toGet = myHeader.length;
        while ( toGet > 0 ) {

            if ( toGet > trans_buff_size ) {
                toRead = trans_buff_size;
            }
            else {
                toRead = toGet;
            }

            // =-=-=-=-=-=-=-
            // read the incoming size as it might differ due to encryption
            int new_size = toRead;
            if ( use_encryption_flg ) {
                bytesRead = myRead( srcFd, &new_size, sizeof( int ), NULL, NULL );
                if ( bytesRead != sizeof( int ) ) {
                    rodsLog( LOG_ERROR, "_partialDataPut:Bytes Read != %d", sizeof( int ) );
                    break;
                }
            }

            // =-=-=-=-=-=-=-
            // now read the provided number of bytes as suggested by the incoming size
            bytesRead = myRead( srcFd, buf, new_size, NULL, NULL );
            if ( bytesRead != new_size ) {
                if ( bytesRead < 0 ) {
                    myInput->status = bytesRead;
                    rodsLogError( LOG_ERROR, bytesRead,
                                  "remToLocPartialCopy: copy error for %lld", bytesRead );
                }
                else if ( ( myInput->flags & NO_CHK_COPY_LEN_FLAG ) == 0 ) {
                    myInput->status = SYS_COPY_LEN_ERR - errno;
                    rodsLog( LOG_ERROR,
                             "remToLocPartialCopy: toGet %lld, bytesRead %d",
                             toGet, bytesRead );
                }
                break;
            }

            // =-=-=-=-=-=-=-
            // if using encryption, strip off the iv
            // and decrypt before writing
            int plain_size = bytesRead;
            if ( use_encryption_flg ) {
                this_iv.assign(
                    &buf[ 0 ],
                    &buf[ iv_size ] );
                if (iv_size > new_size) {
                    log_server::error("{}: Unable to extract cipher to decrypt data: Start position ({}) comes after "
                                      "end position ({}). This is likely due to mismatched client-server negotiation "
                                      "settings resulting in one-sided encrypted communications.",
                                      __func__,
                                      iv_size,
                                      new_size);
                    myInput->status = SYS_SOCK_READ_ERR;
                    break;
                }
                cipher.assign(
                    &buf[ iv_size ],
                    &buf[ new_size ] );

                irods::error ret = crypt.decrypt(
                                       shared_secret,
                                       this_iv,
                                       cipher,
                                       plain );
                if ( !ret.ok() ) {
                    irods::log( PASS( ret ) );
                    myInput->status = SYS_COPY_LEN_ERR;
                    break;
                }

                std::copy(
                    plain.begin(),
                    plain.end(),
                    &buf[0] );
                plain_size = plain.size();

            }

            bytesWritten = _l3Write(
                               myInput->rsComm,
                               destL3descInx,
                               buf,
                               plain_size );

            if ( bytesWritten != plain_size ) {
                rodsLog( LOG_NOTICE,
                         "_partialDataPut:Bytes written %d don't match read %d",
                         bytesWritten, bytesRead );

                if ( bytesWritten < 0 ) {
                    myInput->status = bytesWritten;
                }
                else {
                    myInput->status = SYS_COPY_LEN_ERR;
                }
                break;
            }

            toGet -= bytesWritten;
        }
        curOffset += myHeader.length;
        myInput->bytesWritten += myHeader.length;
    }

    free( buf );
    if ( myInput->threadNum > 0 ) {
        _l3Close( myInput->rsComm, destL3descInx );
    }
    CLOSE_SOCK( srcFd );
}

/* remLocCopy - This routine is very similar to rcPartialDataGet.
 */

int remLocCopy(rsComm_t *rsComm, dataCopyInp_t *dataCopyInp)
{
    int retVal = 0;
    if (!dataCopyInp) {
        rodsLog(LOG_NOTICE, "%s: NULL dataCopyInp input", __FUNCTION__);
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    portalOprOut_t* portalOprOut = &dataCopyInp->portalOprOut;
    const int numThreads = portalOprOut->numThreads;
    if (0 == numThreads) {
        return singleRemLocCopy(rsComm, dataCopyInp);
    }

    dataOprInp_t *dataOprInp = &dataCopyInp->dataOprInp;
    int oprType = dataOprInp->oprType;
    rodsLong_t dataSize = dataOprInp->dataSize;

    if ( numThreads > MAX_NUM_CONFIG_TRAN_THR || numThreads <= 0 ) {
        rodsLog(LOG_NOTICE,
                "%s: numThreads %d out of range",
                __FUNCTION__, numThreads);
        return SYS_INVALID_PORTAL_OPR;
    }

    portList_t *myPortList = &portalOprOut->portList;

    portalTransferInp_t myInput[MAX_NUM_CONFIG_TRAN_THR]{};

    int sock = connectToRhostPortal( myPortList->hostAddr,
                                 myPortList->portNum, myPortList->cookie, rsComm->windowSize );
    if ( sock < 0 ) {
        return sock;
    }

    if ( oprType == COPY_TO_LOCAL_OPR ) {
        fillPortalTransferInp( &myInput[0], rsComm,
                               sock, dataOprInp->destL3descInx, 0, dataOprInp->destRescTypeInx,
                               0, 0, 0, 0 );
    }
    else {
        fillPortalTransferInp( &myInput[0], rsComm,
                               dataOprInp->srcL3descInx, sock, dataOprInp->srcRescTypeInx, 0,
                               0, 0, 0, 0 );
    }

    if (1 == numThreads) {
        if (getValByKey(&dataOprInp->condInput, NO_CHK_COPY_LEN_KW)) {
            myInput[0].flags = NO_CHK_COPY_LEN_FLAG;
        }

        if (COPY_TO_LOCAL_OPR == oprType) {
            remToLocPartialCopy(&myInput[0]);
        }
        else {
            locToRemPartialCopy(&myInput[0]);
        }

        if (myInput[0].status < 0) {
            return myInput[0].status;
        }

        if (dataSize != myInput[0].bytesWritten) {
            rodsLog(LOG_NOTICE,
                    "[%s:%d]:bytesWritten %lld dataSize %lld mismatch",
                    __FUNCTION__, __LINE__, myInput[0].bytesWritten, dataSize);
            return SYS_COPY_LEN_ERR;
        }
        return 0;
    }

    rodsLong_t totalWritten = 0;
    std::unique_ptr<boost::scoped_thread<>> tid[MAX_NUM_CONFIG_TRAN_THR];
    memset( tid, 0, sizeof( tid ) );
    for (int i = 1; i < numThreads; i++) {
        sock = connectToRhostPortal( myPortList->hostAddr,
                                     myPortList->portNum, myPortList->cookie, rsComm->windowSize );
        if (sock < 0) {
            return sock;
        }

        if (COPY_TO_LOCAL_OPR == oprType) {
            const int myFd = l3OpenByHost( rsComm, dataOprInp->destL3descInx, O_WRONLY );
            if ( myFd < 0 ) {  /* error */
                retVal = myFd;
                rodsLog(LOG_NOTICE,
                        "%s: cannot open file, status = %d",
                        __FUNCTION__, myFd);
                CLOSE_SOCK( sock );
                continue;
            }

            fillPortalTransferInp( &myInput[i], rsComm,
                                   sock, myFd, 0, dataOprInp->destRescTypeInx,
                                   i, 0, 0, 0 );

            tid[i] = std::make_unique<boost::scoped_thread<>>( boost::thread( remToLocPartialCopy, &myInput[i] ) );
        }
        else {
            const int myFd = l3OpenByHost( rsComm, dataOprInp->srcL3descInx, O_RDONLY );
            if ( myFd < 0 ) {  /* error */
                retVal = myFd;
                rodsLog(LOG_NOTICE,
                        "%s: cannot open file, status = %d",
                        __FUNCTION__, myFd );
                CLOSE_SOCK( sock );
                continue;
            }

            fillPortalTransferInp( &myInput[i], rsComm,
                                   myFd, sock, dataOprInp->destRescTypeInx, 0,
                                   i, 0, 0, 0 );

            tid[i] = std::make_unique<boost::scoped_thread<>>( boost::thread( locToRemPartialCopy, &myInput[i] ) );
        }
    }

    if (COPY_TO_LOCAL_OPR == oprType) {
        tid[0] = std::make_unique<boost::scoped_thread<>>( boost::thread( remToLocPartialCopy, &myInput[0] ) );
    }
    else {
        tid[0] = std::make_unique<boost::scoped_thread<>>( boost::thread( locToRemPartialCopy, &myInput[0] ) );
    }

    if (retVal < 0) {
        return retVal;
    }

    for (int i = 0; i < numThreads; i++) {
        if (tid[i]) {
            tid[i]->join();
        }
        totalWritten += myInput[i].bytesWritten;
        if (myInput[i].status < 0) {
            retVal = myInput[i].status;
        }
    }

    if (retVal < 0) {
        return retVal;
    }

    if (dataSize > 0 && totalWritten != dataSize) {
        rodsLog(LOG_NOTICE,
                "%s: totalWritten %lld dataSize %lld mismatch",
                __FUNCTION__, totalWritten, dataSize );
        return SYS_COPY_LEN_ERR;
    }

    return 0;
} // remLocCopy

int
sameHostCopy( rsComm_t *rsComm, dataCopyInp_t *dataCopyInp ) {
    dataOprInp_t *dataOprInp;
    int i, out_fd, in_fd;
    int numThreads;
    portalTransferInp_t myInput[MAX_NUM_CONFIG_TRAN_THR];
    int retVal = 0;
    rodsLong_t dataSize;
    rodsLong_t size0, size1, offset0;

    if ( dataCopyInp == NULL ) {
        rodsLog( LOG_NOTICE,
                 "sameHostCopy: NULL dataCopyInp input" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    dataOprInp = &dataCopyInp->dataOprInp;

    numThreads = dataOprInp->numThreads;

    dataSize = dataOprInp->dataSize;

    if ( numThreads == 0 ) {
        numThreads = 1;
    }
    else if ( numThreads > MAX_NUM_CONFIG_TRAN_THR || numThreads < 0 ) {
        rodsLog( LOG_NOTICE,
                 "sameHostCopy: numThreads %d out of range",
                 numThreads );
        return SYS_INVALID_PORTAL_OPR;
    }

    memset( myInput, 0, sizeof( myInput ) );

    size0 = dataOprInp->dataSize / numThreads;
    size1 = dataOprInp->dataSize - size0 * ( numThreads - 1 );
    offset0 = dataOprInp->offset;

    // =-=-=-=-=-=-=-
    // JMC :: since this is a local to local xfer and there is no
    //     :: cookie to share it is set to 0, this may *possibly* be
    //     :: a security issue.
    fillPortalTransferInp( &myInput[0], rsComm,
                           dataOprInp->srcL3descInx, dataOprInp->destL3descInx,
                           dataOprInp->srcRescTypeInx, dataOprInp->destRescTypeInx,
                           0, size0, offset0, 0 );

    if ( numThreads == 1 ) {
        if ( getValByKey( &dataOprInp->condInput,
                          NO_CHK_COPY_LEN_KW ) != NULL ) {
            myInput[0].flags = NO_CHK_COPY_LEN_FLAG;
        }
        sameHostPartialCopy( &myInput[0] );
        return myInput[0].status;
    }
    else {
        rodsLong_t totalWritten = 0;
        rodsLong_t mySize = 0;
        rodsLong_t myOffset = 0;
        std::unique_ptr<boost::scoped_thread<>> tid[MAX_NUM_CONFIG_TRAN_THR];
        memset( tid, 0, sizeof( tid ) );

        for ( i = 1; i < numThreads; i++ ) {
            myOffset += size0;
            if ( i < numThreads - 1 ) {
                mySize = size0;
            }
            else {
                mySize = size1;
            }

            rodsLog(LOG_DEBUG, "[%s:%d] - opening dest file with l3descInx [%d]", __FUNCTION__, __LINE__, dataOprInp->destL3descInx);
            out_fd = l3OpenByHost( rsComm, dataOprInp->destL3descInx, O_WRONLY );
            if ( out_fd < 0 ) {  /* error */
                retVal = out_fd;
                rodsLog( LOG_NOTICE,
                         "sameHostCopy: cannot open dest file, status = %d",
                         out_fd );
                continue;
            }

            in_fd = l3OpenByHost( rsComm, dataOprInp->srcL3descInx, O_RDONLY );
            if ( in_fd < 0 ) {  /* error */
                retVal = in_fd;
                rodsLog( LOG_NOTICE,
                         "sameHostCopy: cannot open src file, status = %d", in_fd );
                continue;
            }
            fillPortalTransferInp(
                &myInput[i], rsComm,
                in_fd, out_fd,
                dataOprInp->srcRescTypeInx,
                dataOprInp->destRescTypeInx,
                i, mySize, myOffset, 0 );

            tid[i] = std::make_unique<boost::scoped_thread<>>( boost::thread( sameHostPartialCopy, &myInput[i] ) );
        }

        tid[0] = std::make_unique<boost::scoped_thread<>>( boost::thread( sameHostPartialCopy, &myInput[0] ) );

        if ( retVal < 0 ) {
            return retVal;
        }

        for ( i = 0; i < numThreads; i++ ) {
            if ( tid[i] != 0 ) {
                tid[i]->join();
            }
            totalWritten += myInput[i].bytesWritten;
            if ( myInput[i].status < 0 ) {
                retVal = myInput[i].status;
            }
        }
        if ( retVal < 0 ) {
            return retVal;
        }
        else {
            if ( dataSize <= 0 || totalWritten == dataSize ) {
                return 0;
            }
            else {
                rodsLog( LOG_NOTICE,
                         "sameHostCopy: totalWritten %lld dataSize %lld mismatch",
                         totalWritten, dataSize );
                return SYS_COPY_LEN_ERR;
            }
        }
    }
}

void
sameHostPartialCopy( portalTransferInp_t *myInput ) {
    int destL3descInx, srcL3descInx;
    void *buf;
    rodsLong_t myOffset = 0;
    rodsLong_t toCopy;
    int bytesRead, bytesWritten;

    if ( myInput == NULL ) {
        rodsLog( LOG_NOTICE,
                 "onsameHostPartialCopy: NULL input" );
        return;
    }

    myInput->status = 0;
    destL3descInx = myInput->destFd;
    srcL3descInx = myInput->srcFd;
    myInput->bytesWritten = 0;

    if ( myInput->offset != 0 ) {
        myOffset = _l3Lseek( myInput->rsComm, destL3descInx,
                             myInput->offset, SEEK_SET );
        if ( myOffset < 0 ) {
            myInput->status = myOffset;
            rodsLog( LOG_NOTICE,
                     "sameHostPartialCopy: _objSeek error, status = %d ",
                     myInput->status );
            if ( myInput->threadNum > 0 ) {
                _l3Close( myInput->rsComm, destL3descInx );
                _l3Close( myInput->rsComm, srcL3descInx );
            }
            return;
        }
        myOffset = _l3Lseek( myInput->rsComm, srcL3descInx,
                             myInput->offset, SEEK_SET );
        if ( myOffset < 0 ) {
            myInput->status = myOffset;
            rodsLog( LOG_NOTICE,
                     "sameHostPartialCopy: _objSeek error, status = %d ",
                     myInput->status );
            if ( myInput->threadNum > 0 ) {
                _l3Close( myInput->rsComm, destL3descInx );
                _l3Close( myInput->rsComm, srcL3descInx );
            }
            return;
        }
    }

    int trans_buff_size;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    buf = malloc( trans_buff_size );

    toCopy = myInput->size;

    while ( toCopy > 0 ) {
        int toRead;

        if ( toCopy > trans_buff_size ) {
            toRead = trans_buff_size;
        }
        else {
            toRead = toCopy;
        }

        bytesRead = _l3Read( myInput->rsComm, srcL3descInx, buf, toRead );

        if ( bytesRead <= 0 ) {
            if ( bytesRead < 0 ) {
                myInput->status = bytesRead;
                rodsLogError( LOG_ERROR, bytesRead,
                              "sameHostPartialCopy: copy error for %lld", bytesRead );
            }
            else if ( ( myInput->flags & NO_CHK_COPY_LEN_FLAG ) == 0 ) {
                myInput->status = SYS_COPY_LEN_ERR - errno;
                rodsLog( LOG_ERROR,
                         "sameHostPartialCopy: toCopy %lld, bytesRead %d",
                         toCopy, bytesRead );
            }
            break;
        }

        bytesWritten = _l3Write( myInput->rsComm, destL3descInx,
                                 buf, bytesRead );

        if ( bytesWritten != bytesRead ) {
            rodsLog( LOG_NOTICE,
                     "sameHostPartialCopy:Bytes written %d don't match read %d",
                     bytesWritten, bytesRead );

            if ( bytesWritten < 0 ) {
                myInput->status = bytesWritten;
            }
            else {
                myInput->status = SYS_COPY_LEN_ERR;
            }
            break;
        }

        toCopy -= bytesWritten;
        myInput->bytesWritten += bytesWritten;
    }

    free( buf );
    if ( myInput->threadNum > 0 ) {
        _l3Close( myInput->rsComm, destL3descInx );
        _l3Close( myInput->rsComm, srcL3descInx );
    }
}

void locToRemPartialCopy(portalTransferInp_t *myInput)
{
    transferHeader_t myHeader;
    int srcL3descInx = 0, destFd = 0;
    unsigned char *buf = 0;
    rodsLong_t curOffset = 0;
    rodsLong_t myOffset = 0;
    int toRead = 0, bytesRead = 0, bytesWritten = 0;

    if ( myInput == NULL ) {
        rodsLog( LOG_NOTICE,
                 "locToRemPartialCopy: NULL input" );
        return;
    }

    myInput->status = 0;
    srcL3descInx = myInput->srcFd;
    destFd = myInput->destFd;
    myInput->bytesWritten = 0;

    // =-=-=-=-=-=-=-
    // flag to determine if we need to use encryption
    bool use_encryption_flg =
        ( myInput->rsComm->negotiation_results ==
          irods::CS_NEG_USE_SSL );

    // =-=-=-=-=-=-=-
    // create an encryption context
    int iv_size = 0;
    irods::buffer_crypt::array_t iv;
    irods::buffer_crypt::array_t cipher;
    irods::buffer_crypt::array_t in_buf;
    irods::buffer_crypt::array_t shared_secret;
    irods::buffer_crypt crypt(
        myInput->key_size,
        myInput->salt_size,
        myInput->num_hash_rounds,
        myInput->encryption_algorithm );

    // =-=-=-=-=-=-=-
    // set iv size
    if ( use_encryption_flg ) {
        iv_size = crypt.key_size();
        shared_secret.assign(
            &myInput->shared_secret[0],
            &myInput->shared_secret[iv_size] );

    }

    int trans_buff_size;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return;
    }

    buf = ( unsigned char* )malloc( 2 * trans_buff_size * sizeof( unsigned char ) );

    while ( myInput->status >= 0 ) {
        rodsLong_t toGet;

        myInput->status = rcvTranHeader( destFd, &myHeader );

        if ( myInput->status < 0 ) {
            break;
        }

        if ( myHeader.oprType == DONE_OPR ) {
            break;
        }

        if ( myHeader.offset != curOffset ) {
            curOffset = myHeader.offset;
            myOffset = _l3Lseek( myInput->rsComm, srcL3descInx,
                                 myHeader.offset, SEEK_SET );
            if ( myOffset < 0 ) {
                myInput->status = myOffset;
                rodsLog( LOG_NOTICE,
                         "locToRemPartialCopy: _objSeek error, status = %d ",
                         myInput->status );
                break;
            }
        }

        toGet = myHeader.length;
        while ( toGet > 0 ) {

            if ( toGet > trans_buff_size ) {
                toRead = trans_buff_size;
            }
            else {
                toRead = toGet;
            }

            bytesRead = _l3Read( myInput->rsComm, srcL3descInx, buf, toRead );

            if ( bytesRead != toRead ) {
                if ( bytesRead < 0 ) {
                    myInput->status = bytesRead;
                    rodsLogError( LOG_ERROR, bytesRead,
                                  "locToRemPartialCopy: copy error for %lld", bytesRead );
                }
                else if ( ( myInput->flags & NO_CHK_COPY_LEN_FLAG ) == 0 ) {
                    myInput->status = SYS_COPY_LEN_ERR - errno;
                    rodsLog( LOG_ERROR,
                             "locToRemPartialCopy: toGet %lld, bytesRead %d",
                             toGet, bytesRead );
                }
                break;
            }

            // =-=-=-=-=-=-=-
            // compute an iv for this particular transmission and use
            // it to encrypt this buffer
            int new_size = bytesRead;
            if ( use_encryption_flg ) {
                irods::error ret = crypt.initialization_vector( iv );
                if ( !ret.ok() ) {
                    ret = PASS( ret );
                    printf( "%s", ret.result().c_str() );
                    break;
                }

                // =-=-=-=-=-=-=-
                // encrypt
                in_buf.assign(
                    &buf[0],
                    &buf[ bytesRead ] );

                ret = crypt.encrypt(
                          shared_secret,
                          iv,
                          in_buf,
                          cipher );
                if ( !ret.ok() ) {
                    ret = PASS( ret );
                    printf( "%s", ret.result().c_str() );
                    break;
                }

                // =-=-=-=-=-=-=-
                // capture the iv with the cipher text
                std::copy(
                    iv.begin(),
                    iv.end(),
                    &buf[0] );
                std::copy(
                    cipher.begin(),
                    cipher.end(),
                    &buf[iv_size] );

                new_size = iv.size() + cipher.size();

                // =-=-=-=-=-=-=-
                // need to send the incoming size as encryption might change
                // the size of the data from the written values
                bytesWritten = myWrite(
                                   destFd,
                                   &new_size,
                                   sizeof( int ),
                                   &bytesWritten );
            }

            bytesWritten = myWrite(
                               destFd,
                               buf,
                               new_size,
                               NULL );

            if ( bytesWritten != new_size ) {
                rodsLog( LOG_NOTICE,
                         "_partialDataPut:Bytes written %d don't match read %d",
                         bytesWritten, bytesRead );

                if ( bytesWritten < 0 ) {
                    myInput->status = bytesWritten;
                }
                else {
                    myInput->status = SYS_COPY_LEN_ERR;
                }
                break;
            }

            toGet -= bytesRead;
        }

        curOffset += myHeader.length;
        myInput->bytesWritten += myHeader.length;
    }

    free( buf );
    if ( myInput->threadNum > 0 ) {
        _l3Close( myInput->rsComm, srcL3descInx );
    }
    CLOSE_SOCK( destFd );
} // locToRemPartialCopy

/*
 Given a zoneName, return the Zone Server ID string (from the
 server_config.json file) if defined. If the input zoneName is
 null, use the local zone.
 Input: zoneName
 Output: zoneSID
 */
void
getZoneServerId( char *zoneName, char *zoneSID ) {
    zoneInfo_t *tmpZoneInfo;
    rodsServerHost_t *tmpRodsServerHost;
    int zoneNameLen = 0;
    char *localZoneName = NULL;

    if ( !zoneSID ) {
        rodsLog( LOG_ERROR, "getZoneServerId - input zoneSID is NULL" );
        return;
    }

    if ( zoneName != NULL ) {
        zoneNameLen = strlen( zoneName );
    }
    if ( zoneNameLen == 0 ) {
        strncpy( zoneSID, localSID, MAX_PASSWORD_LEN );
        return;
    }

    /* get our local zoneName */
    tmpZoneInfo = ZoneInfoHead;
    while ( tmpZoneInfo != NULL ) {
        tmpRodsServerHost = (rodsServerHost_t*) tmpZoneInfo->primaryServerHost;
        if ( tmpRodsServerHost->rcatEnabled == LOCAL_ICAT ) {
            localZoneName = tmpZoneInfo->zoneName;
        }
        tmpZoneInfo = tmpZoneInfo->next;
    }

    /* return the local SID if the local zone is the one requested */
    if ( localZoneName != NULL ) {
        if ( strncmp( localZoneName, zoneName, MAX_NAME_LEN ) == 0 ) {
            strncpy( zoneSID, localSID, MAX_PASSWORD_LEN );
            return;
        }
    }

    // retrieve remote SID from map
    std::string _zone_sid = remote_SID_key_map[zoneName].first;

    if ( !_zone_sid.empty() ) {
        snprintf( zoneSID, MAX_PASSWORD_LEN, "%s", _zone_sid.c_str() );
        return;
    }

    zoneSID[0] = '\0';
    return;
}

int
isUserPrivileged( rsComm_t *rsComm ) {

    if ( rsComm->clientUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
    }
    if ( rsComm->proxyUser.authInfo.authFlag < LOCAL_PRIV_USER_AUTH ) {
        return CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
    }

    return 0;
}

/* generic functions to return SYS_NOT_SUPPORTED */

int
intNoSupport( ... ) {
    return SYS_NOT_SUPPORTED;
}

rodsLong_t
longNoSupport( ... ) {
    return ( rodsLong_t ) SYS_NOT_SUPPORTED;
}

void
reconnManager( rsComm_t *rsComm ) {
    struct sockaddr_in  remoteAddr;
    reconnMsg_t *reconnMsg;
    int acceptFailCnt = 0;

    if ( rsComm == NULL || rsComm->reconnSock <= 0 ) {
        return;
    }

    if ( listen( rsComm->reconnSock, 1 ) < 0 ) {
        rodsLog( LOG_ERROR,
                 "reconnManager: listen failed, errno: %d",
                 errno );
        return;
    }

    const int nSockets = rsComm->reconnSock + 1;
    fd_set basemask;
    FD_ZERO( &basemask );
    FD_SET( rsComm->reconnSock, &basemask );

    while ( 1 ) {
        int nSelected;
        while ( ( nSelected = select( nSockets, &basemask,
                                      ( fd_set * ) NULL, ( fd_set * ) NULL, NULL ) ) < 0 ) {
            if ( errno == EINTR ) {
                rodsLog( LOG_NOTICE, "reconnManager: select interrupted\n" );
                continue;
            }
            else {
                rodsLog( LOG_ERROR, "reconnManager: select failed, errno = %d", errno );
                boost::unique_lock< boost::mutex > boost_lock( *rsComm->thread_ctx->lock );
                close( rsComm->reconnSock );
                rsComm->reconnSock = 0;
                boost_lock.unlock();
                return;
            } // else
        } // while select

        /* don't lock it yet until we are done with establishing a connection */
        socklen_t len = sizeof( remoteAddr );
        std::memset(&remoteAddr, 0, sizeof(remoteAddr));

        const int saved_socket_flags = fcntl( rsComm->reconnSock, F_GETFL );
        fcntl( rsComm->reconnSock, F_SETFL, saved_socket_flags | O_NONBLOCK );
        const int newSock = accept( rsComm->reconnSock, ( struct sockaddr * ) &remoteAddr, &len );
        fcntl( rsComm->reconnSock, F_SETFL, saved_socket_flags );

        if ( newSock < 0 ) {
            acceptFailCnt++;
            rodsLog( LOG_ERROR,
                     "reconnManager: accept for sock %d failed, errno = %d",
                     rsComm->reconnSock, errno );
            if ( acceptFailCnt > MAX_RECON_ERROR_CNT ) {
                rodsLog( LOG_ERROR,
                         "reconnManager: accept failed cnt > 10, reconnManager exit" );
                close( rsComm->reconnSock );
                rsComm->reconnSock = -1;
                rsComm->reconnPort = 0;
                return;
            }
            else {
                continue;
            }
        }

        // =-=-=-=-=-=-=-
        // create a network object
        irods::network_object_ptr net_obj;
        irods::error ret = irods::network_factory( rsComm, net_obj );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return;
        }

        // =-=-=-=-=-=-=-
        // repave sock handle with new socket
        net_obj->socket_handle( newSock );

        ret = readReconMsg( net_obj, &reconnMsg );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            close( newSock );
            continue;
        }
        else if ( reconnMsg->cookie != rsComm->cookie ) {
            rodsLog( LOG_ERROR,
                     "reconnManager: cookie mismatch, got = %d vs %d",
                     reconnMsg->cookie, rsComm->cookie );
            close( newSock );
            free( reconnMsg );
            continue;
        }

        boost::unique_lock<boost::mutex> boost_lock( *rsComm->thread_ctx->lock );
        rsComm->clientState = reconnMsg->procState;
        rsComm->reconnectedSock = newSock;
        /* need to check agentState */
        while ( rsComm->agentState == SENDING_STATE ) {
            /* have to wait until the agent stop sending */
            rsComm->reconnThrState = CONN_WAIT_STATE;
            rsComm->thread_ctx->cond->wait( boost_lock );
        }

        rsComm->reconnThrState = PROCESSING_STATE;
        std::memset(reconnMsg, 0, sizeof(reconnMsg_t));
        reconnMsg->procState = rsComm->agentState;
        ret = sendReconnMsg( net_obj, reconnMsg );
        free( reconnMsg );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            close( newSock );
            rsComm->reconnectedSock = 0;
            boost_lock.unlock();
            continue;
        }
        if ( rsComm->agentState == PROCESSING_STATE ) {
            rodsLog( LOG_NOTICE,
                     "reconnManager: svrSwitchConnect. cliState = %d,agState=%d",
                     rsComm->clientState, rsComm->agentState );
            svrSwitchConnect( rsComm );
        }
        boost_lock.unlock();
    } // while 1
}

int
svrChkReconnAtSendStart( rsComm_t *rsComm ) {
    if ( rsComm->reconnSock > 0 ) {
        /* handle reconn */
        boost::unique_lock<boost::mutex> boost_lock( *rsComm->thread_ctx->lock );
        if ( rsComm->reconnThrState == CONN_WAIT_STATE ) {
            /* should not be here */
            rodsLog( LOG_NOTICE,
                     "svrChkReconnAtSendStart: ThrState = CONN_WAIT_STATE, agentState=%d",
                     rsComm->agentState );
            rsComm->agentState = PROCESSING_STATE;
            rsComm->thread_ctx->cond->notify_all();
        }
        svrSwitchConnect( rsComm );
        rsComm->agentState = SENDING_STATE;
        boost_lock.unlock();
    }
    return 0;
}

int
svrChkReconnAtSendEnd( rsComm_t *rsComm ) {
    if ( rsComm->reconnSock > 0 ) {
        /* handle reconn */
        boost::unique_lock<boost::mutex> boost_lock( *rsComm->thread_ctx->lock );
        rsComm->agentState = PROCESSING_STATE;
        if ( rsComm->reconnThrState == CONN_WAIT_STATE ) {
            rsComm->thread_ctx->cond->wait( boost_lock );
        }
        boost_lock.unlock();
    }
    return 0;
}

int
svrChkReconnAtReadStart( rsComm_t *rsComm ) {
    if ( rsComm->reconnSock > 0 ) {
        /* handle reconn */
        boost::unique_lock< boost::mutex > boost_lock( *rsComm->thread_ctx->lock );
        if ( rsComm->reconnThrState == CONN_WAIT_STATE ) {
            /* should not be here */
            rodsLog( LOG_NOTICE,
                     "svrChkReconnAtReadStart: ThrState = CONN_WAIT_STATE, agentState=%d",
                     rsComm->agentState );
            rsComm->agentState = PROCESSING_STATE;
            rsComm->thread_ctx->cond->wait( boost_lock );
        }
        svrSwitchConnect( rsComm );
        rsComm->agentState = RECEIVING_STATE;
        boost_lock.unlock();
    }
    return 0;
}

int
svrChkReconnAtReadEnd( rsComm_t *rsComm ) {
    if ( rsComm->reconnSock > 0 ) {
        /* handle reconn */
        boost::unique_lock< boost::mutex > boost_lock( *rsComm->thread_ctx->lock );
        rsComm->agentState = PROCESSING_STATE;
        if ( rsComm->reconnThrState == CONN_WAIT_STATE ) {
            rsComm->thread_ctx->cond->notify_all();
        }
        boost_lock.unlock();
    }
    return 0;
}

int svrSockOpenForInConn(rsComm_t* rsComm, int* portNum, char** addr, int proto)
{
    int sfd = sockOpenForInConn(rsComm, portNum, addr, proto);
    if (sfd < 0) {
        return sfd;
    }

    if (const int ec = set_socket_tcp_keepalive_options(sfd); ec < 0) {
        return ec;
    }

    if ( addr != NULL && *addr != NULL &&
            ( isLoopbackAddress( *addr ) || strcmp( *addr, "0.0.0.0" ) == 0 ||
              strcmp( *addr, "localhost" ) == 0 ) ) {
        /* localhost */
        char *myaddr;

        myaddr = getLocalSvrAddr();
        if ( myaddr != NULL ) {
            free( *addr );
            *addr = strdup( myaddr );
        }
        else {
            rodsLog( LOG_NOTICE,
                     "svrSockOpenForInConn: problem resolving local host addr %s",
                     *addr );
        }
    }
    return sfd;
} // svrSockOpenForInConn

char *
getLocalSvrAddr() {
    char *myHost;
    myHost = _getSvrAddr( LocalServerHost );
    return myHost;
}

char *
_getSvrAddr( rodsServerHost_t *rodsServerHost ) {
    hostName_t *tmpHostName;

    if ( rodsServerHost == NULL ) {
        return NULL;
    }

    tmpHostName = rodsServerHost->hostName;
    while ( tmpHostName != NULL ) {
        if ( strcmp( tmpHostName->name, "localhost" ) != 0 &&
                !isLoopbackAddress( tmpHostName->name ) &&
                strcmp( tmpHostName->name, "0.0.0.0" ) != 0 &&
                strchr( tmpHostName->name, '.' ) != NULL ) {
            return tmpHostName->name;
        }
        tmpHostName = tmpHostName->next;
    }
    return NULL;
}

char *
getSvrAddr( rodsServerHost_t *rodsServerHost ) {
    char *myHost;

    myHost = _getSvrAddr( rodsServerHost );
    if ( myHost == NULL ) {
        /* use the first one */
        myHost = rodsServerHost->hostName->name;
    }
    return myHost;
}

int
setLocalSrvAddr( char *outLocalAddr ) {
    char *myHost;

    if ( outLocalAddr == NULL ) {
        return USER__NULL_INPUT_ERR;
    }

    myHost = getSvrAddr( LocalServerHost );

    if ( myHost != NULL ) {
        rstrcpy( outLocalAddr, myHost, NAME_LEN );
        return 0;
    }
    else {
        return SYS_INVALID_SERVER_HOST;
    }
}

int
singleRemLocCopy( rsComm_t *rsComm, dataCopyInp_t *dataCopyInp ) {
    dataOprInp_t *dataOprInp;
    int status = 0;
    int oprType;

    if ( dataCopyInp == NULL ) {
        rodsLog( LOG_NOTICE,
                 "remLocCopy: NULL dataCopyInp input" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }
    dataOprInp = &dataCopyInp->dataOprInp;
    oprType = dataOprInp->oprType;

    if ( oprType == COPY_TO_LOCAL_OPR ) {
        status = singleRemToLocCopy( rsComm, dataCopyInp );
    }
    else {
        status = singleLocToRemCopy( rsComm, dataCopyInp );
    }
    return status;
}

int
singleRemToLocCopy( rsComm_t *rsComm, dataCopyInp_t *dataCopyInp ) {
    dataOprInp_t *dataOprInp;
    rodsLong_t dataSize;
    int l1descInx;
    int destL3descInx;
    bytesBuf_t dataObjReadInpBBuf;
    openedDataObjInp_t dataObjReadInp;
    int bytesWritten, bytesRead;
    rodsLong_t totalWritten = 0;

    /* a GET type operation */
    if ( dataCopyInp == NULL ) {
        rodsLog( LOG_NOTICE,
                 "singleRemToLocCopy: NULL dataCopyInp input" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    int trans_buff_size;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return e.code();
    }

    dataOprInp = &dataCopyInp->dataOprInp;
    l1descInx = dataCopyInp->portalOprOut.l1descInx;
    destL3descInx = dataOprInp->destL3descInx;
    dataSize = dataOprInp->dataSize;

    std::memset(&dataObjReadInp, 0, sizeof(dataObjReadInp));
    dataObjReadInpBBuf.buf = malloc( trans_buff_size );
    dataObjReadInpBBuf.len = dataObjReadInp.len = trans_buff_size;
    dataObjReadInp.l1descInx = l1descInx;
    while ( ( bytesRead = rsDataObjRead( rsComm, &dataObjReadInp,
                                         &dataObjReadInpBBuf ) ) > 0 ) {
        bytesWritten = _l3Write( rsComm, destL3descInx,
                                 dataObjReadInpBBuf.buf, bytesRead );

        if ( bytesWritten != bytesRead ) {
            rodsLog( LOG_ERROR,
                     "singleRemToLocCopy: Read %d bytes, Wrote %d bytes.\n ",
                     bytesRead, bytesWritten );
            free( dataObjReadInpBBuf.buf );
            return SYS_COPY_LEN_ERR;
        }
        else {
            totalWritten += bytesWritten;
        }
    }
    free( dataObjReadInpBBuf.buf );
    if ( dataSize <= 0 || totalWritten == dataSize ||
            getValByKey( &dataOprInp->condInput, NO_CHK_COPY_LEN_KW ) != NULL ) {
        return 0;
    }
    else {
        rodsLog( LOG_ERROR,
                 "singleRemToLocCopy: totalWritten %lld dataSize %lld mismatch",
                 totalWritten, dataSize );
        return SYS_COPY_LEN_ERR;
    }
}

int
singleLocToRemCopy( rsComm_t *rsComm, dataCopyInp_t *dataCopyInp ) {
    dataOprInp_t *dataOprInp;
    rodsLong_t dataSize;
    int l1descInx;
    int srcL3descInx;
    bytesBuf_t dataObjWriteInpBBuf;
    openedDataObjInp_t dataObjWriteInp;
    int bytesWritten, bytesRead;
    rodsLong_t totalWritten = 0;

    /* a PUT type operation */
    if ( dataCopyInp == NULL ) {
        rodsLog( LOG_NOTICE,
                 "singleRemToLocCopy: NULL dataCopyInp input" );
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    int trans_buff_size;
    try {
        trans_buff_size = irods::get_advanced_setting<const int>(irods::KW_CFG_TRANS_BUFFER_SIZE_FOR_PARA_TRANS) * 1024 * 1024;
    } catch ( const irods::exception& e ) {
        irods::log(e);
        return e.code();
    }

    dataOprInp = &dataCopyInp->dataOprInp;
    l1descInx = dataCopyInp->portalOprOut.l1descInx;
    srcL3descInx = dataOprInp->srcL3descInx;
    dataSize = dataOprInp->dataSize;

    std::memset(&dataObjWriteInp, 0, sizeof(dataObjWriteInp));
    dataObjWriteInpBBuf.buf = malloc( trans_buff_size );
    dataObjWriteInpBBuf.len = 0;
    dataObjWriteInp.l1descInx = l1descInx;

    while ( ( bytesRead = _l3Read( rsComm, srcL3descInx,
                                   dataObjWriteInpBBuf.buf, trans_buff_size ) ) > 0 ) {
        dataObjWriteInp.len =  dataObjWriteInpBBuf.len = bytesRead;
        bytesWritten = rsDataObjWrite( rsComm, &dataObjWriteInp,
                                       &dataObjWriteInpBBuf );
        if ( bytesWritten != bytesRead ) {
            rodsLog( LOG_ERROR,
                     "singleLocToRemCopy: Read %d bytes, Wrote %d bytes.\n ",
                     bytesRead, bytesWritten );
            free( dataObjWriteInpBBuf.buf );
            return SYS_COPY_LEN_ERR;
        }
        else {
            totalWritten += bytesWritten;
        }
    }
    free( dataObjWriteInpBBuf.buf );
    if ( dataSize <= 0 || totalWritten == dataSize ||
            getValByKey( &dataOprInp->condInput, NO_CHK_COPY_LEN_KW ) != NULL ) {
        return 0;
    }
    else {
        rodsLog( LOG_ERROR,
                 "singleLocToRemCopy: totalWritten %lld dataSize %lld mismatch",
                 totalWritten, dataSize );
        return SYS_COPY_LEN_ERR;
    }
}

/* readStartupPack - Read the startup packet from client.
 * Note: initServerInfo must be called first because it calls getLocalZoneInfo.
 */

irods::error
readStartupPack(
    irods::network_object_ptr _ptr,
    startupPack_t**     startupPack,
    struct timeval*     tv ) {
    msgHeader_t myHeader;
    irods::error ret = readMsgHeader( _ptr, &myHeader, tv );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    if ( strcmp(myHeader.type, RODS_HEARTBEAT_T) == 0 ) {
        *startupPack = static_cast<startupPack_t*>(malloc(sizeof(startupPack_t)));
        std::memset(*startupPack, 0, sizeof(startupPack_t));
        snprintf((*startupPack)->option, sizeof((*startupPack)->option), "%s", RODS_HEARTBEAT_T);
        return SUCCESS();
    }

    if ( myHeader.msgLen > ( int ) sizeof( startupPack_t ) * 2 ||
            myHeader.msgLen <= 0 ) {
        std::stringstream msg;
        msg << "readStartupPack: problem with myHeader.msgLen = " << myHeader.msgLen;
        return ERROR( SYS_HEADER_READ_LEN_ERR, msg.str() );
    }

    bytesBuf_t inputStructBBuf{}, bsBBuf{}, errorBBuf{};
    ret = readMsgBody(
              _ptr,
              &myHeader,
              &inputStructBBuf,
              &bsBBuf,
              &errorBBuf,
              XML_PROT,
              tv );
    if ( !ret.ok() ) {
        return PASS( ret );
    }

    /* some sanity check */

    if ( strcmp( myHeader.type, RODS_CONNECT_T ) != 0 ) {
        if ( inputStructBBuf.buf != NULL ) {
            clearBBuf( &inputStructBBuf );
        }
        if ( bsBBuf.buf != NULL ) {
            clearBBuf( &bsBBuf );
        }
        if ( errorBBuf.buf != NULL ) {
            clearBBuf( &errorBBuf );
        }
        std::stringstream msg;
        msg << "readStartupPack: wrong mag type - " << myHeader.type << ", expect " << RODS_CONNECT_T;
        return ERROR( SYS_HEADER_TYPE_LEN_ERR, msg.str() );
    }

    if ( myHeader.bsLen != 0 ) {
        if ( bsBBuf.buf != NULL ) {
            clearBBuf( &bsBBuf );
        }
        rodsLog( LOG_NOTICE, "readStartupPack: myHeader.bsLen = %d is not 0",
                 myHeader.bsLen );
    }

    if ( myHeader.errorLen != 0 ) {
        if ( errorBBuf.buf != NULL ) {
            clearBBuf( &errorBBuf );
        }
        rodsLog( LOG_NOTICE,
                 "readStartupPack: myHeader.errorLen = %d is not 0",
                 myHeader.errorLen );
    }

    /* always use XML_PROT for the startup pack */
    int status = unpack_struct( inputStructBBuf.buf, ( void ** ) startupPack,
                           "StartupPack_PI", RodsPackTable, XML_PROT, nullptr);

    clearBBuf( &inputStructBBuf );

    if ( status < 0 ) {
        return ERROR( status, "readStartupPack:unpackStruct error." );
    }

    if ( ( *startupPack )->clientUser[0] != '\0'  &&
            ( *startupPack )->clientRodsZone[0] == '\0' ) {
        /* clientRodsZone is not defined */
        if ( const char* zoneName = getLocalZoneName() ) {
            rstrcpy( ( *startupPack )->clientRodsZone, zoneName, NAME_LEN );
        }
    }
    if ( ( *startupPack )->proxyUser[0] != '\0'  &&
            ( *startupPack )->proxyRodsZone[0] == '\0' ) {
        /* proxyRodsZone is not defined */
        if ( const char* zoneName = getLocalZoneName() ) {
            rstrcpy( ( *startupPack )->proxyRodsZone, zoneName, NAME_LEN );
        }
    }

    return CODE( status );
}

/* initServiceUser - set the username/uid of the unix user to
 *      run the iRODS daemons as if configured using the
 *      irodsServiceUser environment variable. Will also
 *      set effective uid to service user.
 */
int
initServiceUser() {
    char *serviceUser;
    struct passwd *pwent;

    serviceUser = getenv( "irodsServiceUser" );
    if ( serviceUser == NULL || getuid() != 0 ) {
        /* either the option is not set, or not running     */
        /* with the necessary root permission. Just return. */
        return 0;
    }

    /* clear errno before getpwnam to distinguish an error from user */
    /* not existing. pwent == NULL && errno == 0 means no entry      */
    errno = 0;
    pwent = getpwnam( serviceUser );
    if ( pwent ) {
        ServiceUid = pwent->pw_uid;
        ServiceGid = pwent->pw_gid;
        return changeToServiceUser();
    }

    if ( errno ) {
        rodsLogError( LOG_ERROR, SYS_USER_RETRIEVE_ERR,
                      "setServiceUser: error in getpwnam %s, errno = %d",
                      serviceUser, errno );
        return SYS_USER_RETRIEVE_ERR - errno;
    }
    else {
        rodsLogError( LOG_ERROR, SYS_USER_RETRIEVE_ERR,
                      "setServiceUser: user %s doesn't exist", serviceUser );
        return SYS_USER_RETRIEVE_ERR;
    }
}

/* isServiceUserSet - check if the service user has been configured
 */
int
isServiceUserSet() {
    if ( ServiceUid ) {
        return 1;
    }
    else {
        return 0;
    }
}

/* changeToRootUser - take on root privilege by setting the process
 *                    effective uid to zero.
 */
int
changeToRootUser() {
    int prev_errno, my_errno;

    if ( !isServiceUserSet() ) {
        /* not configured ... just return */
        return 0;
    }

    /* preserve the errno from before. We'll often be   */
    /* called after a "permission denied" type error,   */
    /* so we need to preserve this previous error state */
    prev_errno = errno;
    if ( seteuid( 0 ) == -1 ) {
        my_errno = errno;
        errno = prev_errno;
        rodsLogError( LOG_ERROR, SYS_USER_NO_PERMISSION - my_errno,
                      "changeToRootUser: can't change to root user id" );
        return SYS_USER_NO_PERMISSION - my_errno;
    }

    return 0;
}

/* changeToServiceUser - set the process effective uid to that of the
 *                       configured service user. Normally used to give
 *                       up root permission.
 */
int
changeToServiceUser() {
    int prev_errno, my_errno;

    if ( !isServiceUserSet() ) {
        /* not configured ... just return */
        return 0;
    }

    prev_errno = errno;

    if ( setegid( ServiceGid ) == -1 ) {
        /* if only setegid fails, log error but continue */
        rodsLog( LOG_ERROR, "changeToServiceUser: setegid() failed, errno = %d", errno );
        errno = prev_errno;
    }

    if ( seteuid( ServiceUid ) == -1 ) {
        my_errno = errno;
        errno = prev_errno;
        rodsLogError( LOG_ERROR, SYS_USER_NO_PERMISSION - my_errno,
                      "changeToServiceUser: can't change to service user id" );
        return SYS_USER_NO_PERMISSION - my_errno;
    }

    return 0;
}

/* changeToUser - set the process effective uid to the provided uid.
 *                Used to allow the iRODS services the ability to
 *                perform actions as a particular user.
 */
int
changeToUser( uid_t uid ) {
    int prev_errno, my_errno;

    if ( !isServiceUserSet() ) {
        /* not configured ... just return */
        return 0;
    }

    prev_errno = errno;
    if ( geteuid() != 0 ) {
        changeToRootUser();
    }
    if ( seteuid( uid ) == -1 ) {
        my_errno = errno;
        errno = prev_errno;
        rodsLogError( LOG_ERROR, SYS_USER_NO_PERMISSION - my_errno,
                      "changeToUser: can't change to user id %d",
                      uid );
        return SYS_USER_NO_PERMISSION - my_errno;
    }
    errno = prev_errno;

    return 0;
}

/* dropRootPrivilege - set the process real and effective uid to
 *                     the current effective uid of the process.
 *                     Used, for example, to drop root privilege
 *                     before a call to execl().
 */
int
dropRootPrivilege() {
    int prev_errno, my_errno;
    uid_t new_real_uid;

    if ( !isServiceUserSet() ) {
        /* not configured ... just return */
        return 0;
    }

    prev_errno = errno;

    new_real_uid = geteuid();
    if ( new_real_uid == 0 ) {
        /* will become the iRODS service user */
        new_real_uid = ServiceUid;
    }
    else {
        /* need to set effective uid to root
           for the call to setuid() */
        changeToRootUser();
    }

    if ( setuid( new_real_uid ) == -1 ) {
        my_errno = errno;
        errno = prev_errno;
        rodsLogError( LOG_ERROR, SYS_USER_NO_PERMISSION - my_errno,
                      "dropRootPrivilege: can't setuid() to uid %d",
                      new_real_uid );
        return SYS_USER_NO_PERMISSION - my_errno;
    }

    errno = prev_errno;

    return 0;
}

/*
  check a chlModAVUMetadata argument; returning the type.
*/
int
checkModArgType( const char *arg ) {
    if ( arg == NULL || strlen( arg ) == 0 ) {
        return CAT_INVALID_ARGUMENT;
    }
    if ( ':' != arg[1] ) {
        return 0;
    }
    switch ( arg[0] ) {
    case 'n':
        return 1;
    case 'v':
        return 2;
    case 'u':
        return 3;
    default:
        return 0;
    }
}

irods::error setRECacheSaltFromEnv()
{
    // Should only ever set the cache salt once
    try {
        const auto& existing_name = irods::get_server_property<const std::string>(irods::KW_CFG_RE_CACHE_SALT);
        log_server::debug("{}: Cache salt already set [{}]", __func__, irods::KW_CFG_RE_CACHE_SALT, existing_name);
    }
    catch (const irods::exception&) {
        const char* p_mutex_salt = std::getenv(SP_RE_CACHE_SALT); // NOLINT(concurrency-mt-unsafe)

        if (!p_mutex_salt) {
            const auto msg =
                fmt::format("{}: Could not retrieve cache salt environment variable [{}].", __func__, SP_RE_CACHE_SALT);
            log_server::critical(msg);
            return ERROR(SYS_GETENV_ERR, msg);
        }

        try {
            irods::set_server_property<std::string>(irods::KW_CFG_RE_CACHE_SALT, p_mutex_salt);
        }
        catch (const irods::exception& e) {
            log_server::critical("{}: Failed to set [{}] in server properties.", __func__, irods::KW_CFG_RE_CACHE_SALT);
            return irods::error(e);
        }
    }

    return SUCCESS();
} // setRECacheSaltFromEnv

irods::error add_global_re_params_to_kvp_for_dynpep(
    keyValPair_t& _kvp ) {

    irods::error ret = SUCCESS();

    try {
        addKeyVal(&_kvp, irods::CLIENT_USER_NAME_KW.c_str(), irods::get_server_property<const std::string>(irods::CLIENT_USER_NAME_KW).c_str());
    } catch ( const irods::exception& e ) {
        addKeyVal(&_kvp, irods::CLIENT_USER_NAME_KW.c_str(), "");
    }

    try {
        addKeyVal(&_kvp, irods::CLIENT_USER_ZONE_KW.c_str(), irods::get_server_property<const std::string>(irods::CLIENT_USER_ZONE_KW).c_str());
    } catch ( const irods::exception& e ) {
        addKeyVal(&_kvp, irods::CLIENT_USER_ZONE_KW.c_str(), "");
    }

    try {
        addKeyVal(&_kvp, irods::CLIENT_USER_PRIV_KW.c_str(),
            boost::lexical_cast<std::string>(irods::get_server_property<const int>(irods::CLIENT_USER_PRIV_KW)).c_str());
    } catch ( boost::bad_lexical_cast& _e ) {
        // can't actually fail to cast an int to a string
        addKeyVal(&_kvp, irods::CLIENT_USER_PRIV_KW.c_str(), "0");
    } catch ( const irods::exception& e ) {
        addKeyVal(&_kvp, irods::CLIENT_USER_PRIV_KW.c_str(), "0");
    }

    try {
        addKeyVal(&_kvp, irods::PROXY_USER_NAME_KW.c_str(), irods::get_server_property<const std::string>(irods::PROXY_USER_NAME_KW).c_str());
    } catch ( const irods::exception& e ) {
        addKeyVal(&_kvp, irods::PROXY_USER_NAME_KW.c_str(), "");
    }

    try {
        addKeyVal(&_kvp, irods::PROXY_USER_ZONE_KW.c_str(), irods::get_server_property<const std::string>(irods::PROXY_USER_ZONE_KW).c_str());
    } catch ( const irods::exception& e ) {
        addKeyVal(&_kvp, irods::PROXY_USER_ZONE_KW.c_str(), "");
    }

    try {
        addKeyVal(&_kvp, irods::PROXY_USER_PRIV_KW.c_str(),
            boost::lexical_cast<std::string>(irods::get_server_property<const int>(irods::PROXY_USER_PRIV_KW)).c_str());
    } catch ( boost::bad_lexical_cast& _e ) {
        // can't actually fail to cast an int to a string
        addKeyVal(&_kvp, irods::PROXY_USER_PRIV_KW.c_str(), "0");
    } catch ( const irods::exception& e ) {
        addKeyVal(&_kvp, irods::PROXY_USER_PRIV_KW.c_str(), "0");
    }

    return ret;

} // add_global_re_params_to_kvp

irods::error get_catalog_service_role(
    std::string& _role ) {

    try {
        _role = irods::get_server_property<std::string>(irods::KW_CFG_CATALOG_SERVICE_ROLE);
    } catch ( const irods::exception& e ) {
        return irods::error(e);
    }

    return SUCCESS();

} // get_catalog_service_role

irods::error get_default_rule_plugin_instance(std::string& _instance_name)
{
    try {
        _instance_name = irods::get_server_property<const nlohmann::json&>(std::vector<std::string>{irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE})[0].at(irods::KW_CFG_INSTANCE_NAME).get<std::string>();
    }
    catch (const irods::exception& e) {
        return irods::error(e);
    }
    catch (const boost::bad_any_cast& e) {
        return ERROR(INVALID_ANY_CAST, e.what());
    }
    catch (const std::out_of_range& e) {
        return ERROR(KEY_NOT_FOUND, e.what());
    }

    return SUCCESS();
} // get_default_rule_plugin_instance

irods::error list_rule_plugin_instances(std::vector<std::string>& _instance_names)
{
    try {
        const auto& rule_engines = irods::get_server_property<const nlohmann::json&>(std::vector<std::string>{irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE});
        for (const auto& el : rule_engines) {
            _instance_names.push_back(el.at(irods::KW_CFG_INSTANCE_NAME).get_ref<const std::string&>());
        }
    }
    catch (const irods::exception& e) {
        return irods::error(e);
    }
    catch (const boost::bad_any_cast& e) {
        return ERROR(INVALID_ANY_CAST, e.what());
    }
    catch (const std::out_of_range& e) {
        return ERROR(KEY_NOT_FOUND, e.what());
    }

    return SUCCESS();
}
