import { createRequire } from 'module'
import { RtpPacket } from 'werift'
import { EventEmitter } from 'events'
import { createSocket } from 'dgram'

import RtpSequencer from './rtp-sequencer.js'


const require = createRequire(import.meta.url)

const sipLib = require('@vexyl.ai/sip')
const digest = require('@vexyl.ai/sip/digest')


const {
  SIP_DOMAIN,
  SIP_PORT,
  SIP_DEST,
  SIP_USER,
  SIP_PASS,
  LOCAL_IP,
  LOCAL_RTP_PORT,
  LOCAL_SIP_PORT
} = process.env


/*
 * RTP payload type advertised by ring2sip for RFC4733 DTMF.
 */
const LOCAL_DTMF_PAYLOAD_TYPE = 101


const DTMF_EVENT_MAP = {
  0: '0',
  1: '1',
  2: '2',
  3: '3',
  4: '4',
  5: '5',
  6: '6',
  7: '7',
  8: '8',
  9: '9',
  10: '*',
  11: '#',
  12: 'A',
  13: 'B',
  14: 'C',
  15: 'D',
  16: 'FLASH'
}


function rstring() {

  const chars =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

  let result = ''

  for (let i = 0; i < 9; i++) {

    result += chars.charAt(
      Math.floor(
        Math.random() *
        chars.length
      )
    )
  }

  return result
}


class Sip extends EventEmitter {

  constructor() {

    super()

    this.sipSession = null
    this.inviteRequest = null

    this.authSession = {
      realm: SIP_DOMAIN
    }

    this.isSipStackStarted = false
    this.initiatingCall = false

    this.serverRtpInfo = null

    this.udp =
      createSocket('udp4')

    this.localRtpPort = null


    this.rtpReady =
      new Promise(
        (resolve, reject) => {

          const onError =
            error =>
              reject(error)


          this.udp.once(
            'error',
            onError
          )


          this.udp.bind(
            Number(
              LOCAL_RTP_PORT
            ),
            LOCAL_IP,
            () => {

              this.udp.off(
                'error',
                onError
              )


              const port =
                this.udp
                  .address()
                  .port


              this.localRtpPort =
                port


              console.log(
                `SIP - RTP Socket bound to ${LOCAL_IP}:${port}`
              )


              resolve(
                port
              )
            }
          )
        }
      )


    this.rtpSequencer =
      new RtpSequencer()


    this.registerInterval =
      null

    this.registerExpires =
      600

    this.currentCallId =
      null

    this.registered =
      false


    /*
     * The payload type used by the opposite side for
     * RFC4733 telephone events.
     */
    this.remoteDtmfPayloadType =
      null


    /*
     * RFC4733 sends the same event several times.
     * Keep track of completed events so "*" opens
     * the door exactly once.
     */
    this.lastDtmfEventKey =
      null
  }


  async initialize(
    debug = false
  ) {

    if (
      this.isSipStackStarted
    ) {
      return
    }


    await this.rtpReady


    if (
      this.isSipStackStarted
    ) {
      return
    }


    sipLib.start(
      {
        address:
          LOCAL_IP,

        port:
          Number(
            LOCAL_SIP_PORT
          ),

        logger:
          debug
            ? {
                send:
                  (
                    m,
                    target
                  ) =>
                    console.log(
                      'send',
                      m,
                      target
                    ),

                recv:
                  (
                    m,
                    target
                  ) =>
                    console.log(
                      'recv',
                      m,
                      target
                    ),

                error:
                  e =>
                    console.log(
                      'error',
                      e
                    )
              }
            : null
      },

      request => {

        console.log(
          `SIP - Received request: ${request.method}`
        )


        if (
          request.method ===
          'BYE'
        ) {

          this._handleBye(
            request
          )

        } else if (
          request.method ===
          'INVITE'
        ) {

          this._handleInboundInvite(
            request
          )

        } else if (
          request.method ===
          'OPTIONS'
        ) {

          const response =
            sipLib.makeResponse(
              request,
              200,
              'OK'
            )


          sipLib.send(
            response
          )

        } else if (
          request.method ===
          'INFO'
        ) {

          /*
           * RFC4733 is our preferred DTMF method, but accepting
           * SIP INFO as a fallback costs almost nothing and
           * makes the bridge more tolerant of other phones.
           */
          this._handleInfo(
            request
          )
        }
      }
    )


    this.isSipStackStarted =
      true
  }


  register() {

    if (
      this.registerInterval
    ) {
      return
    }


    const sendRegister =
      (
        expires =
          this.registerExpires
      ) => {

        const registerRequest = {

          method:
            'REGISTER',

          uri:
            `sip:${SIP_DOMAIN}:${SIP_PORT}`,

          headers: {

            to: {
              uri:
                `sip:${SIP_USER}@${SIP_DOMAIN}`
            },

            from: {

              uri:
                `sip:${SIP_USER}@${SIP_DOMAIN}`,

              params: {
                tag:
                  rstring()
              }
            },

            'call-id':
              rstring(),

            cseq: {
              method:
                'REGISTER',

              seq:
                1
            },

            contact: [
              {
                uri:
                  `sip:${SIP_USER}@${LOCAL_IP}:${LOCAL_SIP_PORT}`,

                params: {
                  expires
                }
              }
            ],

            'max-forwards':
              70,

            'user-agent':
              'RingIntercom2SIP/1.0',

            expires
          }
        }


        sipLib.send(
          registerRequest,

          response => {

            if (
              response.status ===
                401 &&
              response.headers[
                'www-authenticate'
              ]
            ) {

              this._retryWithDigestAuth(
                registerRequest,
                response,

                'SIP - REGISTER success (after auth).',

                'SIP - REGISTER failed:',

                authResp => {

                  const ok =
                    authResp &&
                    authResp.status >=
                      200 &&
                    authResp.status <
                      300


                  const changed =
                    this.registered !==
                    ok


                  this.registered =
                    ok


                  if (
                    changed
                  ) {

                    this.emit(
                      'registrationChanged',
                      this.registered
                    )
                  }
                }
              )

              return
            }


            if (
              response.status >=
                200 &&
              response.status <
                300
            ) {

              console.log(
                'SIP - REGISTER success.'
              )


              const changed =
                this.registered !==
                true


              this.registered =
                true


              if (
                changed
              ) {

                this.emit(
                  'registrationChanged',
                  true
                )
              }


              return
            }


            if (
              response.status >=
              300
            ) {

              console.error(
                `SIP - REGISTER failed: ${response.status} ${response.reason}`
              )


              this.registered =
                false
            }
          }
        )
      }


    sendRegister()


    this.registerInterval =
      setInterval(
        () =>
          sendRegister(),

        this.registerExpires *
          1000
      )
  }


  isRegistered() {

    return this.registered
  }


  initiateCall() {

    if (
      this.initiatingCall
    ) {
      return
    }


    this.initiatingCall =
      true


    this.remoteDtmfPayloadType =
      null

    this.lastDtmfEventKey =
      null


    console.log(
      `SIP - Initiating call to extension ${SIP_DEST} on ${SIP_DOMAIN}...`
    )


    const sessionId =
      Date.now()


    this.inviteRequest = {

      method:
        'INVITE',

      uri:
        `sip:${SIP_DEST}@${SIP_DOMAIN}:${SIP_PORT}`,

      headers: {

        to: {
          uri:
            `sip:${SIP_DEST}@${SIP_DOMAIN}`
        },

        from: {

          uri:
            `sip:${SIP_USER}@${SIP_DOMAIN}`,

          params: {
            tag:
              rstring()
          }
        },

        'call-id':
          rstring(),

        cseq: {
          method:
            'INVITE',

          seq:
            1
        },

        contact: [
          {
            uri:
              `sip:${SIP_USER}@${LOCAL_IP}:${LOCAL_SIP_PORT}`
          }
        ],

        'max-forwards':
          70,

        'content-type':
          'application/sdp'
      },

      content:
        this._buildLocalSdp(
          sessionId
        )
    }


    sipLib.send(
      this.inviteRequest,

      response => {

        this._handleInviteResponse(
          response
        )
      }
    )
  }


  cleanup() {

    if (
      this.sipSession &&
      this.sipSession.headers
    ) {

      console.log(
        'SIP - Sending BYE to terminate call...'
      )


      const response =
        this.sipSession


      const byeUri =
        response.headers
          ?.contact?.[0]?.uri ||
        response.headers
          ?.to?.uri


      if (
        byeUri
      ) {

        sipLib.send(
          {
            method:
              'BYE',

            uri:
              byeUri,

            headers: {

              to:
                response.headers.to,

              from:
                response.headers.from,

              'call-id':
                response.headers[
                  'call-id'
                ],

              cseq: {
                method:
                  'BYE',

                seq:
                  response.headers
                    .cseq.seq + 1
              },

              via:
                response.headers.via
            }
          }
        )
      }

    } else if (
      this.inviteRequest
    ) {

      console.log(
        'SIP - Sending CANCEL...'
      )


      sipLib.send(
        {
          method:
            'CANCEL',

          uri:
            this.inviteRequest.uri,

          headers: {

            to:
              this.inviteRequest
                .headers.to,

            from:
              this.inviteRequest
                .headers.from,

            'call-id':
              this.inviteRequest
                .headers[
                  'call-id'
                ],

            cseq: {
              method:
                'CANCEL',

              seq:
                this.inviteRequest
                  .headers
                  .cseq.seq
            },

            via:
              this.inviteRequest
                .headers.via
          }
        }
      )
    }


    if (
      this.registerInterval
    ) {

      clearInterval(
        this.registerInterval
      )

      this.registerInterval =
        null
    }


    this.registered =
      false

    this.sipSession =
      null

    this.inviteRequest =
      null

    this.initiatingCall =
      false

    this.serverRtpInfo =
      null

    this.remoteDtmfPayloadType =
      null

    this.lastDtmfEventKey =
      null
  }


  pipeAudio(
    ring
  ) {

    /*
     * Prevent duplicate listeners if pipeAudio() happens
     * more than once during the process lifetime.
     */
    this.udp.removeAllListeners(
      'message'
    )


    this.udp.on(
      'message',

      message => {

        let rtpPacket


        try {

          rtpPacket =
            RtpPacket.deSerialize(
              message
            )

        } catch (
          err
        ) {

          console.error(
            'SIP - Invalid RTP packet:',
            err
          )

          return
        }


        /*
         * RFC4733 DTMF must never be forwarded to Ring as
         * though it were an Opus audio packet.
         */
        if (
          this.remoteDtmfPayloadType !==
            null &&
          rtpPacket.header
            .payloadType ===
            this.remoteDtmfPayloadType
        ) {

          this._handleTelephoneEvent(
            rtpPacket
          )

          return
        }


        ring.sendAudioPacket(
          rtpPacket,
          false
        )
      }
    )
  }


  _handleTelephoneEvent(
    rtpPacket
  ) {

    const payload =
      rtpPacket.payload


    if (
      !payload ||
      payload.length <
        4
    ) {
      return
    }


    const eventCode =
      payload[0]


    const end =
      (
        payload[1] &
        0x80
      ) !== 0


    /*
     * RFC4733 retransmits an event several times.
     * Act only when the sender marks the event complete.
     */
    if (
      !end
    ) {
      return
    }


    const digit =
      DTMF_EVENT_MAP[
        eventCode
      ]


    if (
      !digit
    ) {
      return
    }


    const eventKey =
      `${eventCode}:${rtpPacket.header.timestamp}`


    if (
      eventKey ===
      this.lastDtmfEventKey
    ) {
      return
    }


    this.lastDtmfEventKey =
      eventKey


    console.log(
      `SIP - DTMF received: ${digit}`
    )


    this.emit(
      'dtmf',
      digit
    )
  }


  _handleInfo(
    request
  ) {

    const content =
      request.content ||
      ''


    const match =
      content.match(
        /Signal\s*=\s*([0-9A-D*#])/i
      )


    sipLib.send(
      sipLib.makeResponse(
        request,
        200,
        'OK'
      )
    )


    if (
      !match
    ) {
      return
    }


    const digit =
      match[1]
        .toUpperCase()


    console.log(
      `SIP - DTMF received via INFO: ${digit}`
    )


    this.emit(
      'dtmf',
      digit
    )
  }


  sendAudioPacket(
    rtp,
    isTone = false
  ) {

    if (
      !this.serverRtpInfo
    ) {
      return
    }


    const shouldForward =
      this.rtpSequencer.process(
        rtp,
        isTone
      )


    if (
      !shouldForward
    ) {
      return
    }


    rtp.header.payloadType =
      this.serverRtpInfo
        .payloadType


    this.udp.send(
      rtp.serialize(),

      this.serverRtpInfo.port,

      this.serverRtpInfo.destination
    )
  }


  _retryWithDigestAuth(
    request,
    response,
    successLog,
    errorLog,
    callback
  ) {

    console.log(
      'SIP - Unauthorized (401). Retrying with Digest Authentication...'
    )


    digest.signRequest(
      this.authSession,
      request,
      response,
      {
        user:
          SIP_USER,

        password:
          SIP_PASS
      }
    )


    request.headers
      .cseq.seq += 1


    sipLib.send(
      request,

      authResp => {

        if (
          authResp.status >=
            200 &&
          authResp.status <
            300
        ) {

          console.log(
            successLog
          )

        } else if (
          authResp.status >=
          300
        ) {

          console.error(
            `${errorLog} ${authResp.status} ${authResp.reason}`
          )
        }


        callback(
          authResp
        )
      }
    )
  }


  _handleBye(
    request
  ) {

    const ourCallId =
      this.sipSession
        ?.headers
        ?.['call-id']


    const receivedCallId =
      request.headers[
        'call-id'
      ]


    sipLib.send(
      sipLib.makeResponse(
        request,
        200,
        'OK'
      )
    )


    if (
      ourCallId &&
      receivedCallId &&
      ourCallId ===
        receivedCallId
    ) {

      console.log(
        'SIP - Received BYE. Ending call.'
      )


      this.sipSession =
        null

      this.initiatingCall =
        false


      this.emit(
        'callEnded'
      )
    }
  }


  _handleInboundInvite(
    request
  ) {

    if (
      this.initiatingCall
    ) {
      return
    }


    this.initiatingCall =
      true


    console.log(
      'SIP - Inbound call, checking OPUS...'
    )


    this.currentCallId =
      request.headers[
        'call-id'
      ]


    const remoteInfo =
      this._parseSdpForOpus(
        request.content ||
        ''
      )


    if (
      !remoteInfo
    ) {

      console.log(
        'SIP - Remote did not offer OPUS.'
      )


      sipLib.send(
        sipLib.makeResponse(
          request,
          488,
          'Not Acceptable Here'
        )
      )


      this.initiatingCall =
        false

      return
    }


    this.serverRtpInfo =
      remoteInfo


    this.remoteDtmfPayloadType =
      remoteInfo
        .dtmfPayloadType


    sipLib.send(
      sipLib.makeResponse(
        request,
        100,
        'Trying'
      )
    )


    this.emit(
      'inboundCall'
    )


    sipLib.send(
      sipLib.makeResponse(
        request,
        180,
        'Ringing'
      )
    )


    const sessionId =
      Date.now()


    const okResponse =
      sipLib.makeResponse(
        request,
        200,
        'OK'
      )


    okResponse.headers[
      'content-type'
    ] =
      'application/sdp'


    okResponse.content =
      this._buildLocalSdp(
        sessionId
      )


    sipLib.send(
      okResponse
    )


    this.sipSession =
      okResponse


    this.emit(
      'callEstablished',
      this.serverRtpInfo
    )
  }


  _handleInviteResponse(
    response
  ) {

    if (
      !this.inviteRequest
    ) {
      return
    }


    if (
      response.status ===
        401 &&
      response.headers[
        'www-authenticate'
      ]
    ) {

      this._retryWithDigestAuth(
        this.inviteRequest,
        response,

        'SIP - INVITE success (after auth).',

        'SIP - INVITE failed:',

        authResp => {

          this._handleInviteResponse(
            authResp
          )
        }
      )


      return
    }


    if (
      response.status >=
        100 &&
      response.status <
        200
    ) {

      if (
        response.status ===
        180
      ) {

        this.emit(
          'ringing'
        )
      }


      return
    }


    if (
      response.status >=
        200 &&
      response.status <
        300
    ) {

      console.log(
        `SIP - Call established: ${response.status} ${response.reason}`
      )


      this.sipSession =
        response


      this.serverRtpInfo =
        this._parseSdpForOpus(
          response.content
        )


      if (
        !this.serverRtpInfo
      ) {

        console.error(
          'SIP - 200 OK received but no usable OPUS SDP found.'
        )


        this.emit(
          'callFailed',
          {
            status:
              488,

            reason:
              'No OPUS in SDP answer'
          }
        )


        return
      }


      this.remoteDtmfPayloadType =
        this.serverRtpInfo
          .dtmfPayloadType


      console.log(
        `SIP - Remote DTMF payload type: ${this.remoteDtmfPayloadType ?? 'not negotiated'}`
      )


      const ackUri =
        response.headers
          ?.contact?.[0]?.uri ||
        this.inviteRequest.uri


      sipLib.send(
        {
          method:
            'ACK',

          uri:
            ackUri,

          headers: {

            to:
              response.headers.to,

            from:
              response.headers.from,

            'call-id':
              response.headers[
                'call-id'
              ],

            cseq: {
              method:
                'ACK',

              seq:
                response.headers
                  .cseq.seq
            },

            via:
              response.headers.via
          }
        }
      )


      this.emit(
        'callEstablished',
        this.serverRtpInfo
      )


      return
    }


    console.error(
      `SIP - Call failed: ${response.status} ${response.reason}`
    )


    this.inviteRequest =
      null

    this.initiatingCall =
      false


    this.emit(
      'callFailed',
      {
        status:
          response.status,

        reason:
          response.reason
      }
    )
  }


  _parseSdpForOpus(
    sdp
  ) {

    if (
      !sdp
    ) {
      return null
    }


    const lines =
      sdp
        .split(
          /\r?\n/
        )
        .map(
          line =>
            line.trim()
        )


    const mLine =
      lines.find(
        line =>
          line.startsWith(
            'm=audio'
          )
      )


    if (
      !mLine
    ) {
      return null
    }


    const mMatch =
      mLine.match(
        /m=audio\s+(\d+)\s+RTP\/\S+\s+(.+)/
      )


    if (
      !mMatch
    ) {
      return null
    }


    const port =
      parseInt(
        mMatch[1],
        10
      )


    const payloadTypesInM =
      mMatch[2]
        .split(
          /\s+/
        )
        .map(
          x =>
            parseInt(
              x,
              10
            )
        )
        .filter(
          x =>
            !Number.isNaN(
              x
            )
        )


    let destination =
      SIP_DOMAIN


    const cLine =
      lines.find(
        line =>
          line.startsWith(
            'c=IN IP4'
          )
      )


    if (
      cLine
    ) {

      const cMatch =
        cLine.match(
          /c=IN IP4\s+(\S+)/
        )


      if (
        cMatch
      ) {

        destination =
          cMatch[1]
      }
    }


    let opusPayloadType =
      null

    let dtmfPayloadType =
      null


    for (
      const line of
      lines
    ) {

      if (
        !line.startsWith(
          'a=rtpmap:'
        )
      ) {
        continue
      }


      const match =
        line.match(
          /a=rtpmap:(\d+)\s+([\w-]+)/
        )


      if (
        !match
      ) {
        continue
      }


      const pt =
        parseInt(
          match[1],
          10
        )


      const codec =
        match[2]
          .toUpperCase()


      if (
        !payloadTypesInM.includes(
          pt
        )
      ) {
        continue
      }


      if (
        codec ===
        'OPUS'
      ) {

        opusPayloadType =
          pt
      }


      if (
        codec ===
        'TELEPHONE-EVENT'
      ) {

        dtmfPayloadType =
          pt
      }
    }


    if (
      opusPayloadType ===
      null
    ) {
      return null
    }


    console.log(
      `SIP - Parsed SDP: destination=${destination}, port=${port}, opusPayload=${opusPayloadType}, dtmfPayload=${dtmfPayloadType ?? 'none'}`
    )


    return {
      destination,
      port,

      payloadType:
        opusPayloadType,

      dtmfPayloadType
    }
  }


  _buildLocalSdp(
    sessionId
  ) {

    const rtpPort =
      this.localRtpPort ||
      Number(
        LOCAL_RTP_PORT
      )


    /*
     * Offer both:
     *
     *   PT 96  = Opus
     *   PT 101 = RFC4733 telephone-event
     *
     * This lets Asterisk send "*" to us as RTP DTMF while
     * leaving the voice stream entirely Opus.
     */

    return [

      'v=0',

      `o=- ${sessionId} ${sessionId} IN IP4 ${LOCAL_IP}`,

      's=-',

      `c=IN IP4 ${LOCAL_IP}`,

      't=0 0',

      `m=audio ${rtpPort} RTP/AVP 96 ${LOCAL_DTMF_PAYLOAD_TYPE}`,

      'a=rtpmap:96 opus/48000/2',

      'a=fmtp:96 useinbandfec=1',

      `a=rtpmap:${LOCAL_DTMF_PAYLOAD_TYPE} telephone-event/8000`,

      `a=fmtp:${LOCAL_DTMF_PAYLOAD_TYPE} 0-16`,

      'a=sendrecv'

    ].join(
      '\r\n'
    ) + '\r\n'
  }
}


export const sip =
  new Sip()
