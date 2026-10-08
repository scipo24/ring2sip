import { RingApi } from 'ring-client-api'
import { EventEmitter } from 'events'

import {
  getRefreshTokenFromEnv,
  updateRefreshTokenInEnv
} from './util.js'

import RtpSequencer from './rtp-sequencer.js'
import OpusTranscoder from './opus-transcoder.js'
import SipToRingProcessor from './sip-to-ring-processor.js'

import {
  WebrtcConnection
} from './node_modules/ring-client-api/lib/streaming/webrtc-connection.js'

import {
  appApi
} from './node_modules/ring-client-api/lib/rest-client.js'


const {
  CAMERA_NAME
} = process.env


class IntercomWebrtcConnection extends WebrtcConnection {

  async initiateCall() {

    const {
      sdp
    } = await this.pc.createOffer()


    console.log(
      'RING SIGNAL - Sending live_view offer'
    )


    this.sendMessage({
      method: 'live_view',

      dialog_id:
        this.dialogId,

      body: {

        doorbot_id:
          this.camera.id,

        stream_options: {
          audio_enabled: true,
          video_enabled: true
        },

        sdp,

        type: 'offer'
      }
    })


    this.onOfferSent.next()
  }


  activate() {

    console.log(
      'RING SIGNAL - Sending activate_session'
    )


    this.sendSessionMessage(
      'activate_session'
    )


    console.log(
      'RING SIGNAL - Sending stream_options audio=true video=true'
    )


    this.sendSessionMessage(
      'stream_options',
      {
        audio_enabled: true,
        video_enabled: true
      }
    )
  }
}


class Ring extends EventEmitter {

  constructor() {

    super()

    this.ringApi = null
    this.intercom = null
    this.connection = null
    this.sip = null

    /*
     * Ring -> SIP processor
     */
    this.transcoder = null

    /*
     * SIP -> Ring processor
     */
    this.sipToRingProcessor = null

    this.initiatingCall = false
    this.receivingAudio = false

    /*
     * Used to keep RTP sequence numbers continuous when
     * switching from generated tones to real speech.
     */
    this.rtpSequencer =
      new RtpSequencer()

    this.battery = null
    this.connected = false


    this.audioStatsTimer = null

    this.audioStats = {
      packets: 0,
      bytes: 0,
      minPayload: null,
      maxPayload: null,
      timestampDeltaTotal: 0,
      timestampDeltaCount: 0
    }

    this.lastRingTimestamp = null
  }


  _resetAudioStats() {

    this.audioStats = {
      packets: 0,
      bytes: 0,
      minPayload: null,
      maxPayload: null,
      timestampDeltaTotal: 0,
      timestampDeltaCount: 0
    }
  }


  _startAudioStats() {

    if (
      this.audioStatsTimer
    ) {
      return
    }


    this._resetAudioStats()


    this.audioStatsTimer =
      setInterval(
        () => {

          const stats =
            this.audioStats


          if (
            stats.packets === 0
          ) {

            console.log(
              'RING AUDIO STATS packets=0'
            )

            return
          }


          const avgPayload =
            stats.bytes /
            stats.packets


          const avgDelta =
            stats.timestampDeltaCount > 0
              ? stats.timestampDeltaTotal /
                stats.timestampDeltaCount
              : null


          const avgMs =
            avgDelta !== null
              ? avgDelta / 48
              : null


          console.log(
            'RING AUDIO STATS ' +
            `packets=${stats.packets} ` +
            `bytes=${stats.bytes} ` +
            `avgPayload=${avgPayload.toFixed(1)} ` +
            `minPayload=${stats.minPayload} ` +
            `maxPayload=${stats.maxPayload} ` +
            `avgDelta=${avgDelta !== null ? avgDelta.toFixed(1) : 'n/a'} ` +
            `avgPacketMs=${avgMs !== null ? avgMs.toFixed(1) : 'n/a'}`
          )


          this._resetAudioStats()

        },
        1000
      )
  }


  _stopAudioStats() {

    if (
      this.audioStatsTimer
    ) {

      clearInterval(
        this.audioStatsTimer
      )

      this.audioStatsTimer =
        null
    }


    this._resetAudioStats()

    this.lastRingTimestamp =
      null
  }


  _recordAudioPacket(
    rtpPacket
  ) {

    const payloadBytes =
      rtpPacket.payload?.length ?? 0


    const timestamp =
      rtpPacket.header.timestamp


    const stats =
      this.audioStats


    stats.packets++
    stats.bytes += payloadBytes


    if (
      stats.minPayload === null ||
      payloadBytes < stats.minPayload
    ) {

      stats.minPayload =
        payloadBytes
    }


    if (
      stats.maxPayload === null ||
      payloadBytes > stats.maxPayload
    ) {

      stats.maxPayload =
        payloadBytes
    }


    if (
      this.lastRingTimestamp !==
      null
    ) {

      const delta =
        (
          timestamp -
          this.lastRingTimestamp
        ) >>> 0


      stats.timestampDeltaTotal +=
        delta

      stats.timestampDeltaCount++
    }


    this.lastRingTimestamp =
      timestamp
  }


  _installPushDebug() {

    if (
      !this.intercom ||
      this.intercom.__pushDebugInstalled
    ) {
      return
    }


    const originalProcessPushNotification =
      this.intercom
        .processPushNotification
        .bind(
          this.intercom
        )


    this.intercom.processPushNotification =
      notification => {

        try {

          console.log(
            'RING PUSH JSON ' +
            JSON.stringify(
              notification
            )
          )

        } catch (_) {}


        return originalProcessPushNotification(
          notification
        )
      }


    this.intercom.__pushDebugInstalled =
      true
  }


  async initialize() {

    const refreshToken =
      getRefreshTokenFromEnv()


    this.ringApi =
      new RingApi({
        refreshToken,
        cameraStatusPollingSeconds: 600,
        debug: true
      })


    this.ringApi
      .onRefreshTokenUpdated
      .subscribe(
        async ({
          newRefreshToken
        }) => {

          console.log(
            'RING - Refresh token updated'
          )


          updateRefreshTokenInEnv(
            newRefreshToken
          )
        }
      )


    const locations =
      await this.ringApi
        .getLocations()


    const intercoms =
      locations.flatMap(
        location =>
          location.intercoms ||
          []
      )


    if (
      !intercoms.length
    ) {

      throw new Error(
        'No Ring Intercom devices found.'
      )
    }


    console.log(
      'RING - Intercom devices found:',
      intercoms
        .map(
          x =>
            `${x.name} (${x.deviceType})`
        )
        .join(', ')
    )


    if (
      CAMERA_NAME
    ) {

      this.intercom =
        intercoms.find(
          x =>
            x.name ===
            CAMERA_NAME
        )


      if (
        !this.intercom
      ) {

        throw new Error(
          `Ring Intercom "${CAMERA_NAME}" not found.`
        )
      }

    } else if (
      intercoms.length === 1
    ) {

      this.intercom =
        intercoms[0]


      console.log(
        `RING - CAMERA_NAME empty, automatically using "${this.intercom.name}"`
      )

    } else {

      throw new Error(
        'Multiple Ring Intercom devices found. Set CAMERA_NAME.'
      )
    }


    console.log(
      `RING - Using ${this.intercom.name} (${this.intercom.deviceType})`
    )


    this._installPushDebug()


    return true
  }


  async _createStreamingConnection() {

    const response =
      await this.intercom
        .restClient
        .request({
          method: 'POST',

          url:
            appApi(
              'clap/ticket/request/signalsocket'
            )
        })


    const connection =
      new IntercomWebrtcConnection(
        response.ticket,
        this.intercom,
        {}
      )


    connection
      .onMessage
      .subscribe(
        message => {

          const method =
            message?.method ??
            'unknown'


          const text =
            message?.body?.text


          console.log(
            'RING SIGNAL RX ' +
            `method=${method}` +
            `${text ? ` text=${text}` : ''}`
          )
        }
      )


    /*
     * Critical for actual Intercom audio.
     * On camera_connected this sends:
     *
     * camera_options { stealth_mode:false }
     */
    connection.activateCameraSpeaker()


    return connection
  }


  _createRingToSipProcessor() {

    if (
      this.transcoder
    ) {
      return
    }


    this.transcoder =
      new OpusTranscoder(
        rtpPacket => {

          if (
            this.sip
          ) {

            this.sip.sendAudioPacket(
              rtpPacket,
              false
            )
          }
        }
      )
  }


  _createSipToRingProcessor() {

    if (
      this.sipToRingProcessor
    ) {
      return
    }


    this.sipToRingProcessor =
      new SipToRingProcessor(
        rtpPacket => {

          if (
            !this.connection
          ) {
            return
          }


          /*
           * Preserve the tone -> speech RTP sequence logic.
           */
          const shouldForward =
            this.rtpSequencer.process(
              rtpPacket,
              false
            )


          if (
            !shouldForward
          ) {
            return
          }


          this.connection.sendAudioPacket(
            rtpPacket
          )
        }
      )
  }


  async initiateCall() {

    if (
      this.initiatingCall
    ) {

      console.log(
        'RING - Call already active/in progress; ignoring initiateCall.'
      )

      return
    }


    this.initiatingCall =
      true

    this.receivingAudio =
      false


    this._createRingToSipProcessor()

    this._createSipToRingProcessor()

    this._startAudioStats()


    try {

      console.log(
        `RING - Starting WebRTC call on Intercom: ${this.intercom.name}`
      )


      const connection =
        await this
          ._createStreamingConnection()


      this.connection =
        connection


      connection
        .onCameraConnected
        .subscribe(
          () => {

            console.log(
              'RING SIGNAL - camera_connected received'
            )
          }
        )


      connection
        .onSessionId
        .subscribe(
          sessionId => {

            console.log(
              `RING SIGNAL - Session id ${sessionId}`
            )
          }
        )


      connection
        .onCallEnded
        .subscribe(
          () => {

            console.log(
              'RING - Call ended'
            )


            this.connection =
              null

            this.initiatingCall =
              false


            this._stopAudioStats()


            this.emit(
              'callEnded'
            )
          }
        )


      connection
        .onCallAnswered
        .subscribe(
          sdp => {

            console.log(
              'RING - WebRTC call answered'
            )


            if (
              typeof sdp === 'string'
            ) {

              const audioDirection =
                sdp.match(
                  /m=audio[\s\S]*?a=(sendrecv|sendonly|recvonly|inactive)/
                )?.[1]


              console.log(
                `RING SIGNAL - SDP answer audio direction=${audioDirection ?? 'unknown'}`
              )
            }


            this.emit(
              'callEstablished'
            )
          }
        )


      connection
        .onAudioRtp
        .subscribe(
          rtpPacket => {

            if (
              !this.receivingAudio
            ) {

              this.receivingAudio =
                true


              console.log(
                'RING - Receiving audio RTP'
              )


              this.emit(
                'receivingAudio'
              )
            }


            this._recordAudioPacket(
              rtpPacket
            )


            if (
              this.transcoder
            ) {

              this.transcoder.feed(
                rtpPacket
              )
            }
          }
        )


      connection
        .onError
        .subscribe(
          err => {

            console.error(
              'RING - WebRTC error:',
              err
            )
          }
        )


      return true

    } catch (
      err
    ) {

      this.initiatingCall =
        false


      this._stopAudioStats()


      console.error(
        'RING - Error initiating Intercom call:',
        err
      )


      throw err
    }
  }


  listen() {

    console.log(
      `RING - Attaching ding listener to ${this.intercom.name}`
    )


    this.intercom
      .onDing
      .subscribe(
        () => {

          console.log(
            `RING - Intercom ding: ${this.intercom.name}`
          )


          this.emit(
            'buttonPressed',
            this.intercom
          )
        }
      )


    this.intercom
      .onData
      .subscribe(
        () => {

          this.battery =
            this.intercom
              .batteryLevel


          this.connected =
            !this.intercom
              .isOffline


          console.log(
            `RING - Battery ${this.battery}, connected ${this.connected}`
          )
        }
      )
  }


  isCallActive() {

    return Boolean(
      this.initiatingCall ||
      this.connection
    )
  }


  /*
   * Audio coming from SIP/Asterisk toward Ring.
   */
  sendAudioPacket(
    rtp,
    isTone = false
  ) {

    if (
      !this.connection
    ) {
      return
    }


    /*
     * Keep locally generated ringback tones direct.
     */
    if (
      isTone
    ) {

      const shouldForward =
        this.rtpSequencer.process(
          rtp,
          true
        )


      if (
        shouldForward
      ) {

        this.connection.sendAudioPacket(
          rtp
        )
      }


      return
    }


    /*
     * Real SIP speech goes through:
     *
     * gain +10 dB
     * filtering
     * limiter
     * recording
     */
    this._createSipToRingProcessor()


    this.sipToRingProcessor.feed(
      rtp
    )
  }


  pipeAudio(
    sip
  ) {

    this.sip =
      sip
  }


  async unlockDoor() {

    if (
      !this.intercom
    ) {

      console.error(
        'RING - Cannot unlock: Intercom not initialized.'
      )

      return false
    }


    try {

      console.log(
        `RING - Unlock requested for ${this.intercom.name}`
      )


      await this.intercom.unlock()


      console.log(
        `RING - Door unlock command accepted for ${this.intercom.name}`
      )


      this.emit(
        'doorUnlocked'
      )


      return true

    } catch (
      err
    ) {

      console.error(
        'RING - Door unlock failed:',
        err
      )


      this.emit(
        'doorUnlockFailed',
        err
      )


      return false
    }
  }



  getBattery() {

    return this.battery
  }


  isConnected() {

    return this.connected
  }


  cleanup() {

    this._stopAudioStats()


    if (
      this.connection
    ) {

      console.log(
        'RING - Stopping Intercom WebRTC call...'
      )


      this.connection.stop()

      this.connection =
        null
    }


    if (
      this.transcoder
    ) {

      this.transcoder.stop()

      this.transcoder =
        null
    }


    if (
      this.sipToRingProcessor
    ) {

      this.sipToRingProcessor.stop()

      this.sipToRingProcessor =
        null
    }


    this.initiatingCall =
      false

    this.receivingAudio =
      false
  }
}


export const ring =
  new Ring()
