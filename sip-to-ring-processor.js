import { createSocket } from 'dgram'
import { spawn } from 'child_process'

import {
  writeFileSync,
  unlinkSync,
  existsSync
} from 'fs'

import { RtpPacket } from 'werift'


export default class SipToRingProcessor {

  constructor(onPacket) {

    this.onPacket = onPacket

    this.senderSocket = null
    this.outputSocket = null

    this.inputPort = null
    this.outputPort = null

    this.ffmpeg = null
    this.sdpPath = null

    this.started = false
    this.ready = false

    this.buffer = []

    this.inputPayloadType = null

    this.recordAudio =
      process.env.RECORD_AUDIO !== '0'

    this.originalWavPath = null
    this.processedWavPath = null

    this.debugCount = 0
  }


  async _getFreeUdpPort() {

    const socket =
      createSocket('udp4')


    const port =
      await new Promise(
        (resolve, reject) => {

          const onError =
            error => reject(error)


          socket.once(
            'error',
            onError
          )


          socket.bind(
            0,
            '127.0.0.1',
            () => {

              socket.off(
                'error',
                onError
              )


              resolve(
                socket.address().port
              )
            }
          )
        }
      )


    await new Promise(
      resolve =>
        socket.close(resolve)
    )


    return port
  }


  _makeRecordingPaths() {

    const stamp =
      new Date()
        .toISOString()
        .replace(/[:.]/g, '-')


    this.originalWavPath =
      `/recordings/sip-to-ring-original-${stamp}.wav`


    this.processedWavPath =
      `/recordings/sip-to-ring-processed-${stamp}.wav`


    console.log(
      `SIP->RING - Recording original to ${this.originalWavPath}`
    )


    console.log(
      `SIP->RING - Recording processed to ${this.processedWavPath}`
    )
  }


  async start(inputPayloadType) {

    if (this.started) {
      return
    }


    this.started = true

    this.inputPayloadType =
      inputPayloadType


    this.inputPort =
      await this._getFreeUdpPort()


    this.senderSocket =
      createSocket('udp4')


    this.outputSocket =
      createSocket('udp4')


    await new Promise(
      (resolve, reject) => {

        const onError =
          error => reject(error)


        this.outputSocket.once(
          'error',
          onError
        )


        this.outputSocket.bind(
          0,
          '127.0.0.1',
          () => {

            this.outputSocket.off(
              'error',
              onError
            )


            this.outputPort =
              this.outputSocket
                .address()
                .port


            resolve()
          }
        )
      }
    )


    this.outputSocket.on(
      'message',
      message => {

        try {

          const rtp =
            RtpPacket.deSerialize(
              message
            )


          if (
            this.debugCount < 20
          ) {

            console.log(
              `SIP->RING RTP OUT seq=${rtp.header.sequenceNumber} ` +
              `ts=${rtp.header.timestamp} ` +
              `payload=${rtp.payload?.length ?? 0} ` +
              `pt=${rtp.header.payloadType}`
            )

            this.debugCount++
          }


          this.onPacket(
            rtp
          )

        } catch (
          err
        ) {

          console.error(
            'SIP->RING - Invalid RTP output:',
            err
          )
        }
      }
    )


    const sdp = [

      'v=0',

      'o=- 0 0 IN IP4 127.0.0.1',

      's=Asterisk Opus Input',

      'c=IN IP4 127.0.0.1',

      't=0 0',

      `m=audio ${this.inputPort} RTP/AVP ${this.inputPayloadType}`,

      `a=rtpmap:${this.inputPayloadType} opus/48000/2`,

      `a=fmtp:${this.inputPayloadType} useinbandfec=1`,

      'a=recvonly'

    ].join('\r\n') + '\r\n'


    this.sdpPath =
      `/tmp/sip-to-ring-${process.pid}-${Date.now()}.sdp`


    writeFileSync(
      this.sdpPath,
      sdp
    )


    /*
     * SIP -> Ring
     *
     * The 210 recording showed the microphone path roughly
     * 13-14 dB below the Ring->SIP level.
     *
     * Start conservatively with +10 dB.
     *
     * HP/LP remove energy that is useless for an intercom,
     * then limiter prevents the extra gain from clipping.
     */
    const filterChain =
      [
        'highpass=f=120',

        'lowpass=f=4000',

        'volume=3.16227766',

        'alimiter=limit=0.90'
      ].join(',')


    let filterComplex


    if (this.recordAudio) {

      this._makeRecordingPaths()


      filterComplex =
        [
          '[0:a]asplit=2[original][processin]',

          `[processin]${filterChain},asplit=2[processedwav][processedrtp]`
        ].join(';')

    } else {

      filterComplex =
        `[0:a]${filterChain}[processedrtp]`
    }


    const ffmpegArgs = [

      '-hide_banner',

      '-loglevel',
      'warning',

      '-protocol_whitelist',
      'file,udp,rtp',

      '-fflags',
      'nobuffer',

      '-flags',
      'low_delay',

      '-i',
      this.sdpPath,

      '-filter_complex',
      filterComplex
    ]


    if (this.recordAudio) {

      ffmpegArgs.push(

        '-map',
        '[original]',

        '-c:a',
        'pcm_s16le',

        '-ar',
        '48000',

        '-ac',
        '2',

        '-y',

        this.originalWavPath,


        '-map',
        '[processedwav]',

        '-c:a',
        'pcm_s16le',

        '-ar',
        '48000',

        '-ac',
        '2',

        '-y',

        this.processedWavPath
      )
    }


    ffmpegArgs.push(

      '-map',
      '[processedrtp]',

      '-c:a',
      'libopus',

      '-application',
      'voip',

      '-ar',
      '48000',

      '-ac',
      '2',

      '-frame_duration',
      '20',

      '-f',
      'rtp',

      '-payload_type',
      '96',

      `rtp://127.0.0.1:${this.outputPort}`
    )


    console.log(
      'SIP->RING - Filter: HP120 -> LP4000 -> gain +10 dB -> limiter 0.90'
    )


    console.log(
      'SIP->RING - Starting FFmpeg:',
      ffmpegArgs.join(' ')
    )


    this.ffmpeg =
      spawn(
        'ffmpeg',
        ffmpegArgs,
        {
          stdio: [
            'ignore',
            'ignore',
            'pipe'
          ]
        }
      )


    this.ffmpeg.stderr.on(
      'data',
      data => {

        const msg =
          data
            .toString()
            .trim()


        if (msg) {

          console.log(
            `SIP->RING FFmpeg - ${msg}`
          )
        }
      }
    )


    this.ffmpeg.on(
      'exit',
      (
        code,
        signal
      ) => {

        console.log(
          `SIP->RING - FFmpeg exited code=${code} signal=${signal}`
        )


        this.ffmpeg =
          null

        this.ready =
          false
      }
    )


    setTimeout(
      () => {

        if (
          !this.senderSocket ||
          !this.ffmpeg
        ) {
          return
        }


        this.ready =
          true


        console.log(
          `SIP->RING - Ready. input=${this.inputPort}, output=${this.outputPort}`
        )


        for (
          const packet of
          this.buffer
        ) {

          this.senderSocket.send(
            packet,
            this.inputPort,
            '127.0.0.1'
          )
        }


        this.buffer = []

      },
      500
    )
  }


  feed(rtpPacket) {

    const packet =
      rtpPacket.serialize()


    if (!this.started) {

      this.buffer.push(
        packet
      )


      this.start(
        rtpPacket.header.payloadType
      ).catch(
        err => {

          console.error(
            'SIP->RING - Unable to start:',
            err
          )

          this.started =
            false
        }
      )


      return
    }


    if (!this.ready) {

      if (
        this.buffer.length < 100
      ) {

        this.buffer.push(
          packet
        )
      }

      return
    }


    this.senderSocket.send(
      packet,
      this.inputPort,
      '127.0.0.1'
    )
  }


  stop() {

    this.ready = false
    this.started = false


    if (this.ffmpeg) {

      try {
        this.ffmpeg.kill(
          'SIGINT'
        )
      } catch (_) {}

      this.ffmpeg = null
    }


    if (this.senderSocket) {

      try {
        this.senderSocket.close()
      } catch (_) {}

      this.senderSocket = null
    }


    if (this.outputSocket) {

      try {
        this.outputSocket.close()
      } catch (_) {}

      this.outputSocket = null
    }


    if (
      this.sdpPath &&
      existsSync(
        this.sdpPath
      )
    ) {

      try {
        unlinkSync(
          this.sdpPath
        )
      } catch (_) {}
    }


    this.sdpPath = null

    this.buffer = []

    this.inputPort = null
    this.outputPort = null

    this.debugCount = 0


    console.log(
      'SIP->RING - Stopped'
    )
  }
}
