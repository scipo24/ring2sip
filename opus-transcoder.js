import { createSocket } from 'dgram'
import { spawn } from 'child_process'
import {
  writeFileSync,
  unlinkSync,
  existsSync
} from 'fs'

import { RtpPacket } from 'werift'


export default class OpusTranscoder {

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

    this.debugCount = 0
    this.lastOutputTimestamp = null

    this.recordAudio =
      process.env.RECORD_AUDIO !== '0'

    this.originalWavPath = null
    this.filteredWavPath = null
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
        .replace(
          /[:.]/g,
          '-'
        )


    this.originalWavPath =
      `/recordings/ring-original-${stamp}.wav`


    this.filteredWavPath =
      `/recordings/ring-filtered-${stamp}.wav`


    console.log(
      `TRANSCODER - Recording original audio to ${this.originalWavPath}`
    )


    console.log(
      `TRANSCODER - Recording filtered audio to ${this.filteredWavPath}`
    )
  }


  async start(inputPayloadType) {

    if (
      this.started
    ) {
      return
    }


    this.started = true

    this.inputPayloadType =
      inputPayloadType


    /*
     * FFmpeg opens this RTP input port itself.
     */
    this.inputPort =
      await this._getFreeUdpPort()


    this.senderSocket =
      createSocket('udp4')


    /*
     * FFmpeg sends the processed Opus RTP here.
     */
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
            this.debugCount < 30
          ) {

            const ts =
              rtp.header.timestamp


            let delta =
              null


            if (
              this.lastOutputTimestamp !==
              null
            ) {

              delta =
                (
                  ts -
                  this.lastOutputTimestamp
                ) >>> 0
            }


            console.log(
              `TRANSCODER RTP OUT seq=${rtp.header.sequenceNumber} ` +
              `ts=${ts} ` +
              `delta=${delta} ` +
              `payload=${rtp.payload?.length ?? 0} ` +
              `pt=${rtp.header.payloadType}`
            )


            this.lastOutputTimestamp =
              ts


            this.debugCount++
          }


          this.onPacket(
            rtp
          )

        } catch (
          err
        ) {

          console.error(
            'TRANSCODER - Invalid RTP output:',
            err
          )
        }
      }
    )


    /*
     * SDP describing the RTP coming FROM Ring.
     *
     * Ring uses Opus with the standard 48 kHz RTP clock.
     */
    const sdp = [

      'v=0',

      'o=- 0 0 IN IP4 127.0.0.1',

      's=Ring Intercom Opus Input',

      'c=IN IP4 127.0.0.1',

      't=0 0',

      `m=audio ${this.inputPort} RTP/AVP ${this.inputPayloadType}`,

      `a=rtpmap:${this.inputPayloadType} opus/48000/2`,

      `a=fmtp:${this.inputPayloadType} useinbandfec=1`,

      'a=recvonly'

    ].join('\r\n') + '\r\n'


    this.sdpPath =
      `/tmp/ring-opus-${process.pid}-${Date.now()}.sdp`


    writeFileSync(
      this.sdpPath,
      sdp
    )


    /*
     * ========================================================
     * FINAL RING -> SIP FILTER
     * ========================================================
     *
     * This is the F3 profile selected after offline listening.
     *
     * Stable high-frequency electrical whistles:
     *   5170 Hz
     *   5805 Hz
     *
     * Low/mid-frequency electrical comb/crackle:
     *   200 Hz
     *   250 Hz
     *   350 Hz
     *   400 Hz
     *   550 Hz
     *   850 Hz
     *
     * Final speech band:
     *   HP 180 Hz
     *   LP 3400 Hz
     *
     * After filtering we deliberately reduce to:
     *
     *   8000 Hz / mono
     *
     * because this gave the clearest intercom audio during
     * offline listening tests.
     *
     * Before encoding to Opus for SIP we convert back to
     * 48000 Hz stereo. This does NOT restore removed bandwidth;
     * it only keeps the RTP/Opus format consistent with the
     * SIP SDP (opus/48000/2).
     */

    const finalFilter =
      [

        /*
         * Impulsive electrical clicks.
         */
        'adeclick=w=55:o=90:a=4:t=1.5:b=4:m=s',


        /*
         * Fixed high-frequency whistles.
         */
        'equalizer=f=5170:t=q:w=8:g=-30',

        'equalizer=f=5805:t=q:w=8:g=-30',


        /*
         * Remove useless very-low-frequency energy.
         */
        'highpass=f=180',


        /*
         * F3 electrical-comb suppression.
         */
        'equalizer=f=200:t=q:w=28:g=-18',

        'equalizer=f=250:t=q:w=28:g=-18',

        'equalizer=f=350:t=q:w=28:g=-22',

        'equalizer=f=400:t=q:w=30:g=-16',

        'equalizer=f=550:t=q:w=30:g=-18',

        'equalizer=f=850:t=q:w=32:g=-14',


        /*
         * Final intercom speech bandwidth.
         */
        'lowpass=f=3400',


        /*
         * Control remaining peaks.
         */
        'alimiter=limit=0.70',


        /*
         * The selected "telephone/intercom" sound.
         *
         * First convert to mono, then to 8 kHz.
         */
        'pan=mono|c0=0.5*c0+0.5*c1',

        'aresample=8000'

      ].join(',')


    let filterComplex


    if (
      this.recordAudio
    ) {

      this._makeRecordingPaths()


      filterComplex =
        [

          /*
           * Keep untouched Ring audio for diagnostics.
           */
          '[0:a]asplit=2[original][filterin]',


          /*
           * Apply final selected filter.
           *
           * filtered8k is the actual signal we selected by ear.
           */
          `[filterin]${finalFilter},asplit=2[filteredwav][sipin]`,


          /*
           * Convert ONLY the SIP branch back to the Opus RTP
           * transport format expected by our SIP SDP.
           *
           * No useful high frequencies are recreated here.
           */
          '[sipin]' +
            'aresample=48000,' +
            'pan=stereo|c0=c0|c1=c0' +
            '[filteredrtp]'

        ].join(';')

    } else {

      filterComplex =
        '[0:a]' +
        finalFilter +
        ',' +
        'aresample=48000,' +
        'pan=stereo|c0=c0|c1=c0' +
        '[filteredrtp]'
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


    if (
      this.recordAudio
    ) {

      /*
       * ORIGINAL WAV
       *
       * Preserve original decoded Ring format for analysis.
       */
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

        this.originalWavPath
      )


      /*
       * FILTERED WAV
       *
       * Store exactly the 8 kHz mono signal we selected
       * during offline comparison.
       */
      ffmpegArgs.push(

        '-map',
        '[filteredwav]',

        '-c:a',
        'pcm_s16le',

        '-ar',
        '8000',

        '-ac',
        '1',

        '-y',

        this.filteredWavPath
      )
    }


    /*
     * SIP OUTPUT
     *
     * The useful audio bandwidth remains the 8 kHz/mono
     * intercom signal, but it is wrapped in Opus 48 kHz stereo
     * to remain compatible with our existing SDP and Asterisk.
     */
    ffmpegArgs.push(

      '-map',
      '[filteredrtp]',

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
      'TRANSCODER - FINAL Ring->SIP filter: ' +
      'declick -> ' +
      'notch 5170/5805 -> ' +
      'HP180 -> ' +
      'F3 notches 200/250/350/400/550/850 -> ' +
      'LP3400 -> limiter 0.70 -> ' +
      '8 kHz mono -> Opus 48 kHz stereo transport'
    )


    console.log(
      'TRANSCODER - Starting FFmpeg:',
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


        if (
          msg
        ) {

          console.log(
            `TRANSCODER FFmpeg - ${msg}`
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
          `TRANSCODER - FFmpeg exited code=${code} signal=${signal}`
        )


        if (
          this.recordAudio
        ) {

          console.log(
            `TRANSCODER - Original recording: ${this.originalWavPath}`
          )

          console.log(
            `TRANSCODER - Filtered recording: ${this.filteredWavPath}`
          )
        }


        this.ffmpeg =
          null

        this.ready =
          false
      }
    )


    /*
     * Give FFmpeg enough time to read the SDP, initialize the
     * decoder/filter graph/encoder and bind the RTP input.
     */
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
          `TRANSCODER - Ready. Ring RTP input=${this.inputPort}, output=${this.outputPort}`
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


  feed(
    rtpPacket
  ) {

    const packet =
      rtpPacket.serialize()


    if (
      !this.started
    ) {

      this.buffer.push(
        packet
      )


      this.start(
        rtpPacket
          .header
          .payloadType
      )
        .catch(
          err => {

            console.error(
              'TRANSCODER - Unable to start:',
              err
            )


            this.started =
              false
          }
        )


      return
    }


    if (
      !this.ready
    ) {

      /*
       * Ring sends roughly one packet every 60 ms.
       * 50 packets ~= 3 seconds.
       */
      if (
        this.buffer.length <
        50
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

    this.ready =
      false

    this.started =
      false


    if (
      this.ffmpeg
    ) {

      try {

        /*
         * SIGINT allows FFmpeg to finalize WAV headers.
         */
        this.ffmpeg.kill(
          'SIGINT'
        )

      } catch (_) {}


      this.ffmpeg =
        null
    }


    if (
      this.senderSocket
    ) {

      try {

        this.senderSocket.close()

      } catch (_) {}


      this.senderSocket =
        null
    }


    if (
      this.outputSocket
    ) {

      try {

        this.outputSocket.close()

      } catch (_) {}


      this.outputSocket =
        null
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


    this.sdpPath =
      null


    this.buffer = []


    this.inputPort =
      null

    this.outputPort =
      null


    this.lastOutputTimestamp =
      null

    this.debugCount =
      0


    console.log(
      'TRANSCODER - Stopped'
    )
  }
}
