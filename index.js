import 'dotenv/config'

import { sip } from './sip.js'
import { ring } from './ring.js'
import { tones } from './tones.js'
import { startHealthServer } from './health.js'


function timestamp() {
  return new Date().toISOString()
}


const originalLog =
  console.log.bind(console)

const originalError =
  console.error.bind(console)

const originalWarn =
  console.warn.bind(console)


console.log = (...args) => {
  originalLog(
    `[${timestamp()}]`,
    ...args
  )
}


console.error = (...args) => {
  originalError(
    `[${timestamp()}]`,
    ...args
  )
}


console.warn = (...args) => {
  originalWarn(
    `[${timestamp()}]`,
    ...args
  )
}


const {
  NOTIFY_URL
} = process.env


const health =
  startHealthServer()


let cleaningUp =
  false

let callInProgress =
  false


Promise.all([
  sip.initialize(),
  ring.initialize()
])
  .then(() => {

    tones.initialize(
      sip,
      ring
    )


    console.log(
      'INDEX - Initialized.'
    )


    /*
     * --------------------------------------------------------
     * SIP
     * --------------------------------------------------------
     */


    sip.on(
      'ringing',
      () => {

        console.log(
          'INDEX - SIP ringing'
        )

        tones.sipRinging()
      }
    )


    sip.on(
      'callEstablished',
      () => {

        callInProgress =
          true


        console.log(
          'INDEX - SIP call established'
        )


        tones.sipReady()


        ring.pipeAudio(
          sip
        )


        /*
         * IMPORTANT:
         *
         * For an incoming Intercom ding we deliberately do NOT
         * open Ring WebRTC while the house phones are ringing.
         *
         * Ring WebRTC starts only after somebody answers the
         * SIP call.
         *
         * This keeps the original physical intercom completely
         * independent while it is ringing.
         *
         * For a manual call to extension 220, Ring may already
         * be active because the inboundCall handler starts it.
         */
        if (
          !ring.isCallActive()
        ) {

          console.log(
            'INDEX - SIP answered. Starting Ring WebRTC now.'
          )


          ring.initiateCall()
            .then(
              () => {

                console.log(
                  'INDEX - Ring WebRTC initiation after SIP answer completed.'
                )
              }
            )
            .catch(
              err => {

                console.error(
                  'INDEX - Unable to start Ring after SIP answer:',
                  err
                )


                fullCleanup(
                  'Ring initiation after SIP answer failed'
                )
              }
            )
        }
      }
    )


    sip.on(
      'callFailed',
      err => {

        console.error(
          'INDEX - SIP call failed:',
          err
        )

        fullCleanup(
          'SIP call failed'
        )
      }
    )


    sip.on(
      'callEnded',
      () => {

        console.log(
          'INDEX - SIP call ended. Cleaning up everything.'
        )

        fullCleanup(
          'SIP call ended'
        )
      }
    )


    /*
     * Manual/diagnostic path:
     *
     * phone -> 220 -> Ring
     */
    sip.on(
      'inboundCall',
      () => {

        callInProgress =
          true


        console.log(
          'INDEX - Inbound SIP call. Initiating RING call.'
        )


        ring.initiateCall()
      }
    )


    /*
     * SIP - DTMF unlock handler
     *
     * "*" opens the entrance door, but only while Ring has
     * an active WebRTC call. This prevents "*" during an
     * ordinary telephone call from triggering the lock.
     */
    sip.on(
      'dtmf',
      digit => {

        console.log(
          `INDEX - DTMF received: ${digit}`
        )


        if (
          digit !== '*'
        ) {
          return
        }


        if (
          !ring.isCallActive()
        ) {

          console.log(
            'INDEX - Unlock ignored: no active Ring call.'
          )

          return
        }


        console.log(
          'INDEX - DTMF *: requesting door unlock.'
        )


        ring.unlockDoor()
          .catch(
            err => {

              console.error(
                'INDEX - Door unlock error:',
                err
              )
            }
          )
      }
    )



    /*
     * --------------------------------------------------------
     * RING
     * --------------------------------------------------------
     */


    ring.on(
      'callEstablished',
      () => {

        callInProgress =
          true


        console.log(
          'INDEX - Ring call established'
        )


        sip.pipeAudio(
          ring
        )
      }
    )


    ring.on(
      'receivingAudio',
      () => {

        console.log(
          'INDEX - Receiving audio from Ring'
        )


        tones.ringReady()
      }
    )


    ring.on(
      'callEnded',
      () => {

        console.log(
          'INDEX - Ring call ended. Cleaning up everything.'
        )


        fullCleanup(
          'Ring WebRTC ended'
        )
      }
    )


    /*
     * This is the new path.
     *
     * The active-ding REST monitor tells us that the physical
     * Intercom call has ended.
     *
     * We then terminate the SIP side and WebRTC together.
     */
    ring.on(
      'intercomEnded',
      reason => {

        console.log(
          `INDEX - Physical Intercom call ended (${reason}). Ending bridge.`
        )


        fullCleanup(
          `Intercom ended: ${reason}`
        )
      }
    )


    /*
     * --------------------------------------------------------
     * DING
     * --------------------------------------------------------
     */


    ring.on(
      'buttonPressed',
      intercom => {

        console.log(
          `INDEX - Button pressed for ${intercom.name}`
        )


        if (
          callInProgress ||
          ring.isCallActive()
        ) {

          console.log(
            'INDEX - Ding received while call already active; ignoring.'
          )

          return
        }


        notify()


        callInProgress =
          true


        console.log(
          'INDEX - Incoming Intercom ding: starting SIP ringing only.'
        )


        /*
         * Do NOT start Ring WebRTC here.
         *
         * The original intercom is left completely alone while
         * Asterisk rings the house phones.
         *
         * Ring WebRTC will be opened by the SIP callEstablished
         * handler only after somebody answers.
         */
        try {

          sip.initiateCall()


          console.log(
            'INDEX - SIP call initiated; waiting for somebody to answer before opening Ring WebRTC.'
          )

        } catch (
          err
        ) {

          console.error(
            'INDEX - Error initiating SIP call:',
            err
          )


          fullCleanup(
            'SIP call initiation failed'
          )
        }
      }
    )


    sip.register()

    ring.listen()
  })
  .catch(
    err => {

      console.error(
        'INDEX - Initialization failed:',
        err
      )

      process.exit(1)
    }
  )


process.on(
  'SIGINT',
  () => {

    console.log(
      'INDEX - Caught Ctrl+C.'
    )


    fullCleanup(
      'SIGINT'
    )
  }
)


process.on(
  'SIGTERM',
  () => {

    console.log(
      'INDEX - Caught SIGTERM.'
    )


    fullCleanup(
      'SIGTERM'
    )
  }
)


async function fullCleanup(
  reason = 'unspecified'
) {

  if (
    cleaningUp
  ) {

    console.log(
      'INDEX - Cleanup already in progress.'
    )

    return
  }


  cleaningUp =
    true

  callInProgress =
    false


  console.log(
    `INDEX - Starting cleanup. Reason: ${reason}`
  )


  sip.cleanup()

  ring.cleanup()

  tones.cleanup()


  try {

    await health.close()


    console.log(
      'INDEX - Health server closed.'
    )

  } catch (
    err
  ) {

    console.error(
      'INDEX - Error closing health server:',
      err
    )
  }


  setTimeout(
    () => {

      console.log(
        'INDEX - Exiting process.'
      )


      process.exit(0)

    },
    200
  )
}


function notify() {

  if (
    NOTIFY_URL &&
    NOTIFY_URL.startsWith(
      'http'
    )
  ) {

    fetch(
      NOTIFY_URL
    )
      .then(
        response =>
          response.text()
      )
      .then(
        data => {

          console.log(
            'INDEX - Notification sent successfully:',
            data
          )
        }
      )
      .catch(
        error => {

          console.error(
            'INDEX - Error sending notification:',
            error
          )
        }
      )

  } else {

    console.log(
      'INDEX - NOTIFY_URL is not set or invalid.'
    )
  }
}
