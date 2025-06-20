const get = require("lodash/fp/get.js");
const pipe = require("lodash/fp/pipe.js");
const kefir = require("kefir");
const always = require("lodash/fp/always.js");
const matches = require("lodash/fp/matches.js");
const createHydroClient = require("../lib/hydrophonic.js");

const hydro = createHydroClient({
  socketPath: "/var/run/docker.sock",
});

const prg = `return "Hello World";`;

hydro
  .createContainer({
    name: "my-container",
    image: "node",
    entrypoint: "",
    cmd: ["-e", "setTimeout(function(){}, 10000000);"],
    env: {
      PATH: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      NODE_VERSION: "24.2.0",
      YARN_VERSION: "1.22.22",
    },
  })
  .flatMap((containerId) => {
    return kefir.concat([
      hydro.startContainer({ id: containerId }).ignoreValues(),
      hydro
        .createExec({
          containerId,
          stdout: true,
          cmd: [
            "node",
            "-e",
            `(async function(){${prg}})().then(process.stdout.write.bind(process.stdout))`,
          ],
          entrypoint: "node",
          workingDir: "/tmp",
          env: {
            PATH: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            NODE_VERSION: "24.2.0",
            YARN_VERSION: "1.22.22",
          },
        })
        .map((buf) => buf.toString())
        .flatMap((execId) => hydro.startExec({ id: execId }).skip(1)),
      hydro.removeContainer({ id: containerId, force: true }).ignoreValues(),
    ]);
  })
  .filter(matches({ type: "body", stream: "out" }))
  .map(get("payload"))
  .bufferWhile(always(true))
  .last()
  .map(pipe(Buffer.concat, (buf) => buf.toString()))
  .log();
