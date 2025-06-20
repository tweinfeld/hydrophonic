const
    _ = require('lodash'),
    fp = require('lodash/fp'),
    qs = require('querystring'),
    url = require('url'),
    http = require('http'),
    kefir = require('kefir'),
    https = require('https'),
    { PassThrough } = require('stream');

const
    DOCKER_ENGINE_API_VERSION = "1.37",
    HTTP_RESPONSE = "http",
    TCP_RESPONSE = "tcp";

const
    isOkHttpHeader = (statusCode)=> ~~(statusCode / 100) === 2,
    toArgBoolean = (bol)=> bol ? "1" : "0",
    serializeEnv = (arr)=> _.map(arr, (value, key)=> `${key}=${value}`),
    httpResponseStreamer = ($stream)=> {
        return $stream
            .flatMap(({ req, res })=> {
                let stream =  kefir
                    .merge([
                        kefir.constant(({
                            type: "header",
                            status_code: res.statusCode,
                            headers: res.headers
                        })),
                        kefir
                            .fromEvents(res, 'data')
                            .takeUntilBy((kefir.fromEvents(res, 'end').take(1)))
                            .map((body)=> ({ type: "body", body }))
                    ])
                    .takeUntilBy(kefir.fromEvents(req, 'close').take(1));

                return !isOkHttpHeader(res.statusCode) ? stream.thru(decodeResponse).map((err)=> ({ status_code: res.statusCode, err })).flatMap(kefir.constantError) : stream;
            });
    },
    tcpResponseStreamer = ($stream)=> {
        return $stream
            .flatMap(({ type, res, socket })=> {

                return res.statusCode === 101
                    ? kefir
                        .stream(({emit}) => {
                            let
                                buffer = Buffer.allocUnsafe(0),
                                processor;

                            const
                                readHeader = function () {
                                    if (buffer.length >= 8) {
                                        let
                                            type = buffer[0],
                                            size = buffer.readUInt32BE(4);

                                        buffer = buffer.slice(8);
                                        processor = readData(type, size);
                                        processor();
                                    }
                                },
                                readData = (mode, length) => () => {
                                    if (buffer.length >= length) {
                                        emit({
                                            type: "body",
                                            stream: mode === 1 ? "out" : "err",
                                            payload: buffer.slice(0, length)
                                        });
                                        buffer = buffer.slice(length);
                                        processor = readHeader;
                                        processor();
                                    }
                                };

                            processor = readHeader;

                            let handler = (chunk) => {
                                buffer = Buffer.concat([buffer, chunk]);
                                processor();
                            };

                            emit({ type: "listening", socket });

                            socket.on('data', handler);
                            return () => socket.removeListener('data', handler);
                        })
                        .takeUntilBy(kefir.fromEvents(socket, 'end').take(1))
                    : kefir.constantError({ status_code: res.statusCode });
            })
    },
    bufferResponse = ($res)=> $res.filter(_.matchesProperty('type', 'body')).map(_.property('body')).scan(_.concat, []).last().map(Buffer.concat),
    decodeResponse = (function(decoders){
        return ($res)=> kefir
            .combine(
                [
                    $res.filter(_.matchesProperty('type', 'header')).map(_.property('headers.content-type')),
                    bufferResponse($res)
                ],
                (contentType, body)=> (decoders.find(({ test })=> test(contentType)) || { decode: _.identity }).decode(body)
            )
            .takeErrors(1);
    })([
        { test: (contentType)=> /^application\/json/i.test(contentType), decode: (body)=> JSON.parse(body.toString('utf8')) }
    ]);

module.exports = function ({
                               ca,
                               cert,
                               key,
                               url: baseUrl,
                               socketPath
                           } = {}){

    const client = function({ path, method = "GET", qs: queryString = {}, json, stream, headers = {}, response = HTTP_RESPONSE }){

        const jsonPayload = JSON.stringify(json);

        return kefir
            .later()
            .flatMap(()=> {
                const req = (socketPath ? http : https).request({
                    ...(function(){
                        return baseUrl
                            ? url.parse([[baseUrl, `v${DOCKER_ENGINE_API_VERSION}`, path].join('/'), qs.stringify(queryString)].filter(Boolean).join('?'))
                            : {
                                socketPath,
                                path: [[`/v${DOCKER_ENGINE_API_VERSION}`, path].join('/'), qs.stringify(queryString)].filter(Boolean).join('?')
                            };
                    })(),
                    ca,
                    cert,
                    key,
                    method,
                    headers: Object.assign(
                        headers,
                        json && {
                            "Content-Type": "application/json",
                            "Content-Length": jsonPayload.length
                        },
                        response === TCP_RESPONSE && {
                            "Connection": "Upgrade",
                            "Upgrade": "tcp"
                        }
                    )
                });

                (stream || (function(){ let pt = new PassThrough; pt.end(jsonPayload); return pt; })()).pipe(req);

                return kefir
                    .merge([
                        kefir.fromEvents(req, 'error').flatMap(kefir.constantError),
                        kefir.fromEvents(req, 'response', (res)=> ({ req, res })),
                        response === TCP_RESPONSE ? kefir.fromEvents(req, 'upgrade', (res, socket)=> ({ req, res, socket })) : kefir.never()
                    ])
                    .take(1)
                    .takeErrors(1)
                    .thru(response === HTTP_RESPONSE
                        ? httpResponseStreamer
                        : tcpResponseStreamer
                    );
            });
    };

    return {
        getVersion: ()=> client({ path: "version" }).thru(decodeResponse).map(_.property('Version')).toProperty(),

        getImageList: ()=> client({ path: "images/json" }).thru(decodeResponse).map(fp.map(fp.property('Id'))).toProperty(),

        getContainerList: ()=> client({ path: "containers/json" }).thru(decodeResponse).map(fp.map(fp.property('Id'))).toProperty(),

        getImage: ({ id })=> client({ path: `images/${id}/json` }).thru(decodeResponse),

        pullImage: ({ id })=> {
            let [image, tag = "latest"] = id.split(':');
            return client({ path: "images/create", method: "POST", qs: { fromImage: image, tag } })
                .filter(_.matchesProperty('type', 'body'))
                .map(fp.pipe(fp.property('body'), fp.toString, fp.split(/\r?\n/), fp.compact, fp.map((line)=> _.attempt(()=> JSON.parse(line)))))
                .flatten()
                .filter((progressObj)=>
                    _(progressObj)
                        .chain()
                        .at(["id", "progressDetail"])
                        .map(_.negate(_.isEmpty))
                        .compact()
                        .size()
                        .value() === 2
                )
                .scan((progressAcc, progressObj)=> {
                    let [id, type, current, total] = _.at(progressObj, ["id", "status", "progressDetail.current", "progressDetail.total"]);
                    return _.merge(progressAcc || {}, type === "Downloading" && { [id]: { current, total } });
                })
                .map((downloadProgress)=> {
                    let allProgress = _.values(downloadProgress);
                    return { "download_progress": allProgress.map(_.property('current')).reduce(_.add, 0) / allProgress.map(_.property('total')).reduce(_.add, 0) };
                });
        },

        createVolume: ({ name })=>
            client({
                path: "volumes/create",
                method: "POST",
                json: { name }
            })
                .thru(decodeResponse)
                .map(fp.get('Name'))
                .toProperty(),

        removeVolume: ({ name, force = true })=>
            client({
                method: "DELETE",
                path: `volumes/${name}`,
                qs: { force }
            })
            .thru(decodeResponse)
            .toProperty(),

        createContainer: ({ name, image, cmd, link, entrypoint, env, binds, privileged = false })=> client({
            path: "containers/create",
            method: "POST",
            qs: { name },
            json: Object.assign(
                {
                    "Image": image,
                    "HostConfig": Object.assign(
                        { "Privileged": privileged },
                        link && { "Links": link },
                        binds && { "Binds": binds }
                    )
                },
                cmd && { "Cmd": cmd },
                entrypoint && { "EntryPoint": entrypoint },
                env && { "Env": serializeEnv(env) }
            )
        })
            .thru(decodeResponse)
            .map(fp.property('Id'))
            .toProperty(),

        startContainer: ({ id })=> client({ path: `containers/${id}/start`, method: "POST" })
            .filter(_.matchesProperty('type', 'header'))
            .map(({ "status_code": statusCode })=> isOkHttpHeader(statusCode)),

        removeContainer: ({ id, force = true, removeVolumes = true })=> client({ path: `containers/${id}`, method: "DELETE", qs: { force, v: removeVolumes } }).thru(decodeResponse),

        attachContainer: ({ id, stdout = true, stderr = true })=> {
            return client({
                path: `containers/${id}/attach`,
                method: "POST",
                response: TCP_RESPONSE,
                qs: { "stream": "1", "stdout": toArgBoolean(stdout), "stderr": toArgBoolean(stderr) }
            });
        },

        createExec: ({ containerId, stdout = true, stderr = true, stdin = false, env = {}, cmd = [], privileged = false, workingDir })=> {
            return client({
                path: `containers/${containerId}/exec`,
                method: "POST",
                json: Object
                    .assign({
                            "AttachStdin": stdin,
                            "AttachStdout": stdout,
                            "AttachStderr": stderr,
                            "Env": serializeEnv(env),
                            "Privileged": privileged,
                            "WorkingDir": workingDir
                        },
                        cmd && { "Cmd": _.flatten([cmd]) }
                    )
            }).thru(decodeResponse).map(_.property('Id'));
        },

        startExec: ({ id })=> {
            return client({
                path: `exec/${id}/start`,
                response: TCP_RESPONSE,
                method: "POST",
                json: {
                    "Detach": false,
                    "Tty": false
                }
            });
        },

        copyToContainer: ({ containerId, path = "/" })=> {
            let inputStream = new PassThrough;
            return {
                write: inputStream.write.bind(inputStream),
                end: inputStream.end.bind(inputStream),
                output: client({
                    path: `containers/${containerId}/archive`,
                    method: "PUT",
                    stream: inputStream,
                    qs: { path }
                }).thru(bufferResponse)
            }
        },

        copyFromContainer: ({ containerId, path = "/" })=> {
            return client({ path: `containers/${containerId}/archive`, qs: { path } });
        },

        buildImage: ({ tag, docker_file_path = "Dockerfile", build_args = {}, labels = {} })=> {
            let inputStream = new PassThrough;

            return {
                write: inputStream.write.bind(inputStream),
                end: inputStream.end.bind(inputStream),
                output: client({
                    path: `build`,
                    method: "POST",
                    stream: inputStream,
                    headers: {
                        "Content-Type": "application/x-tar",
                        "X-Registry-Config": JSON.stringify({})
                    },
                    qs: {
                        dockerfile: docker_file_path,
                        forcerm: true,
                        buildargs: JSON.stringify(build_args),
                        t: tag,
                        labels: JSON.stringify(labels),
                        nocache: true,
                        memswap: 0
                    }
                })
                    .map(_.flow(_.property('body'), _.toString, fp.split(/\r?\n/), fp.filter(Boolean), fp.map((line)=> _.attempt(()=> JSON.parse(line)) )))
                    .flatten()
                    .flatMap((logMessage)=> kefir[logMessage["error"] ? "constantError" : "constant"](logMessage))
            };
        },

        exportImage: ({ name })=> {
            return client({
                path: `images/${name}/get`,
                method: "GET"
            });
        },

        inspectExec: ({ id })=> {
            return client({ path: `exec/${id}/json` }).thru(decodeResponse);
        }
    };
};
