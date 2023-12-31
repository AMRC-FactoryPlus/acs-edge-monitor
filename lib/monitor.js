/*
 * ACS Edge Monitor
 * Monitor class
 * Copyright 2023 AMRC
 */

import util         from "util";

import imm          from "immutable";
import rx           from "rxjs";
import k8s          from "@kubernetes/client-node";

import * as rxx             from "@amrc-factoryplus/rx-util";

import { AgentMonitor, NodeMonitor }    from "./node.js";
import { App }                          from "./uuids.js";

const NodeSpec = imm.Record({
    uuid:       null, 
    address:    null,
    edgeAgent:  false,
});

export class Monitor {
    constructor (opts) {
        this.fplus = opts.fplus;

        this.log = this.fplus.debug.log.bind(this.fplus.debug, "monitor");
    }

    async init () {
        //const mqtt = await this.fplus.MQTT.mqtt_client();
        //mqtt.on("authenticated", () => {});

        const kc = this.kubeconfig = new k8s.KubeConfig();
        kc.loadFromDefault();
        this.namespace = kc.getContextObject(kc.currentContext).namespace;

        const nodes = this._init_nodes();
        const [starts, stops] = this._node_start_stops(nodes);
        this.node_checks = this._init_node_checks(starts, stops);

        return this;
    }

    run () {
        this.node_checks.subscribe();
    }

    /* Watch the SparkplugNode objects on the cluster. Publishes
     * Immutable.Maps of the current state. */
    _init_nodes () {
        /* Watch K8s SparkplugNode objects. Converting to an immutable
         * NodeSpec at this point allows the distinct-until-changed to
         * work correctly. */
        return rxx.k8s_watch({
            k8s,
            kubeconfig:     this.kubeconfig,
            errors:         e => this.log("SP node watch error: %s", e),
            apiVersion:     "factoryplus.app.amrc.co.uk/v1",
            kind:           "SparkplugNode",
            namespace:      this.namespace,
            value:          obj => NodeSpec(obj.spec),
        }).pipe(
            /* We only care about the objects, not the k8s UUIDs, and we
             * can only handle Nodes with F+ UUIDs. */
            rx.map(ns => {
                const [uuids, addrs] = ns.valueSeq()
                    .partition(n => !n.uuid);
                for (const n of addrs)
                    this.log("Can't handle non-F+ Node %s", n.address);
                return uuids.toSet();
            }),
            /* Make sure one value is always available whenever we get a
             * new subscriber */
            rxx.shareLatest(),
        );
    }

    _node_start_stops (nodes) {
        /* Work out what's changed */
        const changes = nodes.pipe(
            rx.startWith(imm.Set()),
            rx.pairwise(),
            rx.mergeMap(([then, now]) => imm.Seq([
                then.subtract(now).map(u => [false, u]),
                now.subtract(then).map(u => [true, u]),
            ]).flatten()),
            rx.share(),
        );
        /* Split the changes into starts and stops */
        return rx.partition(changes, ch => ch[0])
            .map(seq => seq.pipe(rx.map(ch => ch[1])));
    }

    _init_node_checks (starts, stops) {
        /* When we get a "start", start a new sequence monitoring that
         * node. Merge the results into our output. */
        return starts.pipe(
            rx.tap(u => this.log("START: %s", u)),
            rx.flatMap(({edgeAgent, uuid}) => {
                /* Watch for a stop signal for this node UUID */
                const stopper = stops.pipe(
                    rx.filter(stop => stop.uuid == uuid),
                );

                const MClass = edgeAgent ? AgentMonitor : NodeMonitor;
                const monitor = new MClass({ 
                    fplus:  this.fplus, 
                    node:   uuid,
                });
                this.log("Using monitor %s for %s", monitor, uuid);

                /* Run the monitor checks until we get a stop */
                return monitor.checks().pipe(
                    rx.takeUntil(stopper),
                    rx.finalize(() => this.log("STOP: %s", uuid)),
                );
            }),
        );
    }
}
