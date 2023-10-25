/*
 * ACS Edge Monitor
 * Monitor class
 * Copyright 2023 AMRC
 */

import rx from "rxjs";

import { Address, UUIDs } from "@amrc-factoryplus/utilities";

import { App } from "./uuids.js";

export class Monitor {
    constructor (opts) {
        this.fplus = opts.fplus;
        this.node = opts.node;

        this.log = this.fplus.debug.log.bind(this.fplus.debug, "monitor");
    }

    async init () {
        this.app = await this.fplus.MQTT.sparkplug_app();
        this.device = await this.app.device({ node: this.node });
        this.config = await this._init_config();
        this.config_updates = this._init_config_updates();
        this.restarter = this._init_restarter();

        return this;
    }

    /* Track the Edge Agent's current config revision published over
     * MQTT, and the latest revision from the ConfigDB. If they don't
     * match, CMD the EA to reload its config file.
     */
    _init_config_updates () {
        const cmdesc = this.fplus.CmdEsc;

        /* Collect the most recent values from... */
        return rx.combineLatest({
            /* Our EA's Sparkplug address */
            address: this.device.address,
            /* Our EA's in-use config revision */
            device: this.device.metric("Config_Revision"),
            /* The ConfigDB's current config revision */
            config: this.config,
        }).pipe(
            /* EA and ConfigDB represent 'no config' differently */
            rx.map(up => ({ ...up,
                device: up.device == UUIDs.Null ? undefined : up.device,
            })),
            /* If they match, we don't care */
            rx.filter(up => up.device != up.config),
            /* Throttle restarts to once every 5s */
            rx.throttleTime(5000, undefined, { trailing: true }),
            rx.tap(up => this.log("Config update: %o", up)),
            /* We only want the address at this point */
            rx.map(up => up.address),
            /* Make a command escalation request. The Promise from
             * request_cmd will be converted to an Observable. */
            rx.switchMap(addr => cmdesc
                .request_cmd({
                    address:    addr,
                    name:       "Node Control/Reload Edge Agent Config",
                    type:       "Boolean",
                    value:      true,
                })
                /* Ensure that the address stays in the pipeline
                 * as the return result */
                .then(() => addr)),
            /* We only get here once the request has finished */
            rx.tap(addr => this.log("Sent reload request to %s", addr)),
        );
    }

    /* Track whether our EA is online. If the Node is offline for too
     * long, restart the Deployment. Trust the MQTT server to publish
     * NDEATH if the EA loses connection. */
    _init_restarter () {
        /* This sequence returns all Sparkplug packets from our EA */
        return this.device.packets.pipe(
            /* We only care about the packet type */
            rx.map(p => p.type),
            /* We can ignore packets that aren't BIRTH or DEATH */
            rx.mergeMap(t =>
                t == "BIRTH"    ? rx.of(true)   :
                t == "DEATH"    ? rx.of(false)  :
                rx.EMPTY),
            rx.tap(o => this.log("ONLINE: %o", o)),
            /* Each time we change state, start a new sequence. If we
             * are online, this sequence never does anything (we are
             * OK if we are online). If we are offline, this sequence
             * emits a value every 10s. Output the values from
             * the most recent of these; abandon the previous one when
             * we get a new state change. */
            rx.switchMap(o => o ? rx.NEVER : rx.interval(10*1000)),
            rx.tap(() => this.log("RESTART")),
        );
    }

    /* Track the current revision of our EA config in the ConfigDB. */
    /* XXX This is far from ideal. The only (working) interface
     * currently exposed by the CDB is Last_Changed/Application, which
     * means we have to check our node's config every time any config
     * changes. */
    async _init_config () {
        const cdb = this.fplus.ConfigDB;
        /* This is an object for watching the Last_Changed metrics on
         * the ConfigDB. */
        const watcher = await cdb.watcher();
        /* Watch for changes to configs for the AgentConfig Application */
        return watcher.application(App.AgentConfig).pipe(
            /* Add an extra notification at the start so we get the
             * current value when we start up */
            rx.startWith(undefined),
            /* Every time there's a change, fetch the current config
             * revision for our config. This does a HEAD request which
             * just gets the revision UUID. switchMap says 'if a new
             * notification comes in before request is answered, abandon
             * the current request and start a new one'. */
            rx.switchMap(() => cdb.get_config_etag(App.AgentConfig, this.node)),
            /* Skip notifications that don't change our revision UUID */
            rx.distinctUntilChanged(),
        );
    }

    run () {
        this.config_updates.subscribe();
        this.restarter.subscribe();
    }
}
