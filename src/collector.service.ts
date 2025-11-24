import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ConfigService } from './config.service';
import { Client, ClientChannel } from 'ssh2';
import * as fs from 'fs';
import axios from 'axios';
import * as net from 'net';

interface NodeConfig {
    job_name: string;
    static_configs: Array<{
        targets: string[];
        labels: Record<string, string>;
    }>;
}

@Injectable()
export class LogCollectorService implements OnApplicationBootstrap {
    private readonly logger = new Logger(LogCollectorService.name);

    private readonly maxRetries = 5;
    private readonly retryDelay = 5000;
    private readonly batchSize = 50;
    private readonly batchInterval = 2000;

    private logQueues: Record<string, string[]> = {};
    private flushIntervals: Record<string, NodeJS.Timeout> = {};
    private logBuffers: Record<string, string> = {};

    constructor(private readonly configService: ConfigService) { }

    onApplicationBootstrap() {
        this.logger.log('Starting log collection on app bootstrap...');
        this.collectLogs();
    }

    async collectLogs() {
        const nodes: NodeConfig[] = this.configService.getNodes();
        nodes.forEach(node => this.handleNode(node));
    }

    private async handleNode(node: NodeConfig) {
        const target = node.static_configs[0].targets[0];
        const labels = { ...node.static_configs[0].labels, nodo: node.job_name };

        this.logQueues[target] = [];
        this.logBuffers[target] = '';

        let retries = 0;

        const connect = async () => {
            const canConnect = await this.checkPort(target, 22);
            if (!canConnect) {
                this.logger.warn(`Node ${target} is offline, retrying in ${this.retryDelay / 1000}s...`);
                setTimeout(connect, this.retryDelay);
                return;
            }

            const conn = new Client();

            conn.on('ready', () => {
                this.logger.log(`Connected to ${target}`);
                this.streamLogs(conn, target, labels);
                this.startFlushInterval(target, labels);
            });

            conn.on('error', err => {
                this.logger.error(`SSH error on ${target}: ${err.message}`);
                if (retries < this.maxRetries) {
                    retries++;
                    setTimeout(connect, this.retryDelay);
                }
            });

            conn.on('end', () => {
                this.logger.warn(`SSH connection ended for ${target}, reconnecting...`);
                setTimeout(connect, this.retryDelay);
            });

            conn.connect({
                host: target,
                port: 22,
                username: 'root',
                privateKey: fs.readFileSync('/usr/src/ssh/tpv_rsa.pem'),
            });
        };

        connect();
    }

    private checkPort(host: string, port: number, timeout = 2000): Promise<boolean> {
        return new Promise(resolve => {
            const socket = new net.Socket();
            let isAvailable = false;

            socket.setTimeout(timeout);
            socket.on('connect', () => {
                isAvailable = true;
                socket.destroy();
            });
            socket.on('timeout', () => socket.destroy());
            socket.on('error', () => { });
            socket.on('close', () => resolve(isAvailable));

            socket.connect(port, host);
        });
    }

    private startFlushInterval(host: string, labels: Record<string, string>) {
        if (this.flushIntervals[host]) return;
        this.flushIntervals[host] = setInterval(() => this.flushLogs(host, labels), this.batchInterval);
    }

    private streamLogs(conn: Client, host: string, labels: Record<string, string>) {
        conn.exec('logread -f', (err, stream: ClientChannel) => {
            if (err) {
                this.logger.error(`Error executing logread on ${host}: ${err.message}`);
                return;
            }

            stream.on('data', (data: Buffer) => {
                this.logBuffers[host] += data.toString();
                const lines = this.logBuffers[host].split('\n');
                this.logBuffers[host] = lines.pop() || '';
                if (lines.length > 0) this.logQueues[host].push(...lines);

                if (this.logQueues[host].length >= this.batchSize) this.flushLogs(host, labels);
            });

            stream.on('close', () => {
                this.logger.warn(`Stream closed for ${host}, reconnecting...`);
                conn.end();
            });

            stream.stderr.on('data', (err: Buffer) => {
                this.logger.error(`SSH STDERR from ${host}: ${err.toString()}`);
            });
        });
    }

    private async flushLogs(host: string, labels: Record<string, string>) {
        const queue = this.logQueues[host];
        if (!queue.length) return;

        const batch = queue.splice(0, this.batchSize);

        const nowNs = Date.now() * 1_000_000;

        const lokiPayload = {
            streams: [
                {
                    stream: labels,
                    values: batch.map((line, i) => [`${nowNs + i}`, line]),
                },
            ],
        };

        try {
            await axios.post('http://172.17.0.1:3100/loki/api/v1/push', lokiPayload);
        } catch (err: any) {
            this.logger.error(`Error sending batch to Loki for ${host}: ${err.message}`);
            this.logQueues[host].unshift(...batch);
        }
    }
}
