import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from './config.service';
import { Client, ClientChannel } from 'ssh2';
import * as fs from 'fs';
import axios from 'axios';

interface NodeConfig {
    job_name: string;
    static_configs: Array<{
        targets: string[];
        labels: Record<string, string>;
    }>;
}

@Injectable()
export class LogCollectorService {
    private readonly logger = new Logger(LogCollectorService.name);

    private readonly maxRetries = 5;
    private readonly retryDelay = 5000; // ms
    private readonly batchSize = 50; // líneas por batch
    private readonly batchInterval = 2000; // enviar batch cada 2 segundos

    private logQueues: Record<string, string[]> = {}; // cola de logs por nodo
    private flushIntervals: Record<string, NodeJS.Timeout> = {}; // intervalos por nodo

    constructor(private readonly configService: ConfigService) { }

    onApplicationBootstrap() {
        this.logger.log('Starting log collection on app bootstrap...');
        this.collectLogs();
    }

    async collectLogs() {
        const nodes: NodeConfig[] = this.configService.getNodes();
        await Promise.all(nodes.map((node) => this.handleNode(node)));
    }

    private async handleNode(node: NodeConfig) {
        const target = node.static_configs[0].targets[0];
        const labels = node.static_configs[0].labels;

        // inicializamos la cola
        this.logQueues[target] = [];

        let retries = 0;

        const connect = () => {
            const conn = new Client();

            conn.on('ready', () => {
                this.logger.log(`Connected to ${target}`);
                this.streamLogs(conn, target, labels);

                // iniciar envío periódico de batches
                this.startFlushInterval(target, labels);
            });

            conn.on('error', (err) => {
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
                privateKey: fs.readFileSync(process.env.key!),
            });
        };

        connect();
    }

    private startFlushInterval(host: string, labels: Record<string, string>) {
        if (this.flushIntervals[host]) return; // ya existe
        this.flushIntervals[host] = setInterval(() => this.flushLogs(host, labels), this.batchInterval);
    }

    private streamLogs(conn: Client, host: string, labels: Record<string, string>) {
        conn.exec('logread -f', (err, stream: ClientChannel) => {
            if (err) {
                this.logger.error(`Error executing logread on ${host}: ${err.message}`);
                return;
            }

            stream.on('data', (data: Buffer) => {
                const lines = data.toString().split('\n').filter(Boolean);
                this.logQueues[host].push(...lines);

                if (this.logQueues[host].length >= this.batchSize) {
                    this.flushLogs(host, labels);
                }
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

        const lokiPayload = {
            streams: [
                {
                    stream: labels,
                    values: batch.map((line) => [`${Date.now() * 1000000}`, line]),
                },
            ],
        };

        try {
            await axios.post('http://localhost:3100/loki/api/v1/push', lokiPayload);
        } catch (err: any) {
            this.logger.error(`Error sending batch to Loki for ${host}: ${err.message}`);
            // reinsertar batch al inicio de la cola si falla
            this.logQueues[host].unshift(...batch);
        }
    }
}
