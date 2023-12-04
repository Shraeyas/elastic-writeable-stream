import stream, { WritableOptions } from 'stream';
import { Client } from '@elastic/elasticsearch'
import { BufferEncoding } from 'typescript';

interface ElasticsearchConfig {
    host: string;
    index: string;
    type: string;
    auth: any;
    cloud: any;
    node: any;
}

class ElasticsearchWritableStream extends stream.Writable {
    private config: ElasticsearchConfig;
    private client: Client;

    constructor(config: ElasticsearchConfig, options?: WritableOptions) {
        super(options);
        this.config = config;
        this.client = new Client({
            cloud: this.config.cloud,
            auth: this.config.auth,
            node: this.config.node,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        });
    }

    _destroy(): Promise<void> {
        return this.client.close();
    }

    async _write(
        body: any,
        enc: BufferEncoding,
        next: any
    ): Promise<void> {
        try {
            await this.client.index({
                index: this.config.index,
                body: body.toString()
            })
            next();
        } catch (err) {
            next(err);
        }
    }

    async _writev(
        chunks: Array<{ chunk: any; encoding: BufferEncoding }>,
        next: any
    ): Promise<void> {
        const body = chunks
            .map((chunk) => chunk.chunk)
            .reduce((arr, obj) => {
                arr.push({ index: {} });
                arr.push(obj);
                return arr;
            }, []);

        try {
            await this.client.bulk({
                index: this.config.index,
                body: body.toString()
            });
            next();
        } catch (err) {
            next(err);
        }
    }
}

export default (options: ElasticsearchConfig) => {
    const sink = new ElasticsearchWritableStream(options);
    return sink;
}