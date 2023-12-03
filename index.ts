import stream, { WritableOptions } from 'stream';
import * as elasticsearch from 'elasticsearch';
import { BufferEncoding } from 'typescript';

interface ElasticsearchConfig {
    host: string;
    index: string;
    type: string;
}

class ElasticsearchWritableStream extends stream.Writable {
    private config: ElasticsearchConfig;
    private client: elasticsearch.Client;

    constructor(config: ElasticsearchConfig, options?: WritableOptions) {
        super(options);
        this.config = config;
        this.client = new elasticsearch.Client({
            host: this.config.host,
        });
    }

    _destroy(): void {
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
                type: this.config.type,
                body: body.toString(),
            });
            next();
        } catch (err) {
            next(err);
        }
    }

    async _writev(
        chunks: Array<{ chunk: any; encoding: BufferEncoding }>,
        next: (param?: any) => void
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
                type: this.config.type,
                body: body.toString(),
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