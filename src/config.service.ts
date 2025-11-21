import * as fs from 'fs';
import * as yaml from 'js-yaml';

export class ConfigService {
    private filePath = './nodes.yaml';

    getNodes() {
        const file = fs.readFileSync(this.filePath, 'utf8');
        const data = yaml.load(file) as any;
        return data.nodes || [];
    }
}
