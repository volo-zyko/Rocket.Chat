import { ImporterInfo } from '../../importer/lib/ImporterInfo';

export class ZulipImporterInfo extends ImporterInfo {
	constructor() {
		super('zulip', 'Zulip (tar.gz)', 'application/gzip');
	}
}
