import { ZulipImporter } from './importer';
import { Importers } from '../../importer/server';
import { ZulipImporterInfo } from '../lib/info';

Importers.add(new ZulipImporterInfo(), ZulipImporter);
