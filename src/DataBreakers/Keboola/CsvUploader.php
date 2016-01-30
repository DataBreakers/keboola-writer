<?php

namespace DataBreakers\Keboola;

use League\Csv\Reader as CsvReader;
use DataBreakers\DataApi;

class CsvUploader {

	private $reader;
	private $client;

	private $batchSize;

	public function __construct(CsvReader $reader, DataApi\Client $client) {
		$this->reader = $reader;
		$this->client = $client;

		$this->batchSize = 100;
	}

	public function run() {
		$offset = 0; // skip header
		while(1) {
			$batchRows = $this->reader->setOffset($offset)->setLimit($this->batchSize)->fetchAssoc();
			// read till returned iterator is empty
			$batchRows->rewind();
			if( ! $batchRows->valid()) {
				break;
			}

			$itemsBatch = new DataApi\Batch\EntitiesBatch();
			foreach($batchRows as $row) {
				// process row - remove id column etc.
				$itemsBatch->addEntity($row['title'], $row);
				//var_dump($row);
			}
			$this->client->insertOrUpdateItems($itemsBatch);

			$offset += $this->batchSize;
		}
	}

}
