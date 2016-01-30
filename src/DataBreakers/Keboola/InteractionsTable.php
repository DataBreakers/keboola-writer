<?php

namespace DataBreakers\Keboola;

use Keboola\Csv\CsvFile;
use DataBreakers\DataApi;

use Exception;
use DateTime;

class InteractionsTable extends Table {

	public function __construct($filePath, Config $config) {
		parent::__construct($filePath, $config);
	}

	public function getName() {
		return 'interactions';
	}

	public function isRequired() {
		return false;
	}

	public function isRequiredOnAppend() {
		return false;
	}

	/**
	 * validate enviroment for the table
	 * - manifest PK
	 * 
	 * PK has to have specific columns: [item_id, user_id, interaction_id, timestamp]
	 *
	 */
	public function validate() {
		if( ! is_null($this->manifest)) {
			$validColumns = ['item_id', 'user_id', 'interaction_id', 'timestamp'];

			// check columns
			$columns = $this->manifest->getColumns();
			if( ! empty( array_diff($validColumns, $columns))) {
				throw new Exception('Interactions table has to consist of columns [item_id, user_id, interaction_id, timestamp]');
			}

			$validPk = $validColumns;

			// check primary key
			$pk = $this->manifest->getPrimaryKey();
			if( ! empty( array_diff($validPk, $pk))) {
				throw new Exception('Primary key of interactions table has to use columns [item_id, user_id, interaction_id, timestamp]');
			}
		}

		return $this;
	}

	/**
	 * delete all items and items' attributes from recommender
	 *
	 */
	public function clear(DataApi\Client $client) {
		if( ! $this->config->isAppend()) {
			$client->deleteInteractions();
		}

		return $this;
	}

	/**
	 * no attributes for interactions can be set
	 * 
	 */
	public function setAttributes(DataApi\Client $client) {
		return $this;
	}

	/**
	 * upload all data to recommeder
	 * 
	 */
	public function upload(DataApi\Client $client) {
		if( ! is_null($this->filePath)) {
			$batchSize = 25000;
			$csvFile = new CsvFile($this->filePath);

			$csvFile->rewind();
			if( ! $csvFile->valid()) {
				break;
			}

			$header = $csvFile->current();
			$csvFile->next();

			$interactionsBatch = new DataApi\Batch\InteractionsBatch();
			$batchRowsCount = 0;
			while($csvFile->valid()) {
				$attributes = array_combine($header, $csvFile->current());
				$csvFile->next();

				// process row
				try {
					if( is_numeric($attributes['timestamp'])) {
						$date = (new DateTime('@'.$attributes['timestamp']));
					} else {
						$date = (new DateTime($attributes['timestamp']));
					}
				} catch(Exception $e) {
					throw new Exception('Invalid date format in "'.$this->getName().'" table');
				}
				$interactionsBatch->addInteraction($attributes['user_id'], $attributes['item_id'], $attributes['interaction_id'], $date);
				$batchRowsCount += 1;

				if($batchRowsCount == $batchSize || ! $csvFile->valid()) {
					$client->insertInteractions($interactionsBatch);
					$interactionsBatch = new DataApi\Batch\InteractionsBatch();
					$batchRowsCount = 0;
				}
			}
		}

		return $this;
	}

}
