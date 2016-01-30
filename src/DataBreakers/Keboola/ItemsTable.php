<?php

namespace DataBreakers\Keboola;

use Keboola\Csv\CsvFile;
use DataBreakers\DataApi;

use Exception;

class ItemsTable extends Table {

	public function __construct($filePath=NULL, Config $config) {
		parent::__construct($filePath, $config);
	}

	public function getName() {
		return 'items';
	}

	public function isRequired() {
		return false;
	}

	public function isRequiredOnAppend() {
		return false;
	}

	/**
	 * validate enviroment for the table
	 * - manifest PK and columns
	 *
	 */
	public function validate() {
		if( ! is_null($this->manifest)) {
			// has primary key and on a single column
			$pk = $this->manifest->getPrimaryKey();
			if( ! isset($pk[0]) || count($pk) != 1) {
				throw new Exception('Primary key of items table needs to be on a single column');
			}

			// primary key is a column
			$columns = $this->manifest->getColumns();
			if( ! in_array($pk[0], $columns)) {
				throw new Exception('Primary key of items table is not presented in columns');
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
			$client->deleteItems();

			$itemsAttributes = $client->getItemsAttributes();
			if( isset($itemsAttributes['attributes'])) {
				foreach($itemsAttributes['attributes'] as $attribute) {
					$client->deleteItemsAttribute($attribute['name']);
				}
			}
		}

		return $this;
	}

	/**
	 * set all attributes in recommender by manifest file
	 *
	 * TODO: do not set existing attributes
	 * TODO: set metatypes
	 *
	 */
	public function setAttributes(DataApi\Client $client) {
		if( ! is_null($this->manifest)) {
			$itemsAttributes = $client->getItemsAttributes();
			$itemsAttributesNames = array_map( function($attr) { return $attr['name'];}, $itemsAttributes['attributes']);

			foreach($this->manifest->getColumns() as $attribute) {
				$pk = $this->manifest->getPrimaryKey()[0];
				if($attribute != $pk && ! in_array($attribute, $itemsAttributesNames)) {
					$client->addItemsAttribute($attribute, DataApi\DataType::TEXT);
				}
			}
		}

		return $this;
	}

	/**
	 * upload all data to recommeder
	 * 
	 */
	public function upload(DataApi\Client $client) {
		if( ! is_null($this->filePath)) {
			$batchSize = 1000;
			$csvFile = new CsvFile($this->filePath);

			$csvFile->rewind();
			if( ! $csvFile->valid()) {
				break;
			}

			$header = $csvFile->current();
			$csvFile->next();

			$itemsBatch = new DataApi\Batch\EntitiesBatch();
			$batchRowsCount = 0;
			while($csvFile->valid()) {
				$attributes = array_combine($header, $csvFile->current());
				$csvFile->next();

				// process row - remove id column etc.
				$pk = $this->manifest->getPrimaryKey()[0];
				$id = $attributes[$pk];
				unset($attributes[$pk]);
				$itemsBatch->addEntity($id, $attributes);
				$batchRowsCount += 1;

				if($batchRowsCount == $batchSize || ! $csvFile->valid()) {
					$client->insertOrUpdateItems($itemsBatch);
					$itemsBatch = new DataApi\Batch\EntitiesBatch();
					$batchRowsCount = 0;
				}
			}
		}

		return $this;
	}

}
