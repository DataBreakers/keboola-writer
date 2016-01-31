<?php

namespace DataBreakers\Keboola;

use Keboola\Csv\CsvFile;
use DataBreakers\DataApi;

use Exception;

class UsersTable extends Table {

	public function __construct($filePath=NULL, Config $config) {
		parent::__construct($filePath, $config);
	}

	public function getName() {
		return 'users';
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
				throw new Exception('Primary key of users table has to be on a single column');
			}

			// primary key is a column
			$columns = $this->manifest->getColumns();
			if( ! in_array($pk[0], $columns)) {
				throw new Exception('Primary key of users table is not presented in columns');
			}
		}

		return $this;
	}

	/**
	 * delete all users and users' attributes from recommender
	 *
	 */
	public function clear(DataApi\Client $client) {
		if( ! $this->config->isAppend()) {
			$client->deleteUsers();

			$usersAttributes = $client->getUsersAttributes();
			if( isset($usersAttributes['attributes'])) {
				foreach($usersAttributes['attributes'] as $attribute) {
					$client->deleteUsersAttribute($attribute['name']);
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
			$usersAttributes = $client->getUsersAttributes();
			$usersAttributesNames = array_map( function($attr) { return $attr['name'];}, $usersAttributes['attributes']);

			foreach($this->manifest->getColumns() as $attribute) {
				$pk = $this->manifest->getPrimaryKey()[0];
				if($attribute != $pk && ! in_array($attribute, $usersAttributesNames)) {
					$client->addUsersAttribute($attribute, DataApi\DataType::TEXT);
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
			$batchSize = 5000;
			$csvFile = new CsvFile($this->filePath);

			$csvFile->rewind();
			if( ! $csvFile->valid()) {
				break;
			}

			$header = $csvFile->current();
			$csvFile->next();

			$usersBatch = new DataApi\Batch\EntitiesBatch();
			$batchRowsCount = 0;
			while($csvFile->valid()) {
				$attributes = array_combine($header, $csvFile->current());
				$csvFile->next();

				// process row - remove id column etc.
				$pk = $this->manifest->getPrimaryKey()[0];
				$id = $attributes[$pk];
				unset($attributes[$pk]);
				$usersBatch->addEntity($id, $attributes);
				$batchRowsCount += 1;

				if($batchRowsCount == $batchSize || ! $csvFile->valid()) {
					$client->insertOrUpdateUsers($usersBatch);
					$usersBatch = new DataApi\Batch\EntitiesBatch();
					$batchRowsCount = 0;
				}
			}
		}

		return $this;
	}

}
