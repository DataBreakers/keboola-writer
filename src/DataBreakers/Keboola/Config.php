<?php

namespace DataBreakers\Keboola;

use Exception;

class Config {

	protected $configArray;

	public function __construct(array $configArray) {

		if( ! isset($configArray['parameters']['databreakers_account']))
			throw new Exception('Parameter "databreakers_account" is required');
		if( ! isset($configArray['parameters']['databreakers_secret_key']))
			throw new Exception('Parameter "databreakers_secret_key" is required');
		if( ! isset($configArray['parameters']['delete_old_data']))
			throw new Exception('Parameter "delete_old_data" is required');

		$this->configArray = $configArray;
	}

	public function getConfigArray() {
		return $this->configArray;
	}

	public function getAcountId() {
		return $this->configArray['parameters']['databreakers_account'];
	}

	public function getSecretKey() {
		return $this->configArray['parameters']['databreakers_secret_key'];
	}

	/**
	 * TODO: implement ($options['delete_old_data'])
	 *
	 */
	public function isAppend() {
		return $this->configArray['parameters']['delete_old_data'] ? false : true;
	}

}
