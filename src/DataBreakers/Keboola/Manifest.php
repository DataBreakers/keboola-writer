<?php

namespace DataBreakers\Keboola;

class Manifest {

	protected $data;

	public function __construct($filePath) {
		if( ! is_readable($filePath)) {
			throw new Exception('Cannot read manifest file');
		}

		$fileData = file_get_contents($filePath);
		$this->data = json_decode($fileData, true);
	}

	protected function validate() {
		if( ! isset($this->data['primary_key']) || ! is_array($this->data['primary_key'])) {
			throw new Exception('"Primary key" not defined in manifest file');
		}

		if( ! isset($this->data['columns']) || ! is_array($this->data['columns'])) {
			throw new Exception('"Columns" not defined in manifest file');
		}

		return $this;
	}

	public function getPrimaryKey() {
		return $this->data['primary_key'];
	}

	public function getColumns() {
		return $this->data['columns'];
	}

}
