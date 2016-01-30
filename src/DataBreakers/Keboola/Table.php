<?php

namespace DataBreakers\Keboola;

use League\Csv\Reader;
use DataBreakers\DataApi;

use Exception;

abstract class Table {

	protected $filePath;
	protected $config;
	protected $manifest;

	/**
	 * set object - load manifest file and keep filepath (both will be null if filepath is not set)
	 * 
	 */
	public function __construct($filePath=NULL, Config $config) {
		if( ! is_null($filePath)) {
			if( ! is_readable($filePath)) {
				throw new Exception('Cannot read "'.$this->getName().'" table file');
			}

			$manifestFilePath = $filePath.'.manifest';
			if( ! is_readable($manifestFilePath)) {
				throw new Exception('Cannot read manifest file of "'.$this->getName().'" table');
			}

			$this->filePath = $filePath;
			$this->manifest = new Manifest($manifestFilePath);

			// validate structures
			$this->validate();

		} elseif($config->isAppend() && $this->isRequiredOnAppend()) {
			throw new Exception('Table "'.$this->getName().'" is required');

		} elseif( ! $config->isAppend() && $this->isRequired()) {
			throw new Exception('Table "'.$this->getName().'" is required');
		}

		$this->config = $config;
	}

	public abstract function getName();

	public abstract function isRequired();

	public abstract function isRequiredOnAppend();

	public abstract function validate();

	public abstract function clear(DataApi\Client $client);

	public abstract function setAttributes(DataApi\Client $client);

	public abstract function upload(DataApi\Client $client);

}
