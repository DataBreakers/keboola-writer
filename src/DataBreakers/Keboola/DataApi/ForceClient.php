<?php

namespace Databreakers\Keboola\DataApi;

use DataBreakers\DataApi\Client;
use DataBreakers\DataApi\Exceptions\RequestFailedException;
use DataBreakers\DataApi\Utils\Restriction;


class ForceClient extends Client
{

	const DELETE_ITEMS_URL = '/{accountId}/items{?force}';
	const DELETE_USERS_URL = '/{accountId}/users{?force}';
	const DELETE_INTERACTIONS_URL = '/{accountId}/interactions{?force}';


	/**
	 * @return NULL
	 * @throws RequestFailedException when request failed for some reason
	 */
	public function deleteItems()
	{
		return $this->forceDelete(self::DELETE_ITEMS_URL);
	}

	/**
	 * @return NULL
	 * @throws RequestFailedException when request failed for some reason
	 */
	public function deleteUsers()
	{
		return $this->forceDelete(self::DELETE_USERS_URL);
	}

	/**
	 * @return NULL
	 * @throws RequestFailedException when request failed for some reason
	 */
	public function deleteInteractions()
	{
		return $this->forceDelete(self::DELETE_INTERACTIONS_URL);
	}

	/**
	 * @param string $pathTemplate
	 * @return NULL
	 */
	private function forceDelete($pathTemplate)
	{
		$restriction = new Restriction(['force' => 'true']);
		return $this->api->performDelete($pathTemplate, $restriction);
	}

}
