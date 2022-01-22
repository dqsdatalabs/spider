# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
import re
from scrapy import Request
from ..loaders import ListingLoader
import json
from datetime import datetime


class OrpixSpider(scrapy.Spider):
    name = 'orpix_immo'
    allowed_domains = ['www.orpix.immo']
    start_urls = ['http://www.orpix.immo/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    position = 0
    thousand_separator = ' '
    scale_separator = ','

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.orpix.immo/index.php?option=com_izimo&view=ajax&layout=get-estates-db&wlang=fr-BE&itemid=143',
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse)

    def parse(self, response, **kwargs):
        json_response = json.loads(response.text)
        for listing in json_response:
            if listing['purposestatus_id'] == 2 and listing['category_id'] in [1, 2]:
                property_url = 'https://www.orpix.immo/fr/a-louer/'+str(listing['estate_id'])
                yield scrapy.Request(
                    url=property_url,
                    callback=self.get_property_details,
                    meta={'request_url': property_url,
                          'listing': listing,
                          }
                )

    def get_property_details(self, response):
        listing = response.meta['listing']
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_id', str(listing['estate_id']))
        item_loader.add_value('external_link', 'https://www.orpix.immo/fr/a-louer/'+str(listing['estate_id']))

        item_loader.add_value('title', listing['estate_name'])
        item_loader.add_value('rent_string', " ".join([str(listing['estate_price']), listing['estate_currency']]))
        # item_loader.add_value('rent', str(listing['estate_price']))
        # item_loader.add_value('currency', listing['estate_currency'])

        item_loader.add_value('zipcode', listing['estate_zip'])
        item_loader.add_value('city', listing['estate_city'])

        address = ' '.join([listing['estate_addr1'], (listing['estate_addr2'] if listing['estate_addr2'] else ''), listing['estate_zip'], listing['estate_city']])
        item_loader.add_value('address', address)

        item_loader.add_value('latitude', str(round(listing['coords']['lat'], 4)))
        item_loader.add_value('longitude', str(round(listing['coords']['lng'], 4)))

        item_loader.add_value('square_meters', str(listing['estate_area']))
        item_loader.add_xpath('utilities', './/span[contains(text(), "Charges")]/../..//span[contains(@class, "value")]/text()')
        if listing['category_id'] == 1:
            item_loader.add_value('property_type', 'house')
        elif listing['category_id'] == 2:
            item_loader.add_value('property_type', 'apartment')

        item_loader.add_value('description', listing['descriptions'][0]['estate_description_content'])

        landlord_details = json.loads(listing['estate_representativelist'].replace('\\',''))

        item_loader.add_value('landlord_name', landlord_details['Name'])
        item_loader.add_value('landlord_phone', landlord_details['Mobile'])
        item_loader.add_value('landlord_email', landlord_details['Email'])

        item_loader.add_value('room_count', str(listing['estate_rooms']))
        item_loader.add_value('bathroom_count', str(listing['estate_bathrooms']))
        if listing['estate_energyvalue']:
            item_loader.add_value('energy_label', str(listing['estate_energyvalue']) + " kWh/m2.an")

        images = []

        for picture in listing['pictures']:
            images.append(picture['estate_picture_url_xxl'].replace('\\', ''))

        item_loader.add_value('images', images)
        item_loader.add_value('external_images_count', len(images))
        if int(re.sub(r"[^\d]+", "", listing['availability_at'])) > 0:
            item_loader.add_value('available_date', datetime.strptime(listing['availability_at'], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d"))

        # Furnished
        if listing['estate_furnished']:
            if listing['estate_furnished'] in [0, 'non']:
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        # Terrace
        if listing['estate_terrace']:
            if listing['estate_terrace'] in [0, 'non']:
                item_loader.add_value('terrace', False)
            else:
                item_loader.add_value('terrace', True)

        # Parking
        if listing['estate_parking']:
            if listing['estate_parking'] in [0, 'non']:
                item_loader.add_value('parking', False)
            else:
                item_loader.add_value('parking', True)

        # swimming pool
        # https://www.orpix.immo/fr/a-louer/4203865/
        swimming_pool = response.xpath('.//span[contains(text(), "Piscine")]/../..//span[contains(@class, "value")]/text()').extract_first()
        if swimming_pool:
            if swimming_pool.lower() == "non":
                item_loader.add_value('swimming_pool', False)
            else:
                item_loader.add_value('swimming_pool', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "OrpixImmo_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
