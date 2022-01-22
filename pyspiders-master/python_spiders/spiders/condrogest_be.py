# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import json, re
import scrapy
from ..items import ListingItem


class CondrogestSpider(scrapy.Spider):
    name = 'condrogest_be'
    allowed_domains = ['condrogest.be']
    start_urls = ['https://condrogest.be/page-data/fr/let/page-data.json']
    execution_type = 'testing'
    country = 'belgium'
    locale ='fr'

    def parse(self, response, **kwargs):
        json_response = json.loads(response.body)
        properties = json_response.get('result').get(
            'data').get('allProperty').get('edges')
        for property_item in properties:
            property_type = property_item.get('node').get('type')
            if property_type in ['apartment', 'house']:
                property_url = "https://condrogest.be/page-data/fr/let/{}/page-data.json".format(
                    property_item.get('node').get('id'))
                yield scrapy.Request(
                    url=property_url,
                    callback=self.get_details
                )

    def get_details(self, response):
        property_item_main = json.loads(response.body).get(
            'result', {}).get('data', {}).get('property', {})
        item = ListingItem()
        item['external_source'] = "Condrogest_PySpider_belgium_fr"
        item['external_link'] = 'https://condrogest.be/fr/let/{}/'.format(property_item_main.get('id'))
        item['external_id'] = property_item_main.get('id')
        item['property_type'] = property_item_main.get('type')
        latitude = property_item_main.get('attributes').get('location').get('geo').get('latitude')
        if latitude:
            item['latitude'] = str(latitude)
        longitude = property_item_main.get('attributes').get('location').get('geo').get('longitude')
        if longitude:
            item['longitude'] = str(longitude)
        item['address'] = property_item_main.get('attributes').get('location').get('formatted_agency')
        item['city'] = property_item_main.get('attributes').get('location').get('city')
        item['zipcode'] = property_item_main.get('attributes').get('location').get('postal_code')
        floor = property_item_main.get('attributes').get('location').get('floor', '')
        if floor:
            item['floor'] = floor
        item['room_count'] = property_item_main.get('attributes').get('structure').get('bedrooms')
        bathroom_count = property_item_main.get('attributes').get('structure').get('bathrooms')
        if bathroom_count:
            item['bathroom_count'] = bathroom_count
        else:
            bathroom_count = response.xpath("//span/span[contains(.,'Salle')]/parent::span/b/text()").get()
            if bathroom_count:
                item["bathroom_count"] = bathroom_count
                
        square_meters = property_item_main.get('attributes').get('structure').get('liveable_area').get('size')
        if square_meters:
            item['square_meters'] = int(square_meters)
        costs = property_item_main.get('attributes').get('price',{}).get('costs',{}).get('fr',None)
        if costs:
            utilities = re.search(r'\d+(?=€)',costs)
            if utilities:
                item['utilities'] = int(utilities.group())
        amenities = property_item_main.get('attributes').get('amenities')
        if 'terrace' in amenities:
            item['terrace'] = True
        if 'parking' in amenities:
            item['parking'] = True
        if 'lift' in amenities:
            item['elevator'] = True
        images = property_item_main.get('attributes').get('images', [])
        item['images'] = [
            "https://cdn.sweepbright.com/properties/presets/agency-website/{}".format(i.get('id')) for i in images]
        item['title'] = property_item_main.get('attributes').get(
            'description', {}).get('title', {}).get('fr', None).replace("Condrogest","")
        item['description'] = property_item_main.get('attributes').get(
            'description', {}).get('description', {}).get('fr', None).replace("Condrogest","")
        rent = property_item_main.get('attributes').get(
            'price', {}).get('published_price', {}).get('amount', None)
        if rent:
            item['rent'] = int(rent)
        item['currency'] = property_item_main.get('attributes').get(
            'price', {}).get('published_price', {}).get('currency', None)
        item['landlord_name'] = "{} {}".format(property_item_main.get('negotiator', {}).get('first_name'),
                                               property_item_main.get('negotiator', {}).get('last_name'))
        item['landlord_email'] = property_item_main.get(
            'negotiator', {}).get('email')
        item['landlord_phone'] = property_item_main.get(
            'negotiator', {}).get('phone')
        epc_value = property_item_main.get('attributes').get('legal').get('energy').get('epc_value', None)
        if epc_value:
            epc_value = "{} kWh / m²".format(epc_value)
            item['energy_label'] = epc_value
        yield item
